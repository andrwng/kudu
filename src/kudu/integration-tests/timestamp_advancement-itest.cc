// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <stdint.h>

#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_gauge_int64(raft_term);
METRIC_DECLARE_counter(rows_inserted);
METRIC_DECLARE_entity(tablet);

DECLARE_bool(enable_maintenance_manager);
DECLARE_bool(raft_enable_pre_election);
DECLARE_bool(raft_prepare_replacement_before_eviction);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_int32(consensus_inject_latency_ms_in_notifications);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(log_segment_size_mb);
DECLARE_int32(maintenance_manager_num_threads);
DECLARE_int32(maintenance_manager_polling_interval_ms);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_int64(log_target_replay_size_mb);
DECLARE_string(log_compression_codec);

using std::string;
using std::shared_ptr;
using std::unique_ptr;
using std::vector;

namespace kudu {

namespace consensus {
class RaftPeerPB;
}  // namespace consensus

namespace rpc {
class RpcController;
}  // namespace rpc

namespace tserver {
class TabletServerServiceProxy;
}  // namespace tserver

using cluster::InternalMiniCluster;
using cluster::InternalMiniClusterOptions;
using consensus::RaftPeerPB;
using log::LogReader;
using pb_util::SecureShortDebugString;
using tablet::TabletReplica;
using tserver::MiniTabletServer;
using tserver::NewScanRequestPB;
using tserver::ScanRequestPB;
using tserver::ScanResponsePB;
using tserver::TabletServerServiceProxy;
using rpc::RpcController;

namespace itest {

class NonAdvancedTimestampsITest : public MiniClusterITestBase {
 public:
  // Sets up a cluster and returns the tablet replica on 'ts' that has written
  // to its WAL. 'replica' will write further messages to a new WAL segment.
  void SetupClusterWithWritesInWAL(int ts, scoped_refptr<TabletReplica>* replica) {
    NO_FATALS(StartCluster(3));

    // Write some rows to the cluster.
    TestWorkload write(cluster_.get());;
    write.set_num_tablets(1);
    write.set_payload_bytes(10);
    write.set_num_write_threads(1);
    write.set_write_batch_size(1);
    write.Setup();
    write.Start();
    while (write.rows_inserted() < 100) {
      SleepFor(MonoDelta::FromMilliseconds(1));
    }
    write.StopAndJoin();

    // Ensure that the replicas eventually get to a point where all of them
    // have all the rows. This will allow us to GC the logs, as they will not
    // need to retain them if fully-replicated.
    const int64_t rows_inserted = write.rows_inserted();
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      HostPort hp;
      hp.set_host(cluster_->mini_tablet_server(i)->bound_http_addr().host());
      hp.set_port(cluster_->mini_tablet_server(i)->bound_http_addr().port());
      ASSERT_EVENTUALLY([&] {
        int64_t rows_in_replica;
        WARN_NOT_OK(itest::GetInt64Metric(hp,
            &METRIC_ENTITY_tablet, nullptr, &METRIC_rows_inserted, "value", &rows_in_replica),
            "Couldn't get metric... will retry");
        ASSERT_EQ(rows_inserted, rows_in_replica);
      });
    }

    // Flush to get these rows to the data blocks, and allocate a new segement
    // and roll over onto it. Note that the allocation happens asynchronously,
    // so we assert eventually here.
    scoped_refptr<TabletReplica> tablet_replica = tablet_replica_on_ts(ts);
    ASSERT_OK(tablet_replica->tablet()->Flush());
    ASSERT_EVENTUALLY([&] {
      ASSERT_OK(tablet_replica->log()->AllocateSegmentAndRollOver());
    });
    *replica = std::move(tablet_replica);
  }

  // Returns the tablet server 'ts'.
  MiniTabletServer* tserver(int ts) const {
    DCHECK(cluster_);
    return cluster_->mini_tablet_server(ts);
  }

  // Creates a scan request for a scan of 'tablet_id', with the assumption that
  // the tablet has the simple schema.
  ScanRequestPB ReqForTablet(const string& tablet_id) const {
    ScanRequestPB req;
    NewScanRequestPB* scan = req.mutable_new_scan_request();
    scan->set_tablet_id(tablet_id);
    scan->set_read_mode(READ_LATEST);
    const Schema schema = GetSimpleTestSchema();
    CHECK_OK(SchemaToColumnPBs(schema, scan->mutable_projected_columns()));
    return req;
  }

  // Get the tablet replica on the tablet server 'ts'.
  scoped_refptr<TabletReplica> tablet_replica_on_ts(int ts) const {
    vector<scoped_refptr<TabletReplica>> replicas;
    tserver(ts)->server()->tablet_manager()->GetTabletReplicas(&replicas);
    return replicas[0];
  }

  // Returns true if there are any write messages in the WALs of 'tablet_id' on
  // 'ts'.
  void HasNoWriteOps(MiniTabletServer* ts, const string& tablet_id, bool* has_write_ops) const {
    shared_ptr<LogReader> reader;
    ASSERT_OK(LogReader::Open(env_,
        ts->server()->fs_manager()->GetTabletWalDir(tablet_id),
        scoped_refptr<log::LogIndex>(), tablet_id,
        scoped_refptr<MetricEntity>(), &reader));
    log::SegmentSequence segs;
    ASSERT_OK(reader->GetSegmentsSnapshot(&segs));
    unique_ptr<log::LogEntryPB> entry;
    for (const auto& seg : segs) {
      log::LogEntryReader reader(seg.get());
      while (true) {
        Status s = reader.ReadNextEntry(&entry);
        if (s.IsEndOfFile() || s.IsCorruption()) break;
        ASSERT_OK(s);
        switch (entry->type()) {
          case log::COMMIT:
            if (entry->commit().op_type() == consensus::WRITE_OP) {
              *has_write_ops = true;
              return;
            }
            break;
          case log::REPLICATE:
            if (entry->replicate().op_type() == consensus::WRITE_OP) {
              *has_write_ops = true;
              return;
            }
            break;
          default:
            continue;
        }
      }
    }
    *has_write_ops = false;
  }
};

// Test that bootstrapping a Raft no-op from the WAL will leave advance the
// replica's MVCC timestamps.
TEST_F(NonAdvancedTimestampsITest, TestTimestampsAdvancedFromBootstrapNoOp) {
  // Set a low Raft heartbeat and encourage frequent elections.
  FLAGS_leader_failure_max_missed_heartbeat_periods = 1.0;
  FLAGS_raft_enable_pre_election = false;
  FLAGS_enable_maintenance_manager = false;
  FLAGS_raft_heartbeat_interval_ms = 10;

  // Inject latency to consensus notifications so we'll trigger elections.
  FLAGS_consensus_inject_latency_ms_in_notifications = 10;

  // Setup a cluster with some writes and a new WAL segment.
  const int kTserver = 0;
  scoped_refptr<TabletReplica> replica;
  NO_FATALS(SetupClusterWithWritesInWAL(kTserver, &replica));
  MiniTabletServer* ts = tserver(kTserver);
  const string tablet_id = replica->tablet_id();

  // Now that we're on a new WAL segment, wait for some more terms to pass.
  const int64_t kNumExtraTerms = 10;
  int64_t initial_raft_term = replica->consensus()->CurrentTerm();
  int64_t raft_term = initial_raft_term;
  while (raft_term < initial_raft_term + kNumExtraTerms) {
    SleepFor(MonoDelta::FromMilliseconds(10));
    raft_term = replica->consensus()->CurrentTerm();
  }
  // Stop elections so we can achieve a stable quorum and get to a point where
  // we can GC our logs (e.g. we won't GC if there are replicas that need to be
  // caught up).
  FLAGS_consensus_inject_latency_ms_in_notifications = 0;
  ASSERT_EVENTUALLY([&] {
    int64_t gcable_size;
    ASSERT_OK(replica->GetGCableDataSize(&gcable_size));
    ASSERT_GT(gcable_size, 0);
    LOG(INFO) << "GCing logs...";
    ASSERT_OK(replica->RunLogGC());
  });

  // Ensure that we have no writes in our WALs.
  bool has_write_ops;
  NO_FATALS(HasNoWriteOps(ts, tablet_id, &has_write_ops));
  EXPECT_FALSE(has_write_ops);

  LOG(INFO) << "Shutting down the cluster...";
  replica.reset();
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    cluster_->mini_tablet_server(i)->Shutdown();
  }

  // Now prevent elections to reduce consensus logging on the single server.
  FLAGS_raft_heartbeat_interval_ms = 100000;
  LOG(INFO) << "Restarting single tablet server...";
  ASSERT_OK(ts->Restart());
  TServerDetails* ts_details = FindOrDie(ts_map_, ts->uuid());

  // Despite there being no writes, there are no-ops, with which we can advance
  // MVCC's timestamps.
  ASSERT_OK(WaitUntilTabletRunning(ts_details, tablet_id, MonoDelta::FromSeconds(30)));
  replica = tablet_replica_on_ts(kTserver);
  Timestamp cleantime = replica->tablet()->mvcc_manager()->GetCleanTimestamp();
  ASSERT_NE(cleantime, Timestamp::kInitialTimestamp);
}

}  // namespace itest
}  // namespace kudu
