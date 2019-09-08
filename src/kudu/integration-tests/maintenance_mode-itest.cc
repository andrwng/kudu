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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_entity(server);
METRIC_DECLARE_counter(tablet_copy_bytes_sent);

using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::ExternalTabletServer;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::HealthReportPB;
using kudu::consensus::IncludeHealthReport;
using kudu::master::MasterServiceProxy;
using kudu::master::SetTServerStateRequestPB;
using kudu::master::SetTServerStateResponsePB;
using kudu::master::TServerStatePB;
using std::shared_ptr;
using std::string;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace itest {

// Set a low unavailability timeout so replicas are considered failed and can
// be re-replicated more quickly.
static const vector<string> kTServerFlags = {
  "--raft_heartbeat_interval_ms=100",
  "--follower_unavailable_considered_failed_sec=3",
};

class MaintenanceModeITest : public ExternalMiniClusterITestBase {
 public:
  void SetUp() override {
    ExternalMiniClusterITestBase::SetUp();
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    opts.extra_tserver_flags = kTServerFlags;
    NO_FATALS(StartClusterWithOpts(std::move(opts)));
  }

  // Sets the tserver state for the given 'uuid' to 'state'.
  Status SetTServerState(const string& uuid,
                         const TServerStatePB::State& state) {
    SetTServerStateRequestPB req;
    SetTServerStateResponsePB resp;
    TServerStatePB* tserver_state = req.mutable_tserver_state();
    tserver_state->set_uuid(uuid);
    tserver_state->set_state(state);
    rpc::RpcController rpc;
    return cluster_->master_proxy()->SetTServerState(req, &resp, &rpc);
  }

  // Return the number of bytes copied for the cluster.
  Status GetNumBytesCopied(int64_t* num_bytes_copied) {
    int64_t total = 0;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      int64_t bytes_for_ts = 0;
      ExternalTabletServer* tserver = cluster_->tablet_server(i);
      if (tserver->IsShutdown()) {
        continue;
      }
      RETURN_NOT_OK(itest::GetInt64Metric(tserver->bound_http_hostport(), &METRIC_ENTITY_server,
          /*entity_id=*/nullptr, &METRIC_tablet_copy_bytes_sent, "value", &bytes_for_ts));
      total += bytes_for_ts;
    }
    *num_bytes_copied = total;
    return Status::OK();
  }

  // Return the number of failed replicas there are in the cluster, according
  // to the tablet leaders.
  Status GetNumFailedReplicas(const unordered_map<string, TServerDetails*>& ts_map,
                              int* num_replicas_failed) {
    int num_failed = 0;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      ExternalTabletServer* tserver = cluster_->tablet_server(i);
      if (tserver->IsShutdown()) {
        continue;
      }
      const string& uuid = tserver->uuid();
      const TServerDetails* ts_details = FindOrDie(ts_map, uuid);
      vector<string> tablet_ids;
      RETURN_NOT_OK(ListRunningTabletIds(ts_details, MonoDelta::FromSeconds(5), &tablet_ids));
      for (const auto& tablet_id : tablet_ids) {
        ConsensusStatePB consensus_state;
        RETURN_NOT_OK(GetConsensusState(ts_details, tablet_id, MonoDelta::FromSeconds(5),
            IncludeHealthReport::INCLUDE_HEALTH_REPORT, &consensus_state));
        // Only consider the health states reported by the leaders.
        if (consensus_state.leader_uuid() != uuid) {
          continue;
        }
        // Go through all the peers and tally up any that are failed.
        const auto& committed_config = consensus_state.committed_config();
        for (int p = 0; p < committed_config.peers_size(); p++) {
          const auto& peer = committed_config.peers(p);
          if (peer.has_health_report() &&
              peer.health_report().overall_health() == HealthReportPB::FAILED) {
            num_failed++;
          }
        }
      }
    }
    *num_replicas_failed = num_failed;
    return Status::OK();
  }
 protected:
};

// Test that placing a tablet server in maintenance mode leads to the replicas
// on that server not being re-replicated.
TEST_F(MaintenanceModeITest, TestMaintenanceModeDoesntRereplicate) {
  // This test will sleep a bit to ensure various properties after the master
  // has had some time to receive heartbeats.
  SKIP_IF_SLOW_NOT_ALLOWED();
  const MonoDelta kDurationForSomeHeartbeats = MonoDelta::FromSeconds(3);
  const int kNumTablets = 6;

  // Create the table with three tablet servers and then add one so we're
  // guaranteed that the replicas are all on the first three servers.
  TestWorkload create_table(cluster_.get());
  create_table.set_num_tablets(kNumTablets);
  create_table.Setup();
  create_table.Start();
  // Add a server so there's one we could move to after bringing down a
  // tserver.
  ASSERT_OK(cluster_->AddTabletServer());
  const auto& addr = cluster_->master(0)->bound_rpc_addr();
  shared_ptr<MasterServiceProxy> m_proxy(
      new MasterServiceProxy(cluster_->messenger(), addr, addr.host()));
  unordered_map<string, TServerDetails*> ts_map;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(CreateTabletServerMap(m_proxy, cluster_->messenger(), &ts_map));
    auto cleanup = MakeScopedCleanup([&] {
      STLDeleteValues(&ts_map);
    });
    ASSERT_EQ(cluster_->num_tablet_servers(), ts_map.size());
    cleanup.cancel();
  });
  ValueDeleter d(&ts_map);

  int num_failed = 0;
  ASSERT_OK(GetNumFailedReplicas(ts_map, &num_failed));
  ASSERT_EQ(0, num_failed);

  // Send a request to put one of the nodes into maintenance mode.
  const string& maintenance_uuid = ts_map.begin()->first;
  ASSERT_OK(SetTServerState(maintenance_uuid, TServerStatePB::MAINTENANCE_MODE));

  // Bringing the tablet server down shouldn't lead to re-replication.
  ExternalTabletServer* maintenance_ts = cluster_->tablet_server_by_uuid(maintenance_uuid);
  NO_FATALS(maintenance_ts->Shutdown());

  // Wait for the failure to be recognized by the other replicas.
  ASSERT_EVENTUALLY([&] {
    int num_failed;
    ASSERT_OK(GetNumFailedReplicas(ts_map, &num_failed));
    ASSERT_EQ(kNumTablets, num_failed);
  });
  // Now wait a bit for this failure to make its way to the master. The failure
  // shouldn't lead to any re-replication.
  SleepFor(kDurationForSomeHeartbeats);
  int64_t num_bytes_copied = 0;
  ASSERT_OK(GetNumBytesCopied(&num_bytes_copied));
  ASSERT_EQ(0, num_bytes_copied);
  NO_FATALS(create_table.StopAndJoin());

  // Restarting the masters shouldn't lead to re-replication either, even
  // though the tablet server is still down.
  NO_FATALS(cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY));
  ASSERT_OK(cluster_->master()->Restart());
  SleepFor(kDurationForSomeHeartbeats);
  ASSERT_OK(GetNumBytesCopied(&num_bytes_copied));
  ASSERT_EQ(0, num_bytes_copied);

  // Now bring the server back up and wait for it to become healthy. It should
  // be able to do this without tablet copies.
  ASSERT_OK(maintenance_ts->Restart());
  ASSERT_EVENTUALLY([&] {
    int num_failed;
    ASSERT_OK(GetNumFailedReplicas(ts_map, &num_failed));
    ASSERT_EQ(0, num_failed);
  });
  // Since our server is healthy, leaving maintenance mode shouldn't trigger
  // any re-replication either.
  ASSERT_OK(SetTServerState(maintenance_uuid, TServerStatePB::NONE));
  SleepFor(kDurationForSomeHeartbeats);
  ASSERT_OK(GetNumBytesCopied(&num_bytes_copied));
  ASSERT_EQ(0, num_bytes_copied);

  // Now set maintenance mode, bring the tablet server down, and then exit
  // maintenance mode without brining the tablet server back up. This should
  // result in tablet copies.
  ASSERT_OK(SetTServerState(maintenance_uuid, TServerStatePB::MAINTENANCE_MODE));
  NO_FATALS(maintenance_ts->Shutdown());
  ASSERT_EVENTUALLY([&] {
    int num_failed;
    ASSERT_OK(GetNumFailedReplicas(ts_map, &num_failed));
    ASSERT_EQ(kNumTablets, num_failed);
  });
  ASSERT_OK(SetTServerState(maintenance_uuid, TServerStatePB::NONE));
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(GetNumBytesCopied(&num_bytes_copied));
    ASSERT_LT(0, num_bytes_copied);
  });
}

} // namespace itest
} // namespace kudu
