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

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/fs/block_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_gauge_uint64(data_dirs_failed);

namespace kudu {

using cluster::ExternalMiniClusterOptions;
using cluster::ExternalTabletServer;
using fs::BlockManager;
using itest::ExternalMiniClusterFsInspector;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;
using tablet::RowSetMetadata;
using tablet::TabletSuperBlockPB;

const MonoDelta kAgreementTimeout = MonoDelta::FromSeconds(30);

class DiskFailureITest : public ExternalMiniClusterITestBase,
                         public ::testing::WithParamInterface<string> {
 public:

  // Waits for 'ext_tserver' to experience 'target_failed_disks' disk failures.
  void WaitForDiskFailures(const ExternalTabletServer* ext_tserver,
                           int64_t target_failed_disks = 1) const {
    ASSERT_EVENTUALLY([&] {
      int64_t failed_on_ts;
      ASSERT_OK(itest::GetInt64Metric(ext_tserver->bound_http_hostport(),
          &METRIC_ENTITY_server, nullptr, &METRIC_data_dirs_failed, "value", &failed_on_ts));
      ASSERT_EQ(target_failed_disks, failed_on_ts);
    });
  }
};

// Test ensuring that tablet server can be started with failed directories. A
// cluster is started and loaded with some tablets. The tablet server is then
// shut down and restarted. Errors are injected to one of the directories while
// it is shut down.
TEST_P(DiskFailureITest, TestFailDuringServerStartup) {
  // Set up a cluster with three servers with five disks each.
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = 3;
  opts.num_data_dirs = 5;
  opts.block_manager_type = GetParam();
  NO_FATALS(StartClusterWithOpts(opts));
  const int kNumTablets = 5;
  const int kNumRows = 100;

  // Write some data to a tablet. This will spread blocks across all
  // directories.
  TestWorkload write_workload(cluster_.get());
  write_workload.set_num_tablets(kNumTablets);
  write_workload.Setup();
  write_workload.Start();
  ASSERT_EVENTUALLY([&] {
    ASSERT_GT(kNumRows, write_workload.rows_inserted());
  });
  write_workload.StopAndJoin();

  // Ensure the tablets get to a running state.
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  ASSERT_OK(cluster_->WaitForTabletsRunning(ts, kNumTablets, kAgreementTimeout));

  // Introduce flags to fail one of the directories, avoiding the metadata
  // directory, the next time the tablet server starts.
  string failed_dir = ts->data_dirs()[1];
  vector<string> extra_flags = {
      Substitute("--env_inject_eio_globs=$0", JoinPathSegments(failed_dir, "**")),
      "--env_inject_eio=1.0",
      "--crash_on_eio=false",
  };
  ts->mutable_flags()->insert(ts->mutable_flags()->begin(), extra_flags.begin(), extra_flags.end());
  ts->Shutdown();

  // Restart the tablet server with disk failures and ensure it can startup.
  ASSERT_OK(ts->Restart());
  NO_FATALS(WaitForDiskFailures(cluster_->tablet_server(0)));

  // Ensure that the tablets are successfully evicted and copied.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(write_workload.table_name(), ClusterVerifier::AT_LEAST,
                            write_workload.batches_completed()));
}

// Test for KUDU-2202 that ensures that a tablet with data in a failed
// directory will not have its block IDs reused.
TEST_P(DiskFailureITest, TestNoBlockIDReuseIfMissingDirectory) {
  // Set up a basic multi-directory server with some rows that flushes often so
  // we get some blocks.
  if (GetParam() != "log") {
    LOG(INFO) << "Failed or missing directories are only supported by the log "
                 "block manager. Exiting early!";
    return;
  }
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = 1;
  opts.num_data_dirs = 2;
  opts.extra_tserver_flags = { "--flush_threshold_secs=1",
                               "--flush_threshold_mb=1" };
  opts.block_manager_type = GetParam();
  NO_FATALS(StartClusterWithOpts(opts));

  // Write to the tserver and wait for some blocks to be written.
  const auto StartSingleTabletWorkload = [&] (const string& table_name) {
    TestWorkload* write_workload(new TestWorkload(cluster_.get()));
    write_workload->set_table_name(table_name);
    write_workload->set_num_tablets(1);
    write_workload->set_num_replicas(1);
    // Write sequentially so we don't have to collect delta blocks.
    write_workload->set_write_pattern(TestWorkload::INSERT_SEQUENTIAL_ROWS);
    write_workload->Setup();
    write_workload->Start();
    return write_workload;
  };

  unique_ptr<TestWorkload> write_workload(StartSingleTabletWorkload("foo"));
  unique_ptr<ExternalMiniClusterFsInspector> inspect(
      new ExternalMiniClusterFsInspector(cluster_.get()));
  vector<string> tablets = inspect->ListTabletsOnTS(0);
  ASSERT_EQ(1, tablets.size());
  const string tablet_id = tablets[0];

  // Get the blocks for a given tablet.
  const auto BlocksForTablet = [&] (const string& tablet_id, vector<BlockId>* block_ids) {
    TabletSuperBlockPB pb;
    RETURN_NOT_OK(inspect->ReadTabletSuperBlockOnTS(0, tablet_id, &pb));
    for (const auto& rowset_pb : pb.rowsets()) {
      for (const auto& col_pb : rowset_pb.columns()) {
        block_ids->push_back(BlockId::FromPB(col_pb.block()));
      }
      if (rowset_pb.has_adhoc_index_block()) {
        block_ids->push_back(BlockId::FromPB(rowset_pb.adhoc_index_block()));
      }
      if (rowset_pb.has_bloom_block()) {
        block_ids->push_back(BlockId::FromPB(rowset_pb.bloom_block()));
      }
    }
    return Status::OK();
  };

  // Collect the blocks for the tablet.
  vector<BlockId> block_ids;
  ASSERT_EVENTUALLY([&] () {
    ASSERT_OK(BlocksForTablet(tablet_id, &block_ids));
    ASSERT_TRUE(!block_ids.empty());
  });
  write_workload->StopAndJoin();
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  ts->Shutdown();

  // Now fail a data directory. The server will start up not read the blocks
  // from the failed disk, and should still avoid the failed tablet's blocks.
  string failed_dir = ts->data_dirs()[1];
  vector<string> extra_flags = {
      Substitute("--env_inject_eio_globs=$0", JoinPathSegments(failed_dir, "**")),
      "--env_inject_eio=1.0",
      "--crash_on_eio=false"
  };
  ts->mutable_flags()->insert(ts->mutable_flags()->begin(), extra_flags.begin(), extra_flags.end());
  ASSERT_OK(ts->Restart());
  NO_FATALS(WaitForDiskFailures(cluster_->tablet_server(0)));

  // Create a new tablet and collect its blocks.
  write_workload.reset(StartSingleTabletWorkload("bar"));
  tablets = inspect->ListTabletsOnTS(0);
  ASSERT_EQ(2, tablets.size());
  const string& new_tablet_id = tablets[0] == tablet_id ? tablets[1] : tablets[0];
  vector<BlockId> new_block_ids;
  ASSERT_EVENTUALLY([&] () {
    ASSERT_OK(BlocksForTablet(new_tablet_id, &new_block_ids));
    ASSERT_TRUE(!new_block_ids.empty());
  });

  // Compare the tablets' block IDs and ensure there is no overlap.
  LOG(INFO) << "First tablet's set of blocks: " << BlockId::JoinStrings(block_ids);
  LOG(INFO) << "Second tablet's set of blocks: " << BlockId::JoinStrings(new_block_ids);
  vector<BlockId> block_id_intersection;
  std::set_intersection(block_ids.begin(), block_ids.end(),
                        new_block_ids.begin(), new_block_ids.end(),
                        std::back_inserter(block_id_intersection));
  LOG(INFO) << "Intersection: " << BlockId::JoinStrings(block_id_intersection);
  ASSERT_TRUE(block_id_intersection.empty());
}

INSTANTIATE_TEST_CASE_P(DiskFailure, DiskFailureITest,
    ::testing::ValuesIn(BlockManager::block_manager_types()));

}  // namespace kudu
