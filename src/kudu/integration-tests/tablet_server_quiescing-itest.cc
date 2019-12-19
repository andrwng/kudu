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

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_int32(consensus_inject_latency_ms_in_notifications);
DECLARE_int32(scanner_default_batch_size_bytes);
DECLARE_int32(raft_heartbeat_interval_ms);

METRIC_DECLARE_entity(server);
METRIC_DECLARE_gauge_int32(num_leaders);

using kudu::client::KuduClient;
using kudu::itest::GetInt64Metric;
using kudu::tserver::MiniTabletServer;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace itest {

class TServerQuiescingITest : public MiniClusterITestBase {
};

TEST_F(TServerQuiescingITest, TestDoesntCallElections) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  const int kNumReplicas = 3;
  const int kNumTablets = 10;
  FLAGS_raft_heartbeat_interval_ms = 100;
  NO_FATALS(StartCluster(kNumReplicas));

  // Set up a table with some replicas.
  const string kTableName = "foo";
  TestWorkload write_workload(cluster_.get());
  write_workload.set_num_replicas(kNumReplicas);
  write_workload.set_num_tablets(kNumTablets);
  write_workload.Setup();

  auto* ts = cluster_->mini_tablet_server(0);
  vector<string> tablet_ids;
  ASSERT_EVENTUALLY([&] {
    vector<string> tablets = ts->ListTablets();
    ASSERT_EQ(kNumTablets, tablets.size());
    tablet_ids = std::move(tablets);
  });

  const MonoDelta kTimeout = MonoDelta::FromSeconds(10);
  // Wait for all of our relicas to have leaders.
  for (const auto& tablet_id : tablet_ids) {
    TServerDetails* leader_details;
    ASSERT_EVENTUALLY([&] {
      ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_details));
    });
    LOG(INFO) << Substitute("Tablet $0 has leader $1", tablet_id, leader_details->uuid());
  }

  LOG(INFO) << Substitute("Quiescing ts $0", ts->uuid());
  ts->server()->set_quiescing(true);

  // Cause a bunch of elections.
  FLAGS_leader_failure_max_missed_heartbeat_periods = 1;
  FLAGS_consensus_inject_latency_ms_in_notifications = FLAGS_raft_heartbeat_interval_ms;

  // Soon enough, elections will occur, and our quiescing server will cease to
  // be leader. 
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, ts->server()->tablet_manager()->num_leaders_->value());
  });

  // When we stop quiescing the server, we should eventually see some
  // leadership return to the server.
  ts->server()->set_quiescing(false);
  ASSERT_EVENTUALLY([&] {
    ASSERT_LT(0, ts->server()->tablet_manager()->num_leaders_->value());
  });
}

TEST_F(TServerQuiescingITest, TestDoesntAllowNewScans) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  const int kNumReplicas = 3;
  // Set a tiny batch size to encourage many batches for a single scan. This
  // will emulate long-running scans.
  FLAGS_scanner_default_batch_size_bytes = 1;
  NO_FATALS(StartCluster(kNumReplicas));

  // Set up a table with some replicas.
  const string kTableName = "foo";
  TestWorkload rw_workload(cluster_.get());
  // The goal of quiescing is that we don't have to run fault tolerant scans in
  // order to restart a tablet server without scan disruption -- we'll just
  // need to wait for on-going scans to finish.
  rw_workload.set_scanner_fault_tolerant(false);
  rw_workload.set_num_replicas(kNumReplicas);
  rw_workload.set_num_read_threads(3);
  rw_workload.set_num_write_threads(3);
  // We're not going to be scanning the leaders only, and we're going to
  // restart a server, so there's a good chance our scans may be stale. As
  // such, we'll set our write pattern so our readers don't expect a
  // monotonically increasing number of rows. 
  rw_workload.set_write_pattern(TestWorkload::INSERT_RANDOM_ROWS_WITH_DELETE);
  rw_workload.set_not_found_allowed(true);
  // We're going to be restarting the tablet server programmatically (vs kill
  // -9), so we might get a remote error when the server is shutting down.
  rw_workload.set_remote_error_allowed(true);
  rw_workload.Setup();
  rw_workload.Start();

  // Wait for the scans to begin. By default, the scanners will be random, so
  // we should eventually see some scans on each tablet server.
  ASSERT_EVENTUALLY([&] {
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      auto* ts = cluster_->mini_tablet_server(i);
      ASSERT_LT(0, ts->server()->scanner_manager()->CountActiveScanners());
    }
  });

  // Mark a tablet server as quiescing. It should eventually stop serving scans.
  auto* ts = cluster_->mini_tablet_server(0);
  ts->server()->set_quiescing(true);
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, ts->server()->scanner_manager()->CountActiveScanners());
  });

  // Restarting the quiesced tablet server shouldn't affect the ongoing read workload.
  ts->Shutdown();
  ASSERT_OK(ts->Restart());
  NO_FATALS(rw_workload.StopAndJoin());
}

TEST_F(TServerQuiescingITest, TestDoesntAllowNewScansLeadersOnly) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  const int kNumReplicas = 3;
  FLAGS_raft_heartbeat_interval_ms = 100;
  // Set a tiny batch size to encourage many batches for a single scan. This
  // will emulate long-running scans.
  FLAGS_scanner_default_batch_size_bytes = 1;
  NO_FATALS(StartCluster(kNumReplicas));

  // Set up a table with some replicas.
  const string kTableName = "foo";
  TestWorkload rw_workload(cluster_.get());
  // The goal of quiescing is that we don't have to run fault tolerant scans in
  // order to restart a tablet server without scan disruption -- we'll just
  // need to wait for on-going scans to finish.
  rw_workload.set_scanner_fault_tolerant(false);
  rw_workload.set_num_replicas(kNumReplicas);
  rw_workload.set_num_read_threads(3);
  rw_workload.set_num_write_threads(3);
  // When we scan with leaders only, we don't need to adjust our write pattern
  // to account for the server restart -- our readers are assured that the
  // number of rows read will be monotonically increasing.
  rw_workload.set_scanner_selection(client::KuduClient::LEADER_ONLY);
  rw_workload.set_remote_error_allowed(true);
  rw_workload.Setup();
  rw_workload.Start();

  // Inject a bunch of leader elections to stress leadership changes.
  FLAGS_leader_failure_max_missed_heartbeat_periods = 1;
  FLAGS_consensus_inject_latency_ms_in_notifications = FLAGS_raft_heartbeat_interval_ms;

  // Wait for the scans to begin.
  vector<MiniTabletServer*> ts_with_scans;
  ASSERT_EVENTUALLY([&] {
    vector<MiniTabletServer*> tservers_with_scans;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      auto* ts = cluster_->mini_tablet_server(i);
      if (ts->server()->scanner_manager()->CountActiveScanners() > 0) {
        tservers_with_scans.emplace_back(ts);
      }
    }
    ASSERT_FALSE(tservers_with_scans.empty());
    ts_with_scans = std::move(tservers_with_scans);
  });

  // Mark the tablet servers as quiescing. They should eventually stop serving
  // scans.
  for (auto* ts : ts_with_scans) {
    ts->server()->set_quiescing(true);
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(0, ts->server()->scanner_manager()->CountActiveScanners());
    });
    ts->Shutdown();
    ASSERT_OK(ts->Restart());
  }

  // Restarting the quiesced tablet server shouldn't affect the ongoing read workload.
  NO_FATALS(rw_workload.StopAndJoin());
}

} // namespace itest
} // namespace kudu
