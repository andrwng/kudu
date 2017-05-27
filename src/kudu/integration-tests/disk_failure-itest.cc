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

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/util/path_util.h"
#include "kudu/util/metrics.h"

METRIC_DECLARE_gauge_uint64(data_dirs_failed);

DECLARE_string(block_manager);

namespace kudu {

using client::KuduInsert;
using client::KuduTable;
using client::KuduSession;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;
using tserver::TabletServerIntegrationTestBase;
using tserver::TServerDetails;

// Returns a glob that matches file-block files within 'data_dir'.
// File-block filenames are sixteen characters and fall somewhere under a
// two-character directory within the parent data dir.
string GlobForBlockFileInDataDir(const string& data_dir) {
  if (FLAGS_block_manager == "file") {
    return JoinPathSegments(data_dir, Substitute("$0/**/$1", string(2, '?'), string(16, '?')));
  }
  const vector<string> lbm_globs = { JoinPathSegments(data_dir, Substitute("*.data")),
                                     JoinPathSegments(data_dir, Substitute("*.metadata")) };
  return JoinStrings(lbm_globs, ",");
}

class DiskFailureITest : public TabletServerIntegrationTestBase {};

// This test is set up so each tablet occupies its own disk on each tserver.
// The '|' represents a separation of disks.
//    ts-0      ts-1      ts-2
// [ a | b ] [ b | a ] [ a | b ]
//
// EIOs are triggered on two disks.
//    ts-0      ts-1      ts-2
// [ X | b ] [ X | a ] [ a | b ]
//
// With improper disk-failure handling, this scenario alone would have been
// enough to leave the server with only a single copy of data, as the two
// servers with EIOs would have been shut down entirely.
//
// With proper disk-failure handling, this scenario would be salvagable, and
// data would be replicated on the remaining disks.
//    ts-0       ts-1      ts-2
// [ X | ba ] [ X | ab ] [ a | b ]
TEST_F(DiskFailureITest, DiskFaultSurvivalOnWrite) {
  FLAGS_num_replicas = 3;
  FLAGS_num_tablet_servers = 3;
  const string kTableIdA = "table_a";
  const string kTableIdB = "table_b";
  vector<TServerDetails*> tservers;
  vector<string> ts_flags, master_flags;
  ts_flags = {
    // Flush frequently to trigger writes.
    "--flush_threshold_mb=1",
    "--flush_threshold_secs=1",

    // Ensure a tablet will only store data on a single disk.
    "--fs_target_data_dirs_per_tablet=1"
  };

  vector<ExternalTabletServer*> ext_tservers;
  NO_FATALS(CreateCluster("survivable_cluster", ts_flags, master_flags, /* num_data_dirs */ 2));
  NO_FATALS(CreateClient(&client_));
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(3, tservers.size());
  for (auto* details : tservers) {
    ext_tservers.push_back(cluster_->tablet_server_by_uuid(details->uuid()));
  }
  TestWorkload workload_a(cluster_.get());
  workload_a.set_table_name(kTableIdA);
  workload_a.set_write_pattern(TestWorkload::WritePattern::INSERT_SEQUENTIAL_ROWS);
  workload_a.Setup();
  workload_a.Start();

  // Wait for the tablets to finish writing and find which data dir the tablet
  // put its data.
  // TODO(awong): Once proper handling of failures of the tablet metadata dir
  // is implemented, it will be necessary to change this avoid failures of the
  // tablet metadata dir, as these failures are expected to shut down all
  // tablets unless tablet metadata striping is also implemented.
  vector<string> dirs_with_A_data;
  vector<string> dirs_without_A_data;
  ASSERT_EVENTUALLY([&] {
    int num_servers_with_data = 0;
    dirs_with_A_data.clear();
    dirs_without_A_data.clear();
    for (int ts = 0; ts < tservers.size(); ts++) {
      // Look for the path that has data.
      string path_with_data = "";
      string path_without_data = "";
      for (const string& data_dir : ext_tservers[ts]->data_dirs()) {
        vector<string> files;
        string data_path = JoinPathSegments(data_dir, "data");
        inspect_->ListFilesInDir(data_path, &files);
        LOG(INFO) << data_path << " has " << files.size();
        if (files.size() == 1) {
          // "Empty" directories will only have the instance file.
          path_without_data = data_path;
        } else {
          path_with_data = data_path;
          num_servers_with_data++;
        }
      }
      dirs_with_A_data.emplace_back(path_with_data);
      dirs_without_A_data.emplace_back(path_without_data);
    }
    ASSERT_EQ(tservers.size(), num_servers_with_data);
  });
  workload_a.StopAndJoin();
  WaitForReplicasAndUpdateLocations(kTableIdA);
  ASSERT_GT(tablet_replicas_.size(), 0);
  string tablet_id_a = (*tablet_replicas_.begin()).first;

  // Create a second table.
  TestWorkload workload_b(cluster_.get());
  workload_b.set_table_name(kTableIdB);
  workload_b.set_write_pattern(TestWorkload::WritePattern::INSERT_SEQUENTIAL_ROWS);
  workload_b.Setup();
  WaitForReplicasAndUpdateLocations(kTableIdB);
  string tablet_id_b = (*(++tablet_replicas_.begin())).first;

  // Set flags to fail the appropriate directories.
  ASSERT_OK(cluster_->SetFlag(ext_tservers[0], "follower_unavailable_considered_failed_sec", "10"));
  ASSERT_OK(cluster_->SetFlag(ext_tservers[1], "follower_unavailable_considered_failed_sec", "10"));
  ASSERT_OK(cluster_->SetFlag(ext_tservers[2], "follower_unavailable_considered_failed_sec", "10"));
  ASSERT_OK(cluster_->SetFlag(ext_tservers[0], "suicide_on_eio", "false"));
  ASSERT_OK(cluster_->SetFlag(ext_tservers[1], "suicide_on_eio", "false"));
  ASSERT_OK(cluster_->SetFlag(ext_tservers[0], "env_inject_eio", "1.0"));
  ASSERT_OK(cluster_->SetFlag(ext_tservers[1], "env_inject_eio", "1.0"));
  ASSERT_OK(cluster_->SetFlag(ext_tservers[0], "env_inject_eio_globs",
                              GlobForBlockFileInDataDir(dirs_with_A_data[0])));
  ASSERT_OK(cluster_->SetFlag(ext_tservers[1], "env_inject_eio_globs",
                              GlobForBlockFileInDataDir(dirs_without_A_data[1])));
  LOG(INFO) << "Flags set! Write ops shall henceforth result in EIOs.";

  // Add data, expecting the flushes to fail.
  workload_a.Start();
  workload_b.Start();
  ASSERT_EVENTUALLY([&] {
    int64_t failed_on_ts0;
    int64_t failed_on_ts1;
    ext_tservers[0]->GetInt64Metric(&METRIC_ENTITY_server, nullptr, &METRIC_data_dirs_failed,
        "value", &failed_on_ts0);
    if (failed_on_ts0 != 0) {
      workload_a.StopAndJoin();
    }
    ext_tservers[1]->GetInt64Metric(&METRIC_ENTITY_server, nullptr, &METRIC_data_dirs_failed,
        "value", &failed_on_ts1);
    if (failed_on_ts1 != 0) {
      workload_b.StopAndJoin();
    }
    ASSERT_EQ(1, failed_on_ts0);
    ASSERT_EQ(1, failed_on_ts1);
  });

  // Check that the servers are still alive.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_TRUE(cluster_->tablet_server(i)->IsProcessAlive());
  }

  // Check that the tablet servers agree.
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(120), tablet_servers_, tablet_id_a, 0));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(120), tablet_servers_, tablet_id_b, 0));
}

}  // namespace kudu
