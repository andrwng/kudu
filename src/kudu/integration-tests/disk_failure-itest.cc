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

#include <map>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/util/path_util.h"
#include "kudu/util/metrics.h"

METRIC_DECLARE_gauge_uint64(data_dirs_failed);

DECLARE_string(block_manager);

namespace kudu {

using client::KuduUpsert;
using client::KuduTable;
using client::KuduSession;
using client::sp::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using std::map;
using strings::Substitute;
using tserver::TabletServerIntegrationTestBase;
using tserver::TServerDetails;

typedef map<string, uint32_t> FilesPerDiskMap;
typedef map<ExternalTabletServer*, FilesPerDiskMap> FilesPerDiskPerTserverMap;

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

const vector<string> ts_flags = {
  // Flush frequently to trigger writes.
  "--flush_threshold_mb=1",
  "--flush_threshold_secs=1",

  // Ensure a tablet will only store data on a single disk.
  "--fs_target_data_dirs_per_tablet=1"
};

class DiskFailureITest : public TabletServerIntegrationTestBase {
 public:
  // Sets up a cluster and creates a tablet with three servers with two disks
  // each.
  void SetupDefaultTable(const string& table_id, vector<ExternalTabletServer*>* ext_tservers) {
    FLAGS_num_replicas = 3;
    FLAGS_num_tablet_servers = 3;
    ext_tservers->clear();
    NO_FATALS(CreateCluster("survivable_cluster", ts_flags, {}, /* num_data_dirs */ 2));
    NO_FATALS(CreateClient(&client_));
    if (!table_id.empty()) {
      NO_FATALS(CreateTable(table_id));
    }
    WaitForTSAndReplicas();
    vector<TServerDetails*> tservers;
    AppendValuesFromMap(tablet_servers_, &tservers);
    ASSERT_EQ(3, tservers.size());
    for (auto* details : tservers) {
      ext_tservers->push_back(cluster_->tablet_server_by_uuid(details->uuid()));
    }
  }

  void SetServerSurvivalFlags(vector<ExternalTabletServer*>& ext_tservers) {
    for (auto& ext_tserver : ext_tservers) {
      ASSERT_OK(cluster_->SetFlag(ext_tserver, "follower_unavailable_considered_failed_sec", "10"));
      ASSERT_OK(cluster_->SetFlag(ext_tserver, "suicide_on_eio", "false"));
    }
  }

  // Returns the number of files in each data dir in each tablet server.
  FilesPerDiskPerTserverMap GetFilesPerTserver(const vector<ExternalTabletServer*> ext_tservers) {
    FilesPerDiskPerTserverMap counts_per_tserver;
    for (const auto& ext_tserver : ext_tservers) {
      // Look for the path that has data.
      FilesPerDiskMap& counts_per_dir = LookupOrInsert(&counts_per_tserver, ext_tserver, {});
      for (const string& data_dir : ext_tserver->data_dirs()) {
        vector<string> files;
        string data_path = JoinPathSegments(data_dir, "data");
        inspect_->ListFilesInDir(data_path, &files);
        LOG(INFO) << data_path << " has " << files.size();
        InsertOrDie(&counts_per_dir, data_path, files.size());
      }
    }
    return counts_per_tserver;
  }

  void GetDataDirsWrittenToByFunction(const vector<ExternalTabletServer*> ext_tservers,
                                      std::function<void(void)> f,
                                      int target_written_dirs,
                                      vector<string>* dirs_written,
                                      vector<string>* dirs_not_written = nullptr) {
    FilesPerDiskPerTserverMap counts_before_workload = GetFilesPerTserver(ext_tservers);
    f();
    vector<string> dirs_not_written_default;
    if (dirs_not_written == nullptr) {
      dirs_not_written = &dirs_not_written_default;
    }
    ASSERT_EVENTUALLY([&] {
      FilesPerDiskPerTserverMap counts_after_workload = GetFilesPerTserver(ext_tservers);
      dirs_written->clear();
      dirs_not_written->clear();
      int total_num_dirs = 0;
      // Go through the counts and see where they differ.
      for (ExternalTabletServer* ext_tserver : ext_tservers) {
        total_num_dirs += ext_tserver->data_dirs().size();
        FilesPerDiskMap counts_per_dir_before = counts_before_workload[ext_tserver];
        FilesPerDiskMap counts_per_dir_after = counts_after_workload[ext_tserver];
        for (const auto& e : counts_per_dir_before) {
          uint32_t counts_after = FindOrDie(counts_per_dir_after, e.first);
          uint32_t counts_before = e.second;
          ASSERT_GE(counts_after, counts_before);
          if (counts_after - counts_before > 0) {
            dirs_written->push_back(e.first);
          } else {
            dirs_not_written->push_back(e.first);
          }
        }
      }
      ASSERT_EQ(target_written_dirs, dirs_written->size());
      ASSERT_EQ(total_num_dirs - target_written_dirs, dirs_not_written->size());
    });
  }

  // Runs a workload and returns a list of data dir paths written to and not
  // written to during the workload. After iterating through all ext_tservers,
  // exactly 'target_written_dirs' must have been written.
  void GetDataDirsWrittenTo(const vector<ExternalTabletServer*> ext_tservers,
                            TestWorkload& workload,
                            int target_written_dirs,
                            vector<string>* dirs_written,
                            vector<string>* dirs_not_written = nullptr) {
    std::function<void(void)> f = [&] {
      workload.Setup();
      workload.Start();
    };
    GetDataDirsWrittenToByFunction(
        ext_tservers, f, target_written_dirs, dirs_written, dirs_not_written);
    workload.StopAndJoin();
  }

  // Waits for 'ext_tserver' to experience 'target_failed_disks' disk failures.
  void WaitForDiskFailures(const ExternalTabletServer* ext_tserver,
                           int64_t target_failed_disks = 1) const {
    ASSERT_EVENTUALLY([&] {
      int64_t failed_on_ts;
      ext_tserver->GetInt64Metric(&METRIC_ENTITY_server, nullptr, &METRIC_data_dirs_failed,
          "value", &failed_on_ts);
      ASSERT_EQ(target_failed_disks, failed_on_ts);
    });
  }

  // Inserts an upsert payload of a specific range of rows.
  void UpsertPayload(const string& table_name, int start_row, int num_rows, int size = 10) {
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(table_name, &table));
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    string payload(size, 'x');
    for (int i = 0; i < num_rows; i++) {
      unique_ptr<KuduUpsert> upsert(table->NewUpsert());
      KuduPartialRow* row = upsert->mutable_row();
	  ASSERT_OK(row->SetInt32(0, i + start_row));
	  ASSERT_OK(row->SetInt32(1, 0));
	  ASSERT_OK(row->SetStringCopy(2, payload));
	  ASSERT_OK(session->Apply(upsert.release()));
	  ignore_result(session->Flush());
    }
  }
};

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
  const string kTableIdA = "table_a";
  const string kTableIdB = "table_b";
  vector<ExternalTabletServer*> ext_tservers;
  SetupDefaultTable("", &ext_tservers);
  TestWorkload workload_a(cluster_.get());
  workload_a.set_table_name(kTableIdA);
  workload_a.set_write_pattern(TestWorkload::WritePattern::INSERT_SEQUENTIAL_ROWS);

  // Wait for the tablets to finish writing and find which data dir the tablet
  // put its data.
  // TODO(awong): Once proper handling of failures of the tablet metadata dir
  // is implemented, it will be necessary to change this avoid failures of the
  // tablet metadata dir, as these failures are expected to shut down all
  // tablets unless tablet metadata striping is also implemented.
  vector<string> dirs_with_A_data;
  vector<string> dirs_without_A_data;
  GetDataDirsWrittenTo(ext_tservers, workload_a, 3, &dirs_with_A_data, &dirs_without_A_data);
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
  SetServerSurvivalFlags(ext_tservers);
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
  WaitForDiskFailures(ext_tservers[0]);
  workload_a.StopAndJoin();
  WaitForDiskFailures(ext_tservers[1]);
  workload_b.StopAndJoin();

  // Check that the servers are still alive.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_TRUE(cluster_->tablet_server(i)->IsProcessAlive());
  }

  // Check that the tablet servers agree.
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(120), tablet_servers_, tablet_id_a, 0));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(120), tablet_servers_, tablet_id_b, 0));
}

// This test is set up so a single tablet occupies one of two disks on each
// tserver.
//    ts-0      ts-1      ts-2
// [ a |   ] [   | a ] [ a |   ]
//
// EIOs are triggered on one disk on one tserver at a time.
//    ts-0      ts-1      ts-2
// [ X |   ] [   | a ] [ a |   ]
//
// This will trigger a tablet copy onto a separate disk.
//    ts-0      ts-1      ts-2
// [ X | a ] [   | a ] [ a |   ]
//
// Repeating this for each tserver should result in tablet copies for each and
// disk failures for each.
//    ts-0       ts-1      ts-2
// [ X | a ] [ a | X ] [ X | a ]
TEST_F(DiskFailureITest, TestFailDuringFlushDMS) {
  vector<ExternalTabletServer*> ext_tservers;
  SetupDefaultTable(kTableId, &ext_tservers);
  std::function<void(void)> f = [&] {
    UpsertPayload(kTableId, 0, 100);
  };
  vector<string> paths_with_data;
  GetDataDirsWrittenToByFunction(ext_tservers, f, 3, &paths_with_data);
  string tablet_id = (*tablet_replicas_.begin()).first;

  // Iterate through the disks and crash the dir with data on each.
  SetServerSurvivalFlags(ext_tservers);
  for (int ts = 0; ts < ext_tservers.size(); ts++) {
    ASSERT_OK(cluster_->SetFlag(ext_tservers[ts], "env_inject_eio", "1.0"));
    ASSERT_OK(cluster_->SetFlag(ext_tservers[ts], "env_inject_eio_globs",
        GlobForBlockFileInDataDir(paths_with_data[ts])));
    UpsertPayload(kTableId, 0, 100);
    WaitForDiskFailures(ext_tservers[ts], 1);
    ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(120), tablet_servers_, tablet_id, 0));
  }
}

TEST_F(DiskFailureITest, TestFailDuringCompaction) {
  // Upsert a bunch of rows repeatedly.
  // Stop and switch on disk failures.
  // Check that failures do occur.
  vector<ExternalTabletServer*> ext_tservers;
  SetupDefaultTable(kTableId, &ext_tservers);
  std::function<void(void)> f = [&] {
    UpsertPayload(kTableId, 0, 100);
  };
  vector<string> paths_with_data;
  GetDataDirsWrittenToByFunction(ext_tservers, f, 3, &paths_with_data);
  string tablet_id = (*tablet_replicas_.begin()).first;
  for (int i = 0; i < 10; i++) {
    UpsertPayload(kTableId, 0, 100);
  }
  // TODO(awong): instead wait for server's compact_rs_duration histogram to hit 1.
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(120), tablet_servers_, tablet_id, 0));

  SetServerSurvivalFlags(ext_tservers);
  ASSERT_OK(cluster_->SetFlag(ext_tservers[0], "env_inject_eio", "1.0"));
  ASSERT_OK(cluster_->SetFlag(ext_tservers[0], "env_inject_eio_globs",
      GlobForBlockFileInDataDir(paths_with_data[0])));
  WaitForDiskFailures(ext_tservers[0], 1);
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(120), tablet_servers_, tablet_id, 0));
}

TEST_F(DiskFailureITest, TestFailDuringScan) {
  // Write some data to a table.
  vector<ExternalTabletServer*> ext_tservers;
  SetupDefaultTable(kTableId, &ext_tservers);
  TestWorkload write_workload(cluster_.get());
  write_workload.set_write_pattern(TestWorkload::WritePattern::INSERT_SEQUENTIAL_ROWS);
  vector<string> paths_with_data;
  GetDataDirsWrittenTo(ext_tservers, write_workload, 3, &paths_with_data);
  string tablet_id = (*tablet_replicas_.begin()).first;
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(120), tablet_servers_, tablet_id, 0));
  SleepFor(MonoDelta::FromSeconds(5));

  // Create a read workload before any failures can happen.
  TestWorkload read_workload(cluster_.get());
  read_workload.set_num_read_threads(1);
  read_workload.set_num_write_threads(0);
  read_workload.set_io_error_allowed(true);
  read_workload.Setup();

  // Fail one of the directories that have data.
  SetServerSurvivalFlags(ext_tservers);
  ASSERT_OK(cluster_->SetFlag(ext_tservers[0], "env_inject_eio", "1.0"));
  ASSERT_OK(cluster_->SetFlag(ext_tservers[0], "env_inject_eio_globs",
      GlobForBlockFileInDataDir(paths_with_data[0])));
  
  // Read from the table and expect the first tserver to fail.
  read_workload.Start();
  WaitForDiskFailures(ext_tservers[0]);
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(120), tablet_servers_, tablet_id, 0));
}

TEST_F(DiskFailureITest, TestFailDuringStartup) {
  vector<ExternalTabletServer*> ext_tservers;
  SetupDefaultTable(kTableId, &ext_tservers);
  TestWorkload write_workload(cluster_.get());
  vector<string> paths_with_data;
  GetDataDirsWrittenTo(ext_tservers, write_workload, 3, &paths_with_data);
  string tablet_id = (*tablet_replicas_.begin()).first;
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(120), tablet_servers_, tablet_id, 0));
  SleepFor(MonoDelta::FromSeconds(5));
  ext_tservers[0]->Shutdown();
  vector<string> extra_startup_flags = {
    "follower_unavailable_considered_failed_sec=10",
    "suicide_on_eio=false",
    "--env_inject_eio=1.0",
    Substitute("--env_inject_eio_globs=$0", GlobForBlockFileInDataDir(paths_with_data[0]))
  };
  ASSERT_OK(ext_tservers[0]->Restart(extra_startup_flags));

  WaitForDiskFailures(ext_tservers[0], 1);
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(120), tablet_servers_, tablet_id, 0));
}

}  // namespace kudu
