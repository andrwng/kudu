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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/write_op.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/fs/block_manager.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_gauge_uint64(data_dirs_failed);

namespace kudu {

using client::KuduUpsert;
using client::KuduTable;
using client::KuduSession;
using client::sp::shared_ptr;
using cluster::ExternalMiniClusterOptions;
using cluster::ExternalTabletServer;
using fs::BlockManager;
using std::map;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

typedef map<string, int> FilesPerDiskMap;
typedef map<ExternalTabletServer*, FilesPerDiskMap> FilesPerDiskPerTserverMap;

const MonoDelta kAgreementTimeout = MonoDelta::FromSeconds(30);

// Flags that specify a small disk-group size and ensure the servers trigger IO
// quickly.
const vector<string> kDefaultFlags = {
  // Flush frequently to trigger writes.
  "--flush_threshold_mb=1",
  "--flush_threshold_secs=1",

  // Ensure a tablet will only store data on a single disk.
  "--fs_target_data_dirs_per_tablet=1",
};

class DiskFailureITest : public ExternalMiniClusterITestBase,
                         public ::testing::WithParamInterface<string> {
 public:
  // Returns a glob that matches the block files within 'data_dir'.
  string GlobForBlocksInDataDir(const string& data_dir) const {
    if (GetParam() == "file") {
      return JoinPathSegments(data_dir, Substitute("$0/$0/$0/$1", string(2, '?'), string(16, '?')));
    }
    const vector<string> lbm_globs = { JoinPathSegments(data_dir, Substitute("*.data")),
                                      JoinPathSegments(data_dir, Substitute("*.metadata")) };
    return JoinStrings(lbm_globs, ",");
  }

  // Sets flags to ensure EIOs will not crash the server.
  void SetServerSurvivalFlags() {
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(i), "crash_on_eio", "false"));
    }
  }

  // Returns the number of files in each data dir in each tablet server.
  FilesPerDiskPerTserverMap GetFilesPerTserver() {
    FilesPerDiskPerTserverMap counts_per_tserver;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      // Look for the path that has data.
      const auto& ext_tserver = cluster_->tablet_server(i);
      FilesPerDiskMap& counts_per_dir = LookupOrInsert(&counts_per_tserver, ext_tserver, {});
      for (const string& data_dir : ext_tserver->data_dirs()) {
        vector<string> files;
        string data_path = JoinPathSegments(data_dir, "data");
        inspect_->ListFilesInDir(data_path, &files);
        LOG(INFO) << data_path << " has " << files.size() << " files";
        InsertOrDie(&counts_per_dir, data_path, files.size());
      }
    }
    return counts_per_tserver;
  }

  // Runs a function and returns a list of the data dir paths written to and
  // not written to during the call. After iterating through all tablet servers
  // in 'cluster_', exactly 'target_written_dirs' must have been written.
  void GetDataDirsWrittenToByFunction(const std::function<void(void)>& f,
                                      int target_written_dirs,
                                      vector<string>* dirs_written,
                                      vector<string>* dirs_not_written = nullptr) {
    FilesPerDiskPerTserverMap counts_before_workload = GetFilesPerTserver();
    f();
    vector<string> dirs_not_written_default;
    if (dirs_not_written == nullptr) {
      dirs_not_written = &dirs_not_written_default;
    }
    ASSERT_EVENTUALLY([&] {
      FilesPerDiskPerTserverMap counts_after_workload = GetFilesPerTserver();
      dirs_written->clear();
      dirs_not_written->clear();
      int total_num_dirs = 0;
      // Go through the counts and see where they differ.
      for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
        const auto& ext_tserver = cluster_->tablet_server(i);
        total_num_dirs += ext_tserver->data_dirs().size();
        FilesPerDiskMap counts_per_dir_before = counts_before_workload[ext_tserver];
        FilesPerDiskMap counts_per_dir_after = counts_after_workload[ext_tserver];
        for (const auto& e : counts_per_dir_before) {
          uint32_t counts_after = FindOrDie(counts_per_dir_after, e.first);
          uint32_t counts_before = e.second;
          ASSERT_GE(counts_after, counts_before);
          if (counts_after > counts_before) {
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

  // Like GetDataDIrsWrittenToByFunction() but includes appropriate TestWorkload calls.
  void GetDataDirsWrittenToByWorkload(TestWorkload* workload,
                                      int target_written_dirs,
                                      vector<string>* dirs_written,
                                      vector<string>* dirs_not_written = nullptr) {
    std::function<void(void)> f = [&] {
      workload->Setup();
      workload->Start();
    };
    GetDataDirsWrittenToByFunction(f, target_written_dirs, dirs_written, dirs_not_written);
    workload->StopAndJoin();
  }

  // Waits for 'ext_tserver' to experience 'target_failed_disks' disk failures.
  // Setting a larger timeout is more important if the injection rate is low.
  void WaitForDiskFailures(const ExternalTabletServer* ext_tserver,
                           int64_t target_failed_disks = 1) const {
    AssertEventually([&] {
      int64_t failed_on_ts;
      ASSERT_OK(ext_tserver->GetInt64Metric(
          &METRIC_ENTITY_server, nullptr, &METRIC_data_dirs_failed, "value", &failed_on_ts));
      ASSERT_EQ(target_failed_disks, failed_on_ts);
    }, MonoDelta::FromSeconds(120));
    NO_PENDING_FATALS();
  }

  // Sets the specified tablet server to inject failures on the specified 'glob'.
  Status SetInjectionFlags(ExternalTabletServer* ts, const string& glob) {
    RETURN_NOT_OK(cluster_->SetFlag(ts, "env_inject_eio_globs", glob));
    // Don't inject 100% of the time so more codepaths can be tested.
    return cluster_->SetFlag(ts, "env_inject_eio", "0.1");
  }

  // Starts a cluster with three tservers, the specified number of directories,
  // and the parameterized block manager type.
  void StartCluster(int num_data_dirs,
                    vector<string> extra_tserver_flags = kDefaultFlags) {
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    opts.num_data_dirs = num_data_dirs;
    opts.block_manager_type = GetParam();
    opts.extra_tserver_flags = std::move(extra_tserver_flags);
    StartClusterWithOpts(opts);
  }
};

// Test ensuring that tablet server can be started with failed directories. A
// cluster is started and loaded with some tablets. The tablet server is then
// shut down and restarted. Errors are injected to one of the directories while
// it is shut down.
TEST_P(DiskFailureITest, TestFailDuringServerStartup) {
  // Set up a cluster with three servers with five disks each.
  NO_FATALS(StartCluster(/*num_data_dirs=*/ 5, {}));
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
TEST_P(DiskFailureITest, TestFailDuringFlushMRS) {
  const string kTableIdA = "table_a";
  const string kTableIdB = "table_b";
  NO_FATALS(StartCluster(/*num_data_dirs=*/ 2));

  // Insert some initial data.
  // Note that inserting sequentially exercises MRS flushes only.
  TestWorkload workload_a(cluster_.get());
  workload_a.set_table_name(kTableIdA);
  workload_a.set_write_pattern(TestWorkload::WritePattern::INSERT_SEQUENTIAL_ROWS);

  // Wait for the tablets to finish writing and find which data dir the tablet
  // put its data.
  // TODO(awong): Once proper handling of failures of the tablet metadata dir
  // is implemented, it will be necessary to change this to avoid failures of
  // the tablet metadata dir, as these failures are expected to shut down all
  // tablets unless tablet metadata striping is also implemented.
  vector<string> dirs_with_A_data;
  vector<string> dirs_without_A_data;
  NO_FATALS(GetDataDirsWrittenToByWorkload(&workload_a, 3, &dirs_with_A_data,
                                           &dirs_without_A_data));

  // Create a second table.
  TestWorkload workload_b(cluster_.get());
  workload_b.set_table_name(kTableIdB);
  workload_b.set_write_pattern(TestWorkload::WritePattern::INSERT_SEQUENTIAL_ROWS);
  workload_b.Setup();

  // Ensure the two tablets get to a running state.
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  ASSERT_OK(cluster_->WaitForTabletsRunning(ts, 2, kAgreementTimeout));

  // Set flags to fail the appropriate directories.
  NO_FATALS(SetServerSurvivalFlags());
  ASSERT_OK(SetInjectionFlags(cluster_->tablet_server(0),
                              GlobForBlocksInDataDir(dirs_with_A_data[0])));
  ASSERT_OK(SetInjectionFlags(cluster_->tablet_server(1),
                              GlobForBlocksInDataDir(dirs_without_A_data[1])));
  LOG(INFO) << "Flags set! Write ops shall henceforth result in EIOs.";

  // Add data, expecting the flushes to fail.
  workload_a.Start();
  workload_b.Start();
  NO_FATALS(WaitForDiskFailures(cluster_->tablet_server(0)));
  NO_FATALS(WaitForDiskFailures(cluster_->tablet_server(1)));
  workload_a.StopAndJoin();
  workload_b.StopAndJoin();

  // Check that the servers are still alive.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_TRUE(cluster_->tablet_server(i)->IsProcessAlive());
  }

  // Wait for the copies to restabilize.
  SleepFor(MonoDelta::FromSeconds(10));

  // Ensure that the tablets are healthy.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload_a.table_name(), ClusterVerifier::AT_LEAST,
                            workload_a.batches_completed()));
  NO_FATALS(v.CheckRowCount(workload_b.table_name(), ClusterVerifier::AT_LEAST,
                            workload_b.batches_completed()));
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
TEST_P(DiskFailureITest, TestFailDuringFlushDMS) {
  NO_FATALS(StartCluster(/*num_data_dirs=*/ 2));
  const int kNumTabletServers = 3;
  const string kTableId = "test_table";
  client::KuduSchema client_schema(client::KuduSchemaFromSchema(GetSimpleTestSchema()));
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableId)
            .schema(&client_schema)
            .set_range_partition_columns({ "key" })
            .num_replicas(kNumTabletServers)
            .Create());

  const int kStartRow = 0;
  const int kNumRows = 100;

  // Insert an initial payload that will be updated with deltas.
  const std::function<void(void)> UpsertPayload = [&] {
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableId, &table));
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    string payload(10, 'x');
    for (int i = 0; i < kNumRows; i++) {
      unique_ptr<KuduUpsert> upsert(table->NewUpsert());
      KuduPartialRow* row = upsert->mutable_row();
      ASSERT_OK(row->SetInt32(0, i + kStartRow));
      ASSERT_OK(row->SetInt32(1, 0));
      ASSERT_OK(row->SetStringCopy(2, payload));
      ASSERT_OK(session->Apply(upsert.release()));
    }
    ignore_result(session->Flush());
  };
  vector<string> paths_with_data;
  NO_FATALS(GetDataDirsWrittenToByFunction(UpsertPayload, 3, &paths_with_data));

  // Ensure the tablets get to a running state.
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  ASSERT_OK(cluster_->WaitForTabletsRunning(ts, 1, kAgreementTimeout));

  // Iterate through the disks and crash the dir with data on each.
  NO_FATALS(SetServerSurvivalFlags());
  for (int ts = 0; ts < cluster_->num_tablet_servers(); ts++) {
    const auto& tserver = cluster_->tablet_server(ts);
    ASSERT_OK(SetInjectionFlags(tserver, GlobForBlocksInDataDir(paths_with_data[ts])));

    // By repeatedly upserting the same range of rows, we can exercise DMS flushes.
    // Keep upserting until the disk fails.
    int64_t failed_on_ts = 0;
    while (failed_on_ts == 0) {
      NO_FATALS(UpsertPayload());
      ASSERT_OK(tserver->GetInt64Metric(
          &METRIC_ENTITY_server, nullptr, &METRIC_data_dirs_failed, "value", &failed_on_ts));
    }

    // Ensure that the tablets are healthy (i.e. the failed replica should be
    // replicated back to the same server, but on a different disk).
    ClusterVerifier v(cluster_.get());
    NO_FATALS(v.CheckCluster());
    NO_FATALS(v.CheckRowCount(kTableId, ClusterVerifier::AT_LEAST,
                              kNumRows));
  }
}

TEST_P(DiskFailureITest, TestFailDuringScan) {
  NO_FATALS(StartCluster(/*num_data_dirs=*/ 2));

  // Write some data to a table. Since we're only interested in the scan
  // behavior, the contents of the data is not particularly important.
  TestWorkload write_workload(cluster_.get());
  vector<string> paths_with_data;
  NO_FATALS(GetDataDirsWrittenToByWorkload(&write_workload, 3, &paths_with_data));

  // Ensure the tablets get to a running state.
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  ASSERT_OK(cluster_->WaitForTabletsRunning(ts, 1, kAgreementTimeout));

  // Create a read workload before any failures can happen.
  // Use many read threads to increase the likelihood a scan will end up at a
  // failed server.
  TestWorkload read_workload(cluster_.get());
  read_workload.set_num_read_threads(6);
  read_workload.set_num_write_threads(0);
  read_workload.Setup();

  // Fail one of the directories that have data.
  NO_FATALS(SetServerSurvivalFlags());
  ASSERT_OK(SetInjectionFlags(cluster_->tablet_server(0),
                              GlobForBlocksInDataDir(paths_with_data[0])));

  // Read from the table and expect the first tserver to fail.
  read_workload.Start();
  NO_FATALS(WaitForDiskFailures(cluster_->tablet_server(0)));
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(write_workload.table_name(), ClusterVerifier::AT_LEAST,
                            write_workload.batches_completed()));
  read_workload.StopAndJoin();
}

INSTANTIATE_TEST_CASE_P(DiskFailure, DiskFailureITest,
    ::testing::ValuesIn(BlockManager::block_manager_types()));

}  // namespace kudu
