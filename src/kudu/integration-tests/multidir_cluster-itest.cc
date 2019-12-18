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
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

using client::KuduSchema;
using cluster::ExternalMiniClusterOptions;
using cluster::ExternalTabletServer;
using env_util::ListDirsInDir;
using env_util::ListFilesInDir;
using std::map;
using std::string;
using std::vector;

class MultiDirClusterITest : public ExternalMiniClusterITestBase {};

TEST_F(MultiDirClusterITest, TestBasicMultiDirCluster) {
  const uint32_t kNumDataDirs = 3;
  const uint32_t kNumWalDirs = 3;
  vector<string> ts_flags = {
    // Flush frequently to trigger writes.
    "--flush_threshold_mb=1",
    "--flush_threshold_secs=1",

    // Spread tablet data across all data dirs.
    "--fs_target_data_dirs_per_tablet=0"
  };

  ExternalMiniClusterOptions opts;
  opts.extra_tserver_flags = std::move(ts_flags);
  opts.num_tablet_servers = 1;
  opts.num_data_dirs = kNumDataDirs;
  opts.num_wal_dirs = kNumWalDirs;
  NO_FATALS(StartClusterWithOpts(opts));
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  TestWorkload work(cluster_.get());
  work.set_num_replicas(1);
  work.set_num_tablets(3);
  work.Setup();

  // Check that all daemons have the expected number of directories.
  ASSERT_EQ(kNumDataDirs, cluster_->master()->data_dirs().size());
  ASSERT_EQ(kNumDataDirs, ts->data_dirs().size());
  ASSERT_EQ(kNumWalDirs, ts->wal_dirs().size());

  // Take an initial snapshot of the number of files in each directory.
  map<string, int> num_files_in_each_dir;
  for (const string& data_dir : ts->data_dirs()) {
    string data_path = JoinPathSegments(data_dir, "data");
    vector<string> files;
    ASSERT_OK(ListFilesInDir(env_, data_path, &files));
    InsertOrDie(&num_files_in_each_dir, data_dir, files.size());
  }

  for (const string& wal_dir : ts->wal_dirs()) {
    string wal_path = JoinPathSegments(wal_dir, "wals");
    vector<string> files;
    ASSERT_OK(ListFilesInDir(env_, wal_path, &files));
    ASSERT_EQ(2, files.size());
    ASSERT_OK(ListDirsInDir(env_, wal_path, &files));
    ASSERT_EQ(1, files.size());
  }

  work.Start();
  ASSERT_EVENTUALLY([&] {
    // Check that files are being written to more than one directory.
    int num_dirs_added_to = 0;
    for (const string& data_dir : ts->data_dirs()) {
      string data_path = JoinPathSegments(data_dir, "data");
      vector<string> files;
      ListFilesInDir(env_, data_path, &files);
      int* num_files_before_insert = FindOrNull(num_files_in_each_dir, data_dir);
      ASSERT_NE(nullptr, num_files_before_insert);
      if (*num_files_before_insert < files.size()) {
        num_dirs_added_to++;
      }
    }
    // Block placement should guarantee that more than one data dir will have
    // data written to it.
    ASSERT_GT(num_dirs_added_to, 1);
    // Check segment and index file in WAL directories num, there is at least one
    // file under every WAL directory.
    vector<string> wal_files;
    ASSERT_OK(ListDirsInDir(env_, JoinPathSegments(ts->wal_dirs()[0], "wals"), &wal_files));
    ASSERT_FALSE(wal_files.empty());
  });
  work.StopAndJoin();
}

TEST_F(MultiDirClusterITest, TestBasicMultiWalDirCluster) {
  const uint32_t kNumWalDirs = 4;

  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = 3;
  opts.num_wal_dirs = kNumWalDirs;
  NO_FATALS(StartClusterWithOpts(opts));
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  ASSERT_EQ(kNumWalDirs, ts->wal_dirs().size());

  // Try to create a table with 4 tablets.
  gscoped_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  auto client_schema = KuduSchema::FromSchema(GetSimpleTestSchema());
  ASSERT_OK(table_creator->table_name("test1")
            .schema(&client_schema)
            .set_range_partition_columns({})
            .add_hash_partitions({ "key" }, kNumWalDirs)
            .wait(false)
            .Create());

  // Sleep until we've seen a couple retries on our live server.
  bool in_progress = true;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(client_->IsCreateTableInProgress("test1", &in_progress));
    ASSERT_FALSE(in_progress);
  });

  // Check that all daemons have the expected number of wal directories.
  for (const string& wal_dir : ts->wal_dirs()) {
    string wal_path = JoinPathSegments(wal_dir, "wals");
    vector<string> files;
    ASSERT_OK(ListFilesInDir(env_, wal_path, &files));
    // Every TS has 4 tablets, four WAL dirs, each has only one tablet.
    ASSERT_EQ(2, files.size());
    ASSERT_OK(ListDirsInDir(env_, wal_path, &files));
    ASSERT_EQ(1, files.size());
  }

  // Try to create a table with 40 tablets.
  gscoped_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
  auto schema = KuduSchema::FromSchema(GetSimpleTestSchema());
  ASSERT_OK(creator->table_name("test2")
            .schema(&schema)
            .set_range_partition_columns({})
            .add_hash_partitions({ "key" }, kNumWalDirs * 10)
            .wait(false)
            .Create());
  in_progress = true;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(client_->IsCreateTableInProgress("test2", &in_progress));
    ASSERT_FALSE(in_progress);
  });

  // Check that all daemons have the expected number of wal directories.
  for (const string& wal_dir : ts->wal_dirs()) {
    string wal_path = JoinPathSegments(wal_dir, "wals");
    vector<string> files;
    ASSERT_OK(ListFilesInDir(env_, wal_path, &files));
    ASSERT_EQ(12, files.size());
    ASSERT_OK(ListDirsInDir(env_, wal_path, &files));
    ASSERT_EQ(11, files.size());
  }

  // Delete the table with 40 tablets
  ASSERT_OK(client_->DeleteTable("test2"));
  ASSERT_OK(inspect_->WaitForReplicaCount(12));

  // Check that all daemons have the expected number of wal directories.
  for (const string& wal_dir : ts->wal_dirs()) {
    string wal_path = JoinPathSegments(wal_dir, "wals");
    vector<string> files;
    ASSERT_OK(ListFilesInDir(env_, wal_path, &files));
    ASSERT_EQ(2, files.size());
    ASSERT_OK(ListDirsInDir(env_, wal_path, &files));
    ASSERT_EQ(1, files.size());
  }

  // Delete the table with 4 tablets
  ASSERT_OK(client_->DeleteTable("test1"));
  ASSERT_OK(inspect_->WaitForReplicaCount(0));

  // Check that all daemons have the expected number of wal directories.
  for (const string& wal_dir : ts->wal_dirs()) {
    string wal_path = JoinPathSegments(wal_dir, "wals");
    vector<string> files;
    ASSERT_OK(ListFilesInDir(env_, wal_path, &files));
    ASSERT_EQ(1, files.size());
    ASSERT_OK(ListDirsInDir(env_, wal_path, &files));
    ASSERT_EQ(0, files.size());
  }
}

}  // namespace kudu
