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

#include <string>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_gauge_uint64(data_dirs_failed);

DECLARE_string(block_manager);
DECLARE_double(env_inject_eio);
DECLARE_string(env_inject_eio_globs);
DECLARE_int32(fs_target_data_dirs_per_tablet);
DECLARE_bool(suicide_on_eio);

namespace kudu {
namespace tserver {

using std::string;
using strings::Substitute;
using fs::CreateBlockOptions;
using fs::DataDir;
using fs::DataDirManager;
using tablet::Tablet;

class TSDiskFailureTest : public TabletServerTestBase {
 public:
  virtual void SetUp() override {
    FLAGS_fs_target_data_dirs_per_tablet = 1;
    NO_FATALS(TabletServerTestBase::SetUp());
    NO_FATALS(StartTabletServer(/* num_data_dirs */ 3));
  }

  // Fails a single directory belonging to the tablet.
  void FailSingleTabletDirectory(const string& tablet_id = kTabletId) {
    DataDirManager* dd_manager = mini_server_->server()->fs_manager()->dd_manager();
    DataDir* dir_with_tablet;
    ASSERT_OK(dd_manager->GetNextDataDir(CreateBlockOptions({ tablet_id }), &dir_with_tablet));
    FLAGS_suicide_on_eio = false;
    FLAGS_env_inject_eio = 1.0;
    FLAGS_env_inject_eio_globs = GlobForFilesInDir(dir_with_tablet);
  }

  string GlobForFilesInDir(const DataDir* dir) {
    if (FLAGS_block_manager == "file") {
      return JoinPathSegments(dir->dir(), Substitute("$0/**", string(2, '?')));
    }
    vector<string> lbm_globs = { JoinPathSegments(dir->dir(), "*.data"),
                                 JoinPathSegments(dir->dir(), "*.metadata") };
    return JoinStrings(lbm_globs, ",");
  }

  // When a tablet runs into a disk failure, the tablet replica is failed
  // immediately and shutdown asynchonously.
  void AssertReplicaFailed() {
    LOG(INFO) << TabletStatePB_Name(tablet_replica_->state());
    ASSERT_TRUE(tablet_replica_->state() == tablet::FAILED ||
                tablet_replica_->state() == tablet::QUIESCING ||
                tablet_replica_->state() == tablet::SHUTDOWN ||
                tablet_replica_->state() == tablet::FAILED_AND_SHUTDOWN);
  }
};

// Test disk failure during a write.
TEST_F(TSDiskFailureTest, TestFailDuringFlush) {
  InsertTestRowsDirect(0, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  FailSingleTabletDirectory();

  // Insert an arbitrary number of rows and flush, expecting a failure.
  InsertTestRowsDirect(1, 1);
  Status s = tablet_replica_->tablet()->Flush();
  ASSERT_EQ(EIO, s.posix_code());
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), "INJECTED FAILURE");
  AssertReplicaFailed();
}

// Test disk failure during a read.
TEST_F(TSDiskFailureTest, TestFailDuringScan) {
  InsertTestRowsDirect(0, 1);
  Status s = tablet_replica_->tablet()->Flush();
  ASSERT_OK(s);
  FailSingleTabletDirectory();
  VerifyScanRequestFailure(schema_, TabletServerErrorPB::TABLET_FAILED, "INJECTED FAILURE");
  AssertReplicaFailed();
}

// Test disk failure during compaction.
TEST_F(TSDiskFailureTest, TestFailDuringCompaction) {
  InsertTestRowsDirect(0, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  // Fail during a compaction.
  FailSingleTabletDirectory();
  Status s = tablet_replica_->tablet()->Compact(Tablet::FORCE_COMPACT_ALL);
  ASSERT_EQ(EIO, s.posix_code());
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), "INJECTED FAILURE");
  AssertReplicaFailed();
}

// Test disk failure while opening tablets.
TEST_F(TSDiskFailureTest, TestFailDuringTabletStartup) {
  InsertTestRowsDirect(0, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  DataDirManager* dd_manager = mini_server_->server()->fs_manager()->dd_manager();
  DataDir* dir_with_tablet;
  ASSERT_OK(dd_manager->GetNextDataDir(CreateBlockOptions({ kTabletId }), &dir_with_tablet));
  string glob_for_dir = GlobForFilesInDir(dir_with_tablet);

  ShutdownTablet();

  // Fail the tablet's directories.
  FLAGS_suicide_on_eio = false;
  FLAGS_env_inject_eio = 1.0;
  FLAGS_env_inject_eio_globs = glob_for_dir;
  mini_server_.reset(new MiniTabletServer(GetTestPath("TabletServerTest-fsroot"),
                                          HostPort("127.0.0.1", 0), 3));
  ASSERT_OK(mini_server_->Start());
}

}  // namespace tserver
}  // namespace kudu
