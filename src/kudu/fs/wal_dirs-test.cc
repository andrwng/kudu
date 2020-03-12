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

#include "kudu/fs/wal_dirs.h"

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/fs/dir_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::set;
using std::string;
using std::thread;
using std::vector;
using std::unique_ptr;
using strings::Substitute;

DECLARE_bool(crash_on_eio);
DECLARE_double(env_inject_eio);
DECLARE_double(env_inject_full);
DECLARE_int32(fs_wal_dirs_available_space_cache_seconds);
DECLARE_int64(disk_reserved_bytes_free_for_testing);
DECLARE_int64(fs_wal_dirs_reserved_bytes);
DECLARE_string(env_inject_eio_globs);
DECLARE_string(env_inject_full_globs);

METRIC_DECLARE_gauge_uint64(wal_dirs_failed);

namespace kudu {
namespace fs {

namespace {

const char* kDirNamePrefix = "test_wal_dir";
const int kNumDirs = 10;

} // anonymous namespace

class WalDirManagerTest : public KuduTest {
 public:
  WalDirManagerTest() :
      test_tablet_name_("test_tablet"),
      entity_(METRIC_ENTITY_server.Instantiate(&registry_, "test")) {}

  virtual void SetUp() override {
    KuduTest::SetUp();
    WalDirManagerOptions opts;
    opts.metric_entity = entity_;
    test_roots_ = GetDirNames(kNumDirs);
    ASSERT_OK(WalDirManager::CreateNewForTests(
        env_, test_roots_, opts, &wd_manager_));
    ASSERT_EQ(wd_manager_->GetRoots().size(), kNumDirs);
    ASSERT_EQ(wd_manager_->dirs().size(), kNumDirs);
    ASSERT_EQ(wd_manager_->GetFailedDirs().size(), 0);
  }

 protected:
  // Returns the paths for the given number of WAL directories.
  vector<string> GetDirNames(int num_dirs) {
    vector<string> ret;
    for (int i = 0; i < num_dirs; ++i) {
      ret.emplace_back(GetTestPath(Substitute("$0-$1", kDirNamePrefix, i)));
      bool created;
      CHECK_OK(env_util::CreateDirIfMissing(env_, ret[i], &created));
    }
    return ret;
  }

  // Returns fake tablet IDs for the given number of tablets.
  vector<string> GetTabletIds(int num_tablets) {
    vector<string> ret;
    for (int i = 0; i < num_tablets; ++i) {
      ret.emplace_back(Substitute("$0-$1", test_tablet_name_, i));
    }
    return ret;
  }

  vector<string> test_roots_;
  const string test_tablet_name_;
  MetricRegistry registry_;
  scoped_refptr<MetricEntity> entity_;
  std::unique_ptr<WalDirManager> wd_manager_;
};

TEST_F(WalDirManagerTest, TestOpenExisted) {
  ASSERT_OK(WalDirManager::OpenExistingForTests(env_, test_roots_,
                                                WalDirManagerOptions(), &wd_manager_));
  ASSERT_EQ(kNumDirs, wd_manager_->GetRoots().size());
  ASSERT_EQ(kNumDirs, wd_manager_->dirs().size());
  ASSERT_EQ(0, wd_manager_->GetFailedDirs().size());
}

// Test ensuring that the directory manager can be opened with failed disks,
// provided it was successfully created.
TEST_F(WalDirManagerTest, TestOpenWithFailedDirs) {
  // Kill the first directory.
  FLAGS_crash_on_eio = false;
  FLAGS_env_inject_eio = 1.0;
  FLAGS_env_inject_eio_globs = JoinPathSegments(test_roots_[0], "**");

  // The directory manager will successfully open with the single failed directory.
  ASSERT_OK(WalDirManager::OpenExistingForTests(env_, test_roots_,
      WalDirManagerOptions(), &wd_manager_));
  set<int> failed_dirs;
  ASSERT_EQ(1, wd_manager_->GetFailedDirs().size());

  // Now fail almost all of the other directories, leaving the last intact.
  for (int i = 1; i < kNumDirs - 1; ++i) {
    FLAGS_env_inject_eio_globs = Substitute("$0,$1", FLAGS_env_inject_eio_globs,
                                            JoinPathSegments(test_roots_[i], "**"));
  }
  // The directory manager should be aware of the new failures.
  ASSERT_OK(WalDirManager::OpenExistingForTests(env_, test_roots_,
      WalDirManagerOptions(), &wd_manager_));
  ASSERT_EQ(kNumDirs - 1, wd_manager_->GetFailedDirs().size());

  // Ensure that when there are no healthy wal directories, the open will
  // yield an error.
  FLAGS_env_inject_eio_globs = JoinStrings(JoinPathSegmentsV(test_roots_, "**"), ",");
  Status s = WalDirManager::OpenExistingForTests(env_, test_roots_,
      WalDirManagerOptions(), &wd_manager_);
  ASSERT_STR_CONTAINS(s.ToString(), "could not open directory manager");
  ASSERT_TRUE(s.IsNotFound());
}

/*
TEST_F(WalDirManagerTest, TestUpdateDirsFromOldVersion) {
  // Remove the "wal_manager_instance" file under "wals" directory.
  string manger_instance_file = JoinPathSegments(
      JoinPathSegments(test_roots_[0], kWalDirName), kWalInstanceMetadataFileName);
  ASSERT_OK(env_->DeleteFile(manger_instance_file));
  for (int i = 1; i < kNumDirs; ++i) {
    ASSERT_OK(env_->DeleteRecursively(JoinPathSegments(test_roots_[i], kWalDirName)));
  }
  WalDirManagerOptions opts;
  opts.update_instances = UpdateInstanceBehavior::UPDATE_AND_ERROR_ON_FAILURE;
  // The directory manager will successfully open with the single failed directory.
  ASSERT_OK(WalDirManager::OpenExistingForTests(env_, test_roots_,
      opts, &wd_manager_));
  ASSERT_EQ(wd_manager_->GetRoots().size(), kNumDirs);
  ASSERT_EQ(wd_manager_->dirs().size(), kNumDirs);
  ASSERT_EQ(wd_manager_->GetFailedDirs().size(), 0);
}
*/

TEST_F(WalDirManagerTest, TestTestUpdateDirsWithFailedDirs) {
  // Kill the first directory.
  FLAGS_crash_on_eio = false;
  FLAGS_env_inject_eio = 1.0;
  FLAGS_env_inject_eio_globs = JoinPathSegments(test_roots_[0], "**");

  // The directory manager will successfully open with the single failed directory.
  ASSERT_OK(WalDirManager::OpenExistingForTests(env_, test_roots_,
      WalDirManagerOptions(), &wd_manager_));
  ASSERT_EQ(1, wd_manager_->GetFailedDirs().size());

  // Remove a directory; a new directory should be populated here upon opening.
  string orig_empty_uuid;
  ASSERT_TRUE(wd_manager_->FindUuidByRoot(test_roots_[1], &orig_empty_uuid));
  ASSERT_FALSE(orig_empty_uuid.empty());
  ASSERT_OK(env_->DeleteRecursively(JoinPathSegments(test_roots_[1], kWalDirName)));

  // Fail almost all the other directories.
  for (int i = 2; i < kNumDirs - 1; ++i) {
    FLAGS_env_inject_eio_globs = Substitute("$0,$1", FLAGS_env_inject_eio_globs,
                                            JoinPathSegments(test_roots_[i], "**"));
  }
  WalDirManagerOptions opts;
  opts.update_instances = UpdateInstanceBehavior::UPDATE_AND_ERROR_ON_FAILURE;
  ASSERT_OK(WalDirManager::OpenExistingForTests(env_, test_roots_, opts, &wd_manager_));

  // Only two directories should be healthy: the remaining healthy directory,
  // and the empty one that was repopulated.
  ASSERT_EQ(kNumDirs - 2, wd_manager_->GetFailedDirs().size());

  // The repopulated directory should have been assigned a new UUID since it's
  // effectively a new directory.
  string new_uuid;
  ASSERT_TRUE(wd_manager_->FindUuidByRoot(test_roots_[1], &new_uuid));
  ASSERT_NE(orig_empty_uuid, new_uuid);
}

TEST_F(WalDirManagerTest, TestLoadFromPB) {
  ASSERT_OK(wd_manager_->RegisterWalDir(test_tablet_name_));

  // Create a PB, delete the dir, then load the dir from the PB.
  WalDirPB orig_pb;
  string orig_tablet_dir;
  ASSERT_OK(wd_manager_->GetWalDirPB(test_tablet_name_, &orig_pb));
  ASSERT_OK(wd_manager_->FindWalDirByTabletId(test_tablet_name_, &orig_tablet_dir));
  wd_manager_->UnregisterWalDir(test_tablet_name_);

  string loaded_tablet_dir;
  Status s = wd_manager_->FindWalDirByTabletId(test_tablet_name_, &loaded_tablet_dir);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_OK(wd_manager_->LoadWalDirFromPB(test_tablet_name_, orig_pb));
  ASSERT_OK(wd_manager_->FindWalDirByTabletId(test_tablet_name_, &loaded_tablet_dir));
  ASSERT_EQ(orig_tablet_dir, loaded_tablet_dir);

  // Ensure that loading from a PB will fail if the WalDirManager already
  // knows about the tablet.
  s = wd_manager_->LoadWalDirFromPB(test_tablet_name_, orig_pb);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "tried to load directory for tablet");
}

TEST_F(WalDirManagerTest, TestFindAndRegisterWalDir) {
  // Create a tablet directory under each wal dir.
  vector<string> tablet_ids = GetTabletIds(kNumDirs);
  vector<string> wal_subdirs = JoinPathSegmentsV(test_roots_, "wals");
  for (int i = 0; i < kNumDirs; i++) {
    ASSERT_OK(env_->CreateDir(JoinPathSegments(wal_subdirs[i], tablet_ids[i])));
    ASSERT_OK(wd_manager_->FindOnDiskDirAndRegister(tablet_ids[i]));
  }
  Status s = wd_manager_->FindOnDiskDirAndRegister("fake_tablet");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // Create another on-disk WAL for each tablet.
  for (int i = 0; i < kNumDirs; i++) {
    ASSERT_OK(env_->CreateDir(JoinPathSegments(wal_subdirs[(i + 1) % kNumDirs],
                                               tablet_ids[i])));
  }
  for (int i = 0; i < kNumDirs; ++i) {
    // Our tablet is already registered, so trying to register a new directory
    // should fail.
    Status s = wd_manager_->FindOnDiskDirAndRegister(tablet_ids[i]);
    ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();

    // Now unregister and try again. This will fail too because there are
    // multiple directories for each tablet.
    wd_manager_->UnregisterWalDir(tablet_ids[i]);
    s = wd_manager_->FindOnDiskDirAndRegister(tablet_ids[i]);
    ASSERT_STR_CONTAINS(s.ToString(), "has multiple registered WAL directories");
    ASSERT_TRUE(s.IsCorruption());
  }
}

// Test that concurrently create wal dirs for tablets yields the expected
// number of dirs added.
TEST_F(WalDirManagerTest, TestCreateDirInParallel) {
  // In parallel, try creating directories for tablets.
  const int kNumThreads = 32;
  vector<thread> threads;
  threads.reserve(kNumThreads);
  vector<Status> statuses(kNumThreads);
  vector<string> tablet_ids = GetTabletIds(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i] {
      statuses[i] = wd_manager_->RegisterWalDir(tablet_ids[i]);
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  for (const auto& s : statuses) {
    EXPECT_OK(s);
  }
}

// Test that concurrently find and register wal dirs for tablets yields the
// expected number of dirs found.
TEST_F(WalDirManagerTest, TestFindAndRegisterDirInParallel) {
  vector<string> tablet_ids = GetTabletIds(kNumDirs);
  for (int i = 0; i < kNumDirs; ++i) {
    ASSERT_OK(env_->CreateDir(JoinPathSegments(
        JoinPathSegments(test_roots_[i], "wals"), tablet_ids[i])));
  }
  vector<thread> threads;
  threads.reserve(kNumDirs);
  vector<Status> statuses(kNumDirs);
  // In parallel, try find and register directories for tablets.
  for (int i = 0; i < kNumDirs; ++i) {
    threads.emplace_back([&, i] {
      statuses[i] = wd_manager_->FindOnDiskDirAndRegister(tablet_ids[i]);
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  for (const auto& s : statuses) {
    EXPECT_OK(s);
  }
}

TEST_F(WalDirManagerTest, TestRegisterWalDir) {
  WalDirPB orig_pb;
  ASSERT_OK(wd_manager_->RegisterWalDir(test_tablet_name_));
  ASSERT_OK(wd_manager_->GetWalDirPB(test_tablet_name_, &orig_pb));

  // Ensure that the WalDirManager will not create a dir for a tablet that
  // it already knows about.
  Status s = wd_manager_->RegisterWalDir(test_tablet_name_);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Tried to create WAL directory for tablet "
                                    "but one is already registered");
  WalDirPB pb;
  ASSERT_OK(wd_manager_->GetWalDirPB(test_tablet_name_, &pb));

  // Verify that the wal directory is unchanged after failing to create an
  // existing tablet.
  ASSERT_EQ(orig_pb.uuid(), pb.uuid());

  // Check that the tablet's WalDirPB spans the right number of dirs.
  int num_dirs_with_tablets = 0;
  for (const auto& e: wd_manager_->tablets_by_uuid_idx_map_) {
    if (!e.second.empty()) {
      ASSERT_EQ(1, e.second.size());
      num_dirs_with_tablets++;
    }
  }
  ASSERT_EQ(1, num_dirs_with_tablets);
  string tablet_wal_dir;
  ASSERT_OK(wd_manager_->FindWalDirByTabletId(test_tablet_name_, &tablet_wal_dir));
}

TEST_F(WalDirManagerTest, TestUnregisterWalDir) {
  ASSERT_OK(wd_manager_->RegisterWalDir(test_tablet_name_));
  string tablet_wal_dir;
  ASSERT_OK(wd_manager_->FindWalDirByTabletId(test_tablet_name_, &tablet_wal_dir));

  // Once the tablet is unregistered in memory, subsequent lookups should fail.
  wd_manager_->UnregisterWalDir(test_tablet_name_);
  Status s = wd_manager_->FindWalDirByTabletId(test_tablet_name_, &tablet_wal_dir);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not find wal dir for tablet");
}

// Inject full disk errors a couple different ways and make sure that we can't
// create wal directory.
TEST_F(WalDirManagerTest, TestFullDisk) {
  FLAGS_env_inject_full = 1.0;
  Status s = wd_manager_->RegisterWalDir(test_tablet_name_);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not find a healthy path for new tablet");
  FLAGS_env_inject_full = 0;
  // Don't cache device available space.
  FLAGS_fs_wal_dirs_available_space_cache_seconds = 0;
  // Reserved space.
  FLAGS_fs_wal_dirs_reserved_bytes = 1;
  // Free space.
  FLAGS_disk_reserved_bytes_free_for_testing = 0;

  s = wd_manager_->RegisterWalDir(test_tablet_name_);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not find a healthy path for new tablet");
}

TEST_F(WalDirManagerTest, TestFailedDirNotSelected) {
  ASSERT_OK(wd_manager_->RegisterWalDir(test_tablet_name_));
  // Fail one of the directories that it is used.
  string failed_dir;
  ASSERT_OK(wd_manager_->FindWalDirByTabletId(test_tablet_name_, &failed_dir));
  const int* uuid_idx = FindOrNull(wd_manager_->uuid_idx_by_tablet_, test_tablet_name_);
  ASSERT_NE(nullptr, uuid_idx);
  ASSERT_OK(wd_manager_->MarkDirFailed(*uuid_idx));

  vector<string> tablet_ids = GetTabletIds(kNumDirs);
  string new_dir;
  for (int i = 0; i < kNumDirs; i++) {
    ASSERT_OK(wd_manager_->RegisterWalDir(tablet_ids[i]));
    ASSERT_OK(wd_manager_->FindWalDirByTabletId(tablet_ids[i], &new_dir));
    ASSERT_NE(new_dir, failed_dir);
  }
}

// WAL directory selection will favor placement into directories with fewer
// tablets. Our tablets should be evenly distributed across disks.
TEST_F(WalDirManagerTest, TestSpreadTabletsAcrossDirs) {
  vector<string> tablet_ids = GetTabletIds(kNumDirs * 2);
  for (const auto& tablet_id : tablet_ids) {
    ASSERT_OK(wd_manager_->RegisterWalDir(tablet_id));
  }
  for (int i = 0; i < kNumDirs; i++) {
    ASSERT_EQ(2, wd_manager_->FindTabletsByDirUuidIdx(i).size());
  }
}

} // namespace fs
} //namespace kudu
