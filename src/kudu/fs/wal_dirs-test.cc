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
#include "kudu/util/barrier.h"
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

static const char* kDirNamePrefix = "test_wal_dir";
static const int kNumDirs = 10;

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
  vector<string> GetDirNames(int num_dirs) {
    vector<string> ret;
    for (int i = 0; i < num_dirs; ++i) {
      string dir_name = Substitute("$0-$1", kDirNamePrefix, i);
      ret.push_back(GetTestPath(dir_name));
      bool created;
      CHECK_OK(env_util::CreateDirIfMissing(env_, ret[i], &created));
    }
    return ret;
  }
  vector<string> GetTabletIds(int num_tablets) {
    vector<string> ret;
    for (int i = 0; i < num_tablets; ++i) {
      string tablet_id = Substitute("$0-$1", test_tablet_name_, i);
      ret.push_back(tablet_id);
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
  ASSERT_EQ(wd_manager_->GetRoots().size(), kNumDirs);
  ASSERT_EQ(wd_manager_->dirs().size(), kNumDirs);
  ASSERT_EQ(wd_manager_->GetFailedDirs().size(), 0);
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

  // Now remove the second directory and fail almost all of the other directories,
  // leaving the last intact.
  ASSERT_OK(env_->DeleteRecursively(JoinPathSegments(test_roots_[1], kWalDirName)));
  for (int i = 2; i < kNumDirs - 1; ++i) {
    FLAGS_env_inject_eio_globs = Substitute("$0,$1", FLAGS_env_inject_eio_globs,
                                            JoinPathSegments(test_roots_[i], "**"));
  }
  WalDirManagerOptions opts;
  opts.update_instances = UpdateInstanceBehavior::UPDATE_AND_ERROR_ON_FAILURE;
  ASSERT_OK(WalDirManager::OpenExistingForTests(env_, test_roots_,
      opts, &wd_manager_));
}

TEST_F(WalDirManagerTest, TestLoadFromPB) {
  // Create a PB, delete the dir, then load the dir from the PB.
  WalDirPB orig_pb;
  ASSERT_OK(wd_manager_->CreateWalDir(test_tablet_name_));
  string tablet_dir;
  ASSERT_OK(wd_manager_->GetWalDirPB(test_tablet_name_, &orig_pb));
  ASSERT_OK(wd_manager_->FindWalDirByTabletId(test_tablet_name_, &tablet_dir));
  wd_manager_->DeleteWalDir(test_tablet_name_);
  ASSERT_TRUE(wd_manager_->FindWalDirByTabletId(
      test_tablet_name_, &tablet_dir).IsNotFound());
  ASSERT_OK(wd_manager_->LoadWalDirFromPB(test_tablet_name_, orig_pb));
  ASSERT_OK(wd_manager_->FindWalDirByTabletId(test_tablet_name_, &tablet_dir));

  // Ensure that loading from a PB will fail if the WalDirManager already
  // knows about the tablet.
  Status s = wd_manager_->LoadWalDirFromPB(test_tablet_name_, orig_pb);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "tried to load directory for tablet");
}

TEST_F(WalDirManagerTest, TestFindAndRegisterWalDir) {
  // create a tablet directory under each wal dir.
  for (int i = 0; i < kNumDirs; ++i) {
    ASSERT_OK(env_->CreateDir(JoinPathSegments(test_roots_[i], Substitute(
        "wals/$0-$1", test_tablet_name_, i))));
  }

  for (int i = 0; i < kNumDirs; ++i) {
    ASSERT_OK(wd_manager_->FindAndRegisterWalDirOnDisk(Substitute(
        "$0-$1", test_tablet_name_, i)));
  }
  Status s = wd_manager_->FindAndRegisterWalDirOnDisk("no_existed_tablet");
  ASSERT_TRUE(s.IsNotFound());

  for (int i = 0; i < kNumDirs; ++i) {
    ASSERT_OK(env_->CreateDir(JoinPathSegments(test_roots_[kNumDirs - i - 1],
        Substitute("wals/$0-$1", test_tablet_name_, i))));
    wd_manager_->DeleteWalDir(Substitute("$0-$1", test_tablet_name_, i));
  }
  for (int i = 0; i < kNumDirs; ++i) {
    Status s = wd_manager_->FindAndRegisterWalDirOnDisk(Substitute(
        "$0-$1", test_tablet_name_, i));
    ASSERT_STR_CONTAINS(s.ToString(), "has at least two WAL directories");
    ASSERT_TRUE(s.IsCorruption());
  }
  for (int i = 0; i < kNumDirs; ++i) {
    InsertOrDie(&wd_manager_->uuid_idx_by_tablet_, Substitute(
        "$0-$1", test_tablet_name_, i), i);
  }
  for (int i = 0; i < kNumDirs; ++i) {
    Status s = wd_manager_->FindAndRegisterWalDirOnDisk(Substitute(
        "$0-$1", test_tablet_name_, i));
    ASSERT_TRUE(s.IsAlreadyPresent());
  }
}

// Test that concurrently find and register wal dirs for tablets yields the
// expected number of dirs found.
TEST_F(WalDirManagerTest, TestFindAndRegisterDirInParallel) {
  // In parallel, try find and register directories for tablets.
  for (int i = 0; i < kNumDirs; ++i) {
    ASSERT_OK(env_->CreateDir(JoinPathSegments(test_roots_[i], Substitute(
        "wals/$0-$1", test_tablet_name_, i))));
  }
  vector<thread> threads;
  threads.reserve(kNumDirs);
  vector<Status> statuses(kNumDirs);
  vector<string> tablet_ids = GetTabletIds(kNumDirs);
  Barrier b(kNumDirs);
  for (int i = 0; i < kNumDirs; ++i) {
    threads.emplace_back([&, i] {
      b.Wait();
      statuses[i] = wd_manager_->FindAndRegisterWalDirOnDisk(tablet_ids[i]);
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  for (const auto& s : statuses) {
    EXPECT_OK(s);
  }
}

TEST_F(WalDirManagerTest, TestCreateWalDir) {
  WalDirPB orig_pb;
  ASSERT_OK(wd_manager_->CreateWalDir(test_tablet_name_));
  ASSERT_OK(wd_manager_->GetWalDirPB(test_tablet_name_, &orig_pb));

  // Ensure that the WalDirManager will not create a dir for a tablet that
  // it already knows about.
  Status s = wd_manager_->CreateWalDir(test_tablet_name_);
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

TEST_F(WalDirManagerTest, TestDeleteWalDir) {
  ASSERT_OK(wd_manager_->CreateWalDir(test_tablet_name_));
  string tablet_wal_dir;
  ASSERT_OK(wd_manager_->FindWalDirByTabletId(test_tablet_name_, &tablet_wal_dir));
  wd_manager_->DeleteWalDir(test_tablet_name_);
  Status s = wd_manager_->FindWalDirByTabletId(test_tablet_name_, &tablet_wal_dir);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not find wal dir for tablet");
}

// Inject full disk errors a couple different ways and make sure that we can't
// create wal directory.
TEST_F(WalDirManagerTest, TestFullDisk) {
  FLAGS_env_inject_full = 1.0;
  Status s = wd_manager_->CreateWalDir(test_tablet_name_);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not find a healthy path for new tablet");
  FLAGS_env_inject_full = 0;
  // Don't cache device available space.
  FLAGS_fs_wal_dirs_available_space_cache_seconds = 0;
  // Reserved space.
  FLAGS_fs_wal_dirs_reserved_bytes = 1;
  // Free space.
  FLAGS_disk_reserved_bytes_free_for_testing = 0;

  s = wd_manager_->CreateWalDir(test_tablet_name_);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not find a healthy path for new tablet");
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
  Barrier b(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i] {
      b.Wait();
      statuses[i] = wd_manager_->CreateWalDir(tablet_ids[i]);
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  for (const auto& s : statuses) {
    EXPECT_OK(s);
  }
}

TEST_F(WalDirManagerTest, TestFailedDirNotSelected) {
  ASSERT_OK(wd_manager_->CreateWalDir(test_tablet_name_));
  // Fail one of the directories that it is used.
  string tablet_wal_dir;
  ASSERT_OK(wd_manager_->FindWalDirByTabletId(test_tablet_name_, &tablet_wal_dir));
  const int* uuid_idx = FindOrNull(wd_manager_->uuid_idx_by_tablet_, test_tablet_name_);
  ASSERT_TRUE(uuid_idx != nullptr);
  // These calls are idempotent.
  ASSERT_OK(wd_manager_->MarkDirFailed(*uuid_idx));
  ASSERT_OK(wd_manager_->MarkDirFailed(*uuid_idx));
  ASSERT_OK(wd_manager_->MarkDirFailed(*uuid_idx));
  ASSERT_EQ(1, down_cast<AtomicGauge<uint64_t>*>(
        entity_->FindOrNull(METRIC_wal_dirs_failed).get())->value());

  // Fail the other directory and verify that neither will be used.
  vector<string> tablet_ids = GetTabletIds(kNumDirs);
  for (int i = 0; i < kNumDirs; ++i) {
    ASSERT_OK(wd_manager_->CreateWalDir(tablet_ids[i]));
  }
  string new_wal_dir;
  for (int i = 0; i < kNumDirs; ++i) {
    ASSERT_OK(wd_manager_->FindWalDirByTabletId(tablet_ids[i], &new_wal_dir));
    ASSERT_NE(new_wal_dir, tablet_wal_dir);
  }
}

TEST_F(WalDirManagerTest, TestLoadBalancingDistribution) {
  const double kNumTablets = 20;

  // Add 'kNumTablets' tablets, each with groups of size
  // 'FLAGS_fs_target_data_dirs_per_tablet'.
  for (int tablet_idx = 0; tablet_idx < kNumTablets; ++tablet_idx) {
    ASSERT_OK(wd_manager_->CreateWalDir(Substitute("$0-$1", test_tablet_name_,
        tablet_idx)));
  }
  const double kMeanTabletsPerDir = kNumTablets / kNumDirs;

  // Calculate the standard deviation of the number of tablets per disk.
  // If tablets are evenly spread across directories, this should be small.
  for (const auto& e : wd_manager_->tablets_by_uuid_idx_map_) {
    ASSERT_EQ(kMeanTabletsPerDir, e.second.size());
  }
}

TEST_F(WalDirManagerTest, TestLoadBalancingBias) {
  // Shows that block placement will tend to favor directories with less load.
  // First add a set of tablets for skew. Then add more tablets and check that
  // there's still roughly a uniform distribution.

  // Start with some wal directories that has some tablets.
  // Number of tablets (pre-replication) added after the skew tablets.
  // This configuration will proceed with 5 directories, total 10 tablets.
  const int kTabletsSkewedDir = 10;
  const int kNumSkewedDirs = 5;
  const string kSkewTabletPrefix = "skew_tablet";

  // Add tablets to each skewed directory.
  for (int skew_tablet_idx = 0; skew_tablet_idx < kTabletsSkewedDir; ++skew_tablet_idx) {
    int uuid_idx = skew_tablet_idx % kNumSkewedDirs;
    string skew_tablet = Substitute("$0-$1", kSkewTabletPrefix, skew_tablet_idx);
    InsertOrDie(&wd_manager_->uuid_idx_by_tablet_, skew_tablet, uuid_idx);
    InsertOrDie(&FindOrDie(wd_manager_->tablets_by_uuid_idx_map_, uuid_idx), skew_tablet);
  }

  const double kNumAdditionalTablets = 10;
  // Add the additional tablets.
  for (int tablet_idx = 0; tablet_idx < kNumAdditionalTablets; ++tablet_idx) {
    ASSERT_OK(wd_manager_->CreateWalDir(Substitute("$0-$1", test_tablet_name_, tablet_idx)));
  }

  // Calculate the standard deviation of the number of tablets per disk.
  const double kMeanTabletsPerDir = (kTabletsSkewedDir +
      kNumAdditionalTablets) / kNumDirs;
  for (const auto& e : wd_manager_->tablets_by_uuid_idx_map_) {
    ASSERT_EQ(kMeanTabletsPerDir, e.second.size());
  }
}

} // namespace fs
} //namespace kudu
