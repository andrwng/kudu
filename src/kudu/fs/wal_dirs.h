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

#pragma once

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/fs/dir_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

namespace kudu {

class Env;
class MetricEntity;
class ThreadPool;
class WalDirPB;

namespace fs {

class DirInstanceMetadataFile;

const char kWalInstanceMetadataFileName[] = "wal_manager_instance";
const char kWalManagerType[] = "wal_manager";
const char kWalDirName[] = "wals";

struct WalDirMetrics : public DirMetrics {
  explicit WalDirMetrics(const scoped_refptr<MetricEntity>& metric_entity);
};

// Representation of a WAL directory in use by the WAL manager.
class WalDir : public Dir {
 public:
  WalDir(Env* env,
         DirMetrics* metrics,
         FsType fs_type,
         std::string dir,
         std::unique_ptr<DirInstanceMetadataFile> metadata_file,
         std::unique_ptr<ThreadPool> pool);
  int available_space_cache_secs() const override;
  int reserved_bytes() const override;
};

// Directory manager creation options.
struct WalDirManagerOptions : public DirManagerOptions {
  WalDirManagerOptions();
};

// Encapsulates knowledge of WAL directory management.
class WalDirManager : public DirManager {
 public:
  // Public static initializers for use in tests. When used, data_fs_roots is
  // expected to be the successfully canonicalized directories.
  static Status CreateNewForTests(Env* env, const std::vector<std::string>& wal_fs_roots,
                                  const WalDirManagerOptions& opts,
                                  std::unique_ptr<WalDirManager>* wd_manager);
  static Status OpenExistingForTests(Env* env, const std::vector<std::string>& wal_fs_roots,
                                     const WalDirManagerOptions& opts,
                                     std::unique_ptr<WalDirManager>* wd_manager);

  // Constructs a WAL directory manager and creates its necessary files on-disk.
  //
  // Returns an error if any of the directories already exist.
  static Status CreateNew(Env* env, CanonicalizedRootsList wal_fs_roots,
                          const WalDirManagerOptions& opts,
                          std::unique_ptr<WalDirManager>* wd_manager);

  // Constructs a directory manager and indexes the files found on-disk.
  //
  // Returns an error if the number of on-disk directories found exceeds the
  // max allowed, or if locks need to be acquired and cannot be.
  static Status OpenExisting(Env* env, CanonicalizedRootsList wal_fs_roots,
                             const WalDirManagerOptions& opts,
                             std::unique_ptr<WalDirManager>* wd_manager);

  // ==========================================================================
  // Tablet Placement
  // ==========================================================================

  // Deserializes a WalDirPB and associates the resulting WalDirPB
  // with a tablet_id.
  //
  // Returns an error if the tablet already exists or if a WAL dir is missing.
  Status LoadWalDirFromPB(const std::string& tablet_id,
                          const WalDirPB& pb);

  // Serializes the WalDirPB associated with the given tablet_id.
  //
  // Returns an error if the tablet was not already registered or if a WAL dir
  // is missing.
  Status GetWalDirPB(const std::string& tablet_id, WalDirPB* pb) const;

  // Results in an error if the tablet has already been registered, return NotFound if
  // cannot find a WAL directory for the tablet from disk. If "suffix" is ".recovery",
  // it's used to find WAL recovery directory, if it's empty, it's used to find WAL
  // directory.
  Status FindTabletDirFromDisk(const std::string& tablet_id,
                               const std::string& suffix,
                               std::string* wal_dir);

  // Results in an error if the tablet has no WAL dir associated with it. If
  // returning with an error, the WalDirManager will be unchanged.
  Status FindAndRegisterWalDirOnDisk(const std::string& tablet_id);

  // Results in an error if all disks are full or if the tablet already has a
  // WAL dir associated with it. If returning with an error, the
  // WalDirManager will be unchanged.
  Status CreateWalDir(const std::string& tablet_id);

  // Deletes the WAL directory for the specified tablet. Maps from tablet_id to
  // WAL dir to tablet set are cleared of all references to the tablet.
  void DeleteWalDir(const std::string& tablet_id);

  // Finds a WAL directory by uuid, returning null if it can't be found.
  WalDir* FindWalDirByUuid(const std::string& uuid) const;

  // Finds the set of tablet_ids in the WAL dir specified by 'uuid' and
  // returns a copy, returning an empty set if none are found.
  std::set<std::string> FindTabletsByWalDirUuid(const std::string& uuid) const;

  // Looks into each WAL directory for an existing WAL for 'tablet_id'. Results
  // in an error if no such WAL exists, or if there are multiple associated with
  // the given tablet ID. Returns 'wal_dir' the directory name for the WAL dir of
  // the tablet specified by 'tablet_id'.
  Status FindWalDirByTabletId(const std::string& tablet_id,
                              std::string* wal_dir) const;

  // Create a new WAL directory.
  std::unique_ptr<Dir> CreateNewDir(Env* env,
                                    DirMetrics* metrics,
                                    FsType fs_type,
                                    std::string dir,
                                    std::unique_ptr<DirInstanceMetadataFile> metadata_file,
                                    std::unique_ptr<ThreadPool> pool) override;

 private:
  FRIEND_TEST(WalDirManagerTest, TestFindAndRegisterWalDir);
  FRIEND_TEST(WalDirManagerTest, TestCreateWalDir);
  FRIEND_TEST(WalDirManagerTest, TestFailedDirNotSelected);
  FRIEND_TEST(WalDirManagerTest, TestLoadBalancingBias);
  FRIEND_TEST(WalDirManagerTest, TestLoadBalancingDistribution);

  const char* dir_name() const override {
    return kWalDirName;
  }

  const char* instance_metadata_filename() const override {
    return kWalInstanceMetadataFileName;
  }

  bool sync_dirs() const override;
  bool lock_dirs() const override;
  int max_dirs() const override;

  // Constructs a WAL directory manager.
  WalDirManager(Env* env,
                const WalDirManagerOptions& opts,
                CanonicalizedRootsList canonicalized_wal_roots);

  typedef std::unordered_map<std::string, int> TabletWalDirMap;
  TabletWalDirMap uuid_idx_by_tablet_;

  DISALLOW_COPY_AND_ASSIGN(WalDirManager);
};

} // namespace fs
} // namespace kudu
