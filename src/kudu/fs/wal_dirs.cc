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

#include <cerrno>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/fs/dir_util.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

DEFINE_int64(fs_wal_dirs_reserved_bytes, -1,
             "Number of bytes to reserve on each WAL directory filesystem for "
             "non-Kudu usage. The default, which is represented by -1, is that "
             "1% of the disk space on each disk will be reserved. Any other "
             "value specified represents the number of bytes reserved and must "
             "be greater than or equal to 0. Explicit percentages to reserve "
             "are not currently supported");
DEFINE_validator(fs_wal_dirs_reserved_bytes, [](const char* /*n*/, int64_t v) { return v >= -1; });
TAG_FLAG(fs_wal_dirs_reserved_bytes, runtime);
TAG_FLAG(fs_wal_dirs_reserved_bytes, evolving);
DEFINE_int32(fs_wal_dirs_available_space_cache_seconds, 10,
             "Number of seconds we cache the available disk space in the block manager. ");
TAG_FLAG(fs_wal_dirs_available_space_cache_seconds, advanced);
TAG_FLAG(fs_wal_dirs_available_space_cache_seconds, evolving);
DEFINE_bool(fs_lock_wal_dirs, true,
            "Lock the wal directories to prevent concurrent usage. "
            "Note that read-only concurrent usage is still allowed.");
TAG_FLAG(fs_lock_wal_dirs, unsafe);
TAG_FLAG(fs_lock_wal_dirs, evolving);

METRIC_DEFINE_gauge_uint64(server, wal_dirs_failed,
                           "WAL Directories Failed",
                           kudu::MetricUnit::kDataDirectories,
                           "Number of WAL directories whose disks are currently "
                           "in a failed state",
                           kudu::MetricLevel::kWarn);
METRIC_DEFINE_gauge_uint64(server, wal_dirs_full,
                           "WAL Directories Full",
                           kudu::MetricUnit::kDataDirectories,
                           "Number of WAL directories whose disks are currently full",
                           kudu::MetricLevel::kWarn);
DECLARE_bool(enable_data_block_fsync);
DECLARE_string(block_manager);

namespace kudu {

namespace fs {

using std::default_random_engine;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

#define GINIT(member, x) member = METRIC_##x.Instantiate(metric_entity, 0)
WalDirMetrics::WalDirMetrics(const scoped_refptr<MetricEntity>& metric_entity) {
  GINIT(dirs_failed, wal_dirs_failed);
  GINIT(dirs_full, wal_dirs_full);
}
#undef GINIT

WalDir::WalDir(Env* env, DirMetrics* metrics, FsType fs_type, std::string dir,
               std::unique_ptr<DirInstanceMetadataFile> metadata_file,
               std::unique_ptr<ThreadPool> pool)
    : Dir(env, metrics, fs_type, std::move(dir), std::move(metadata_file), std::move(pool)) {}

int WalDir::available_space_cache_secs() const {
  return FLAGS_fs_wal_dirs_available_space_cache_seconds;
}

int WalDir::reserved_bytes() const {
  return FLAGS_fs_wal_dirs_reserved_bytes;
}

WalDirManagerOptions::WalDirManagerOptions()
    : DirManagerOptions(kWalDirName) {}

std::unique_ptr<Dir> WalDirManager::CreateNewDir(
    Env* env, DirMetrics* metrics, FsType fs_type,
    std::string dir, std::unique_ptr<DirInstanceMetadataFile> metadata_file,
    std::unique_ptr<ThreadPool> pool) {
  return unique_ptr<Dir>(new WalDir(env, metrics, fs_type, std::move(dir),
                                    std::move(metadata_file), std::move(pool)));
}

WalDirManager::WalDirManager(Env* env,
                             const WalDirManagerOptions& opts,
                             CanonicalizedRootsList canonicalized_wal_roots)
    : DirManager(env, opts.metric_entity ?
                 unique_ptr<DirMetrics>(new WalDirMetrics(opts.metric_entity)) : nullptr,
                 /*num_threads_per_dir=*/1, opts, std::move(canonicalized_wal_roots)) {}


Status WalDirManager::OpenExistingForTests(Env* env,
                                           const vector<string>& wal_fs_roots,
                                           const WalDirManagerOptions& opts,
                                           unique_ptr<WalDirManager>* wd_manager) {
  CanonicalizedRootsList roots;
  for (const auto& r : wal_fs_roots) {
    roots.push_back({ r, Status::OK() });
  }
  return WalDirManager::OpenExisting(env, std::move(roots), opts, wd_manager);
}

Status WalDirManager::OpenExisting(Env* env, CanonicalizedRootsList wal_fs_roots,
                                   const WalDirManagerOptions& opts,
                                   unique_ptr<WalDirManager>* wd_manager) {
  unique_ptr<WalDirManager> wm(new WalDirManager(env, opts, std::move(wal_fs_roots)));
  RETURN_NOT_OK(wm->Open());
  *wd_manager = std::move(wm);
  return Status::OK();
}

Status WalDirManager::CreateNewForTests(Env* env,
                                        const vector<string>& wal_fs_roots,
                                        const WalDirManagerOptions& opts,
                                        unique_ptr<WalDirManager>* wd_manager) {
  CanonicalizedRootsList roots;
  for (const auto& r : wal_fs_roots) {
    roots.push_back({ r, Status::OK() });
  }
  return WalDirManager::CreateNew(env, std::move(roots), opts, wd_manager);
}

Status WalDirManager::CreateNew(Env* env, CanonicalizedRootsList wal_fs_roots,
                                const WalDirManagerOptions& opts,
                                unique_ptr<WalDirManager>* wd_manager) {
  unique_ptr<WalDirManager> wm;
  wm.reset(new WalDirManager(env, opts, std::move(wal_fs_roots)));
  RETURN_NOT_OK(wm->Create());
  RETURN_NOT_OK(wm->Open());
  wd_manager->swap(wm);
  return Status::OK();
}

bool WalDirManager::sync_dirs() const {
  return FLAGS_enable_data_block_fsync;
}

bool WalDirManager::lock_dirs() const {
  return FLAGS_fs_lock_wal_dirs;
}

int WalDirManager::max_dirs() const {
  return kint32max;
}

Status WalDirManager::LoadWalDirFromPB(const std::string& tablet_id,
                                       const WalDirPB& pb) {
  std::lock_guard<percpu_rwlock> lock(dir_group_lock_);
  const string& uuid = pb.uuid();
  int uuid_idx;
  if (!FindCopy(idx_by_uuid_, uuid, &uuid_idx)) {
    return Status::NotFound(Substitute(
        "could not find WAL dir with uuid $0", uuid));
  }

  Dir* wal_dir = FindPtrOrNull(dir_by_uuid_idx_, uuid_idx);
  if (!wal_dir) {
    return Status::NotFound(Substitute(
        "could not find WAL dir with uuid $0", uuid));
  }

  int* other = InsertOrReturnExisting(&uuid_idx_by_tablet_,
                                      tablet_id,
                                      uuid_idx);
  if (other) {
    return Status::AlreadyPresent(Substitute(
        "tried to load directory for tablet $0 but one $1 is already registered",
        tablet_id, *other));
  }
  InsertOrDie(&FindOrDie(tablets_by_uuid_idx_map_, uuid_idx), tablet_id);
  return Status::OK();
}

Status WalDirManager::FindOnDiskDir(const string& tablet_id, const string& suffix, Dir** wal_dir) {
  // Check to see if any WAL subdirectories have the given tablet ID.
  vector<Dir*> wds;
  for (const auto& wd : dirs_) {
    string tablet_path = JoinPathSegments(wd->dir(), tablet_id);
    tablet_path.append(suffix);
    if (env_->FileExists(tablet_path)) {
      if (wd->instance()->healthy()) {
        wds.emplace_back(wd.get());
      }
    }
  }
  if (wds.empty()) {
    return Status::NotFound(Substitute(
        "could not find a WAL dir for tablet $0", tablet_id));
  }
  if (wds.size() > 1) {
    vector<string> wd_strs;
    for (const auto* wd : wds) {
      wd_strs.emplace_back(Substitute("$0 ($1)", wd->instance()->uuid(), wd->dir()));
    }
    return Status::Corruption(Substitute("Tablet $0 has multiple registered WAL directories: $1",
                                         tablet_id, JoinStrings(wd_strs, ",")));
  }
  *wal_dir = wds[0];
  return Status::OK();
}

Status WalDirManager::FindOnDiskDirWithSuffix(const string& tablet_id, const string& suffix,
                                              string* wal_dir) {
  Dir* dir = nullptr;
  RETURN_NOT_OK(FindOnDiskDir(tablet_id, suffix, &dir));
  DCHECK(dir);
  string suffixed_dir = JoinPathSegments(dir->dir(), tablet_id);
  suffixed_dir.append(suffix);
  *wal_dir = std::move(suffixed_dir);
  return Status::OK();
}

Status WalDirManager::FindOnDiskDirAndRegister(const string& tablet_id) {
  const auto& kAlreadyPresentMsg = Substitute("Tried to register WAL directory for tablet $0 "
                                              "but one is already registered", tablet_id);
  {
    shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
    if (ContainsKey(uuid_idx_by_tablet_, tablet_id)) {
      return Status::AlreadyPresent(kAlreadyPresentMsg);
    }
  }
  Dir* dir = nullptr;
  RETURN_NOT_OK(FindOnDiskDir(tablet_id, /*suffix*/"", &dir));
  DCHECK(dir);
  const string& wd_uuid = dir->instance()->uuid();
  {
    std::lock_guard<percpu_rwlock> write_lock(dir_group_lock_);
    // Check for presence again in case we registered a directory while we were
    // looking on disk.
    if (ContainsKey(uuid_idx_by_tablet_, tablet_id)) {
      return Status::AlreadyPresent(kAlreadyPresentMsg);
    }
    int uuid_idx = FindOrDie(idx_by_uuid_, wd_uuid);
    InsertOrDie(&uuid_idx_by_tablet_, tablet_id, uuid_idx);
    InsertOrDie(&FindOrDie(tablets_by_uuid_idx_map_, uuid_idx), tablet_id);
  }
  LOG(INFO) << Substitute("Registered WAL directory for tablet $0: $1", tablet_id, wd_uuid);
  return Status::OK();
}

Status WalDirManager::RegisterWalDir(const string& tablet_id) {
  std::lock_guard<percpu_rwlock> write_lock(dir_group_lock_);
  if (ContainsKey(uuid_idx_by_tablet_, tablet_id)) {
    return Status::AlreadyPresent("Tried to create WAL directory for tablet but one is already "
                                  "registered", tablet_id);
  }
  if (dirs_.size() <= failed_dirs_.size()) {
    return Status::IOError("No healthy wal directories available", "", ENODEV);
  }
  vector<int> candidate_indices;
  int min_tablets_in_dir = kint32max;
  for (const auto& idx_and_tablet : tablets_by_uuid_idx_map_) {
    int uuid_idx = idx_and_tablet.first;
    if (ContainsKey(failed_dirs_, uuid_idx)) {
      continue;
    }
    auto* candidate = FindDirByUuidIndex(uuid_idx);
    CHECK(candidate);
    Status s = candidate->RefreshAvailableSpace(Dir::RefreshMode::ALWAYS);
    WARN_NOT_OK(s, Substitute("failed to refresh fullness of $0", candidate->dir()));
    if (!s.ok() || candidate->is_full()) {
      continue;
    }
    // TODO(awong): we should probably check to see that the directory has
    // enough space to store some WAL segments.
    // Find the directories that have the least number of tablets in them.
    if (idx_and_tablet.second.size() < min_tablets_in_dir) {
      min_tablets_in_dir = idx_and_tablet.second.size();
      candidate_indices = { idx_and_tablet.first };
    } else if (idx_and_tablet.second.size() == min_tablets_in_dir) {
      candidate_indices.emplace_back(idx_and_tablet.first);
    }
  }
  if (candidate_indices.empty()) {
    return Status::NotFound("could not find a healthy path for new tablet");
  }
  // Select the candidate with the most space available.
  int64_t max_space = -1;
  Dir* selected_dir = nullptr;
  int selected_idx = -1;
  for (auto idx : candidate_indices) {
    Dir* dir = FindDirByUuidIndex(idx);
    DCHECK(dir);
    int64_t space_bytes = dir->available_bytes();
    if (space_bytes > max_space) {
      max_space = space_bytes;
      selected_dir = dir;
      selected_idx = idx;
    }
  }
  DCHECK_GE(selected_idx, 0);
  DCHECK(selected_dir);
  LOG(INFO) << Substitute("Tablet $0 was assigned WAL directory $1 ($2)",
      tablet_id, selected_dir->instance()->uuid(), selected_dir->dir());
  InsertOrDie(&uuid_idx_by_tablet_, tablet_id, selected_idx);
  InsertOrDie(&FindOrDie(tablets_by_uuid_idx_map_, selected_idx), tablet_id);
  return Status::OK();
}

void WalDirManager::UnregisterWalDir(const std::string& tablet_id) {
  std::lock_guard<percpu_rwlock> lock(dir_group_lock_);
  const int* uuid_idx = FindOrNull(uuid_idx_by_tablet_, tablet_id);
  if (!uuid_idx) {
    return;
  }
  // Remove the tablet_id from every map.
  FindOrDie(tablets_by_uuid_idx_map_, *uuid_idx).erase(tablet_id);
  uuid_idx_by_tablet_.erase(tablet_id);
}

Status WalDirManager::GetWalDirPB(const string& tablet_id,
                                  WalDirPB* pb) const {
  shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
  const int* uuid_idx = FindOrNull(uuid_idx_by_tablet_, tablet_id);
  if (!uuid_idx) {
    return Status::NotFound(Substitute(
        "could not find wal dir for tablet $0", tablet_id));
  }
  string uuid;
  if (!FindCopy(uuid_by_idx_, *uuid_idx, &uuid)) {
    return Status::NotFound(Substitute(
        "could not find data dir with uuid index $0", *uuid_idx));
  }
  pb->set_uuid(uuid);
  return Status::OK();
}

Status WalDirManager::FindWalDirByTabletId(
    const string& tablet_id, string* wal_dir) const {
  CHECK(wal_dir);
  shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
  const int* uuid_idx = FindOrNull(uuid_idx_by_tablet_, tablet_id);
  if (!uuid_idx) {
    return Status::NotFound(Substitute(
        "could not find wal dir for tablet $0", tablet_id));
  }
  *wal_dir = FindOrDie(dir_by_uuid_idx_, *uuid_idx)->dir();
  return Status::OK();
}

} // namespace fs
} // namespace kudu
