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
DEFINE_bool(fs_wal_dirs_consider_available_space, true,
            "Whether to consider available space when selecting a WAL "
            "directory during tablet creation.");
TAG_FLAG(fs_wal_dirs_consider_available_space, runtime);
TAG_FLAG(fs_wal_dirs_consider_available_space, evolving);

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
  unique_ptr<WalDirManager> wm;
  wm.reset(new WalDirManager(env, opts, std::move(wal_fs_roots)));
  RETURN_NOT_OK(wm->Open());
  wd_manager->swap(wm);
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

Status WalDirManager::FindAndRegisterWalDirOnDisk(const string& tablet_id) {
  {
    shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
    if (ContainsKey(uuid_idx_by_tablet_, tablet_id)) {
      return Status::AlreadyPresent(Substitute("Tried to create WAL directory for tablet $0"
                                    "but one is already registered", tablet_id));
    }
  }

  // Check to see if any of our WAL directories have the given tablet ID.
  string wd_uuid;
  int dir_number = 0;
  for (const auto& wd : dirs_) {
    string tablet_path = JoinPathSegments(wd->dir(), tablet_id);
    if (env_->FileExists(tablet_path)) {
      ++dir_number;
      if (wd->instance()->healthy()) {
        wd_uuid = wd->instance()->uuid();
      }
    }
  }
  if (wd_uuid.empty()) {
    return Status::NotFound(Substitute(
        "could not find a healthy WAL dir for tablet $0", tablet_id));
  }
  if (dir_number != 1) {
    return Status::Corruption(Substitute("Tablet $0 has at least two WAL directories.",
                                          tablet_id));
  }
  {
    std::lock_guard<percpu_rwlock> write_lock(dir_group_lock_);
    int uuid_idx;
    if (!FindCopy(idx_by_uuid_, wd_uuid, &uuid_idx)) {
      return Status::NotFound(Substitute(
          "could not find WAL dir with uuid $0", wd_uuid));
    }
    InsertOrDie(&uuid_idx_by_tablet_, tablet_id, uuid_idx);
    InsertOrDie(&FindOrDie(tablets_by_uuid_idx_map_, uuid_idx), tablet_id);
  }
  LOG(INFO) << Substitute("Tablet $0 find a WAL directory UUID: $1", tablet_id,
      wd_uuid);
  return Status::OK();
}

Status WalDirManager::FindTabletDirFromDisk(
    const string& tablet_id, const string& suffix, string* wal_dir) {
  {
    shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
    if (ContainsKey(uuid_idx_by_tablet_, tablet_id)) {
      return Status::AlreadyPresent(Substitute("Tried to create WAL directory for tablet $0"
                                    "but one is already registered", tablet_id));
    }
  }
  string wd_uuid;
  for (const auto& wd : dirs_) {
    string tablet_path = JoinPathSegments(wd->dir(), tablet_id);
    tablet_path.append(suffix);
    if (env_->FileExists(tablet_path)) {
      *wal_dir = tablet_path;
      return Status::OK();
    }
  }
  return Status::NotFound("could not find a path for tablet on disk");
}

Status WalDirManager::CreateWalDir(const string& tablet_id) {
  std::lock_guard<percpu_rwlock> write_lock(dir_group_lock_);
  if (ContainsKey(uuid_idx_by_tablet_, tablet_id)) {
    return Status::AlreadyPresent("Tried to create WAL directory for tablet but one is already "
                                  "registered", tablet_id);
  }
  if (dirs_.size() <= failed_dirs_.size()) {
    return Status::IOError("No healthy wal directories available", "", ENODEV);
  }
  vector<int> candidate_indices;
  int min_tablets_num = kint32max;
  int target_uuid_idx = -1;
  for (const auto& tablets_by_uuid : tablets_by_uuid_idx_map_) {
    int uuid_idx = tablets_by_uuid.first;
    if (ContainsKey(failed_dirs_, uuid_idx)) {
      continue;
    }
    auto* candidate = FindDirByUuidIndex(uuid_idx);
    CHECK(candidate);
    Status s = candidate->RefreshAvailableSpace(Dir::RefreshMode::ALWAYS);
    WARN_NOT_OK(s, Substitute("failed to refresh fullness of $0", candidate->dir()));
    // Maybe we should check the "available_bytes_", it should larger than 8M or 128M.
    if (!s.ok() || candidate->is_full()) {
      continue;
    }
    if (tablets_by_uuid.second.size() < min_tablets_num) {
      min_tablets_num = tablets_by_uuid.second.size();
      candidate_indices.clear();
      candidate_indices.emplace_back(tablets_by_uuid.first);
    } else if (tablets_by_uuid.second.size() == min_tablets_num) {
      candidate_indices.emplace_back(tablets_by_uuid.first);
    }
  }
  if (candidate_indices.empty()) {
    return Status::NotFound("could not find a healthy path for new tablet");
  }
  if (FLAGS_fs_wal_dirs_consider_available_space) {
    int64_t max_available_space = -1;
    for (auto i : candidate_indices) {
      int64_t space_i = FindOrDie(dir_by_uuid_idx_, i)->available_bytes();
      if (space_i > max_available_space) {
        max_available_space = space_i;
        target_uuid_idx = i;
      }
    }
  } else {
    target_uuid_idx = candidate_indices[0];
  }

  LOG(INFO) << Substitute("Tablet $0 get a WAL directory UUID index: $1",
      tablet_id, target_uuid_idx);
  InsertOrDie(&uuid_idx_by_tablet_, tablet_id, target_uuid_idx);
  InsertOrDie(&FindOrDie(tablets_by_uuid_idx_map_, target_uuid_idx), tablet_id);
  return Status::OK();
}

void WalDirManager::DeleteWalDir(const std::string& tablet_id) {
  std::lock_guard<percpu_rwlock> lock(dir_group_lock_);
  const int* uuid_idx = FindOrNull(uuid_idx_by_tablet_, tablet_id);
  if (!uuid_idx) {
    return;
  }
  // Remove the tablet_id from every wal dir in its group.
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
  Dir* wd = FindPtrOrNull(dir_by_uuid_idx_, *uuid_idx);
  if (!wd) {
    return Status::NotFound(Substitute(
        "could not find wal dir for tablet $0", tablet_id));
  }
  *wal_dir = wd->dir();
  return Status::OK();
}

} // namespace fs
} // namespace kudu
