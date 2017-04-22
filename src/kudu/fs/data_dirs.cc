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

#include "kudu/fs/data_dirs.h"

#include <algorithm>
#include <cerrno>
#include <deque>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/block_manager_util.h"
#include "kudu/fs/data_dir_group.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"

DEFINE_uint32(fs_target_data_dirs_per_tablet, 0,
              "Indicates the target number of data dirs to spread each "
              "tablet's data across. If greater than the number of data dirs "
              "available, data will be striped across those available. The "
              "default value 0 indicates striping should occur across all "
              "data directories.");
TAG_FLAG(fs_target_data_dirs_per_tablet, advanced);
TAG_FLAG(fs_target_data_dirs_per_tablet, evolving);

DEFINE_int64(fs_data_dirs_reserved_bytes, -1,
             "Number of bytes to reserve on each data directory filesystem for "
             "non-Kudu usage. The default, which is represented by -1, is that "
             "1% of the disk space on each disk will be reserved. Any other "
             "value specified represents the number of bytes reserved and must "
             "be greater than or equal to 0. Explicit percentages to reserve "
             "are not currently supported");
DEFINE_validator(fs_data_dirs_reserved_bytes, [](const char* /*n*/, int64_t v) { return v >= -1; });
TAG_FLAG(fs_data_dirs_reserved_bytes, runtime);
TAG_FLAG(fs_data_dirs_reserved_bytes, evolving);

DEFINE_int32(fs_data_dirs_full_disk_cache_seconds, 30,
             "Number of seconds we cache the full-disk status in the block manager. "
             "During this time, writes to the corresponding root path will not be attempted.");
TAG_FLAG(fs_data_dirs_full_disk_cache_seconds, advanced);
TAG_FLAG(fs_data_dirs_full_disk_cache_seconds, evolving);

METRIC_DEFINE_gauge_uint64(server, data_dirs_full,
                           "Data Directories Full",
                           kudu::MetricUnit::kDataDirectories,
                           "Number of data directories whose disks are currently full");

namespace kudu {

namespace fs {

using env_util::ScopedFileDeleter;
using std::deque;
using std::iota;
using std::random_shuffle;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace {

const char kInstanceMetadataFileName[] = "block_manager_instance";

const char kHolePunchErrorMsg[] =
    "Error during hole punch test. The log block manager requires a "
    "filesystem with hole punching support such as ext4 or xfs. On el6, "
    "kernel version 2.6.32-358 or newer is required. To run without hole "
    "punching (at the cost of some efficiency and scalability), reconfigure "
    "Kudu with --block_manager=file. Refer to the Kudu documentation for more "
    "details. Raw error message follows";

Status CheckHolePunch(Env* env, const string& path) {
  // Arbitrary constants.
  static uint64_t kFileSize = 4096 * 4;
  static uint64_t kHoleOffset = 4096;
  static uint64_t kHoleSize = 8192;
  static uint64_t kPunchedFileSize = kFileSize - kHoleSize;

  // Open the test file.
  string filename = JoinPathSegments(path, "hole_punch_test_file");
  unique_ptr<RWFile> file;
  RWFileOptions opts;
  RETURN_NOT_OK(env->NewRWFile(opts, filename, &file));

  // The file has been created; delete it on exit no matter what happens.
  ScopedFileDeleter file_deleter(env, filename);

  // Preallocate it, making sure the file's size is what we'd expect.
  uint64_t sz;
  RETURN_NOT_OK(file->PreAllocate(0, kFileSize, RWFile::CHANGE_FILE_SIZE));
  RETURN_NOT_OK(env->GetFileSizeOnDisk(filename, &sz));
  if (sz != kFileSize) {
    return Status::IOError(Substitute(
        "Unexpected pre-punch file size for $0: expected $1 but got $2",
        filename, kFileSize, sz));
  }

  // Punch the hole, testing the file's size again.
  RETURN_NOT_OK(file->PunchHole(kHoleOffset, kHoleSize));
  RETURN_NOT_OK(env->GetFileSizeOnDisk(filename, &sz));
  if (sz != kPunchedFileSize) {
    return Status::IOError(Substitute(
        "Unexpected post-punch file size for $0: expected $1 but got $2",
        filename, kPunchedFileSize, sz));
  }

  return Status::OK();
}

} // anonymous namespace

#define GINIT(x) x(METRIC_##x.Instantiate(entity, 0))
DataDirMetrics::DataDirMetrics(const scoped_refptr<MetricEntity>& entity)
  : GINIT(data_dirs_full)  {
}
#undef GINIT


DataDir::DataDir(Env* env,
                 DataDirMetrics* metrics,
                 string dir,
                 unique_ptr<PathInstanceMetadataFile> metadata_file,
                 unique_ptr<ThreadPool> pool)
    : env_(env),
      metrics_(metrics),
      dir_(std::move(dir)),
      metadata_file_(std::move(metadata_file)),
      pool_(std::move(pool)),
      is_shutdown_(false),
      is_full_(false) {
}

DataDir::~DataDir() {
  Shutdown();
}

void DataDir::Shutdown() {
  if (is_shutdown_) {
    return;
  }

  pool_->Wait();
  pool_->Shutdown();
  is_shutdown_ = true;
}

void DataDir::ExecClosure(const Closure& task) {
  Status s = pool_->SubmitClosure(task);
  if (!s.ok()) {
    WARN_NOT_OK(
        s, "Could not submit task to thread pool, running it synchronously");
    task.Run();
  }
}

void DataDir::WaitOnClosures() {
  pool_->Wait();
}

Status DataDir::RefreshIsFull(RefreshMode mode) {
  switch (mode) {
    case RefreshMode::EXPIRED_ONLY: {
      std::lock_guard<simple_spinlock> l(lock_);
      DCHECK(last_check_is_full_.Initialized());
      MonoTime expiry = last_check_is_full_ + MonoDelta::FromSeconds(
          FLAGS_fs_data_dirs_full_disk_cache_seconds);
      if (!is_full_ || MonoTime::Now() < expiry) {
        break;
      }
      FALLTHROUGH_INTENDED; // Root was previously full, check again.
    }
    case RefreshMode::ALWAYS: {
      Status s = env_util::VerifySufficientDiskSpace(
          env_, dir_, 0, FLAGS_fs_data_dirs_reserved_bytes);
      bool is_full_new;
      if (PREDICT_FALSE(s.IsIOError() && s.posix_code() == ENOSPC)) {
        LOG(WARNING) << Substitute(
            "Insufficient disk space under path $0: creation of new data "
            "blocks under this path can be retried after $1 seconds: $2",
            dir_, FLAGS_fs_data_dirs_full_disk_cache_seconds, s.ToString());
        s = Status::OK();
        is_full_new = true;
      } else {
        is_full_new = false;
      }
      RETURN_NOT_OK(s); // Catch other types of IOErrors, etc.
      {
        std::lock_guard<simple_spinlock> l(lock_);
        if (metrics_ && is_full_ != is_full_new) {
          metrics_->data_dirs_full->IncrementBy(is_full_new ? 1 : -1);
        }
        is_full_ = is_full_new;
        last_check_is_full_ = MonoTime::Now();
      }
      break;
    }
    default:
      LOG(FATAL) << "Unknown check mode";
  }
  return Status::OK();
}

DataDirManager::DataDirManager(Env* env,
                               scoped_refptr<MetricEntity> metric_entity,
                               string block_manager_type,
                               vector<string> paths)
    : env_(env),
      block_manager_type_(std::move(block_manager_type)),
      paths_(std::move(paths)) {
  DCHECK_GT(paths_.size(), 0);

  if (metric_entity) {
    metrics_.reset(new DataDirMetrics(metric_entity));
  }
}

DataDirManager::~DataDirManager() {
  Shutdown();
}

void DataDirManager::Shutdown() {
  // We may be waiting here for a while on outstanding closures.
  LOG_SLOW_EXECUTION(INFO, 1000,
                     Substitute("waiting on $0 block manager thread pools",
                                data_dirs_.size())) {
    for (const auto& dd : data_dirs_) {
      dd->Shutdown();
    }
  }
}

Status DataDirManager::Create(int flags) {
  deque<ScopedFileDeleter*> delete_on_failure;
  ElementDeleter d(&delete_on_failure);

  // The UUIDs and indices will be included in every instance file.
  ObjectIdGenerator gen;
  vector<string> all_uuids(paths_.size());
  for (string& u : all_uuids) {
    u = gen.Next();
  }
  int idx = 0;

  // Ensure the data paths exist and create the instance files.
  unordered_set<string> to_sync;
  for (const auto& p : paths_) {
    bool created;
    RETURN_NOT_OK_PREPEND(env_util::CreateDirIfMissing(env_, p, &created),
                          Substitute("Could not create directory $0", p));
    if (created) {
      delete_on_failure.push_front(new ScopedFileDeleter(env_, p));
      to_sync.insert(DirName(p));
    }

    if (flags & FLAG_CREATE_TEST_HOLE_PUNCH) {
      RETURN_NOT_OK_PREPEND(CheckHolePunch(env_, p), kHolePunchErrorMsg);
    }

    string instance_filename = JoinPathSegments(p, kInstanceMetadataFileName);
    PathInstanceMetadataFile metadata(env_, block_manager_type_,
                                      instance_filename);
    RETURN_NOT_OK_PREPEND(metadata.Create(all_uuids[idx], all_uuids), instance_filename);
    delete_on_failure.push_front(new ScopedFileDeleter(env_, instance_filename));
    idx++;
  }

  // Ensure newly created directories are synchronized to disk.
  if (flags & FLAG_CREATE_FSYNC) {
    for (const string& dir : to_sync) {
      RETURN_NOT_OK_PREPEND(env_->SyncDir(dir),
                            Substitute("Unable to synchronize directory $0", dir));
    }
  }

  // Success: don't delete any files.
  for (ScopedFileDeleter* deleter : delete_on_failure) {
    deleter->Cancel();
  }
  return Status::OK();
}

Status DataDirManager::Open(int max_data_dirs, LockMode mode) {
  vector<PathInstanceMetadataFile*> instances;
  vector<unique_ptr<DataDir>> dds;

  int i = 0;
  for (const auto& p : paths_) {
    // Open and lock the data dir's metadata instance file.
    string instance_filename = JoinPathSegments(p, kInstanceMetadataFileName);
    gscoped_ptr<PathInstanceMetadataFile> instance(
        new PathInstanceMetadataFile(env_, block_manager_type_,
                                     instance_filename));
    RETURN_NOT_OK_PREPEND(instance->LoadFromDisk(),
                          Substitute("Could not open $0", instance_filename));
    if (mode != LockMode::NONE) {
      Status s = instance->Lock();
      if (!s.ok()) {
        Status new_status = s.CloneAndPrepend(Substitute(
            "Could not lock $0", instance_filename));
        if (mode == LockMode::OPTIONAL) {
          LOG(WARNING) << new_status.ToString();
          LOG(WARNING) << "Proceeding without lock";
        } else {
          DCHECK(LockMode::MANDATORY == mode);
          RETURN_NOT_OK(new_status);
        }
      }
    }
    instances.push_back(instance.get());

    // Create a per-dir thread pool.
    gscoped_ptr<ThreadPool> pool;
    RETURN_NOT_OK(ThreadPoolBuilder(Substitute("data dir $0", i))
                  .set_max_threads(1)
                  .Build(&pool));

    // Create the data directory in-memory structure itself.
    unique_ptr<DataDir> dd(new DataDir(
        env_, metrics_.get(), p,
        unique_ptr<PathInstanceMetadataFile>(instance.release()),
        unique_ptr<ThreadPool>(pool.release())));

    // Initialize the 'fullness' status of the data directory.
    RETURN_NOT_OK(dd->RefreshIsFull(DataDir::RefreshMode::ALWAYS));

    dds.emplace_back(std::move(dd));
    i++;
  }

  RETURN_NOT_OK_PREPEND(PathInstanceMetadataFile::CheckIntegrity(instances),
                        Substitute("Could not verify integrity of files: $0",
                                   JoinStrings(paths_, ",")));

  // Build uuid index and data directory maps.
  UuidIndexMap dd_by_uuid_idx;
  ReverseUuidIndexMap uuid_idx_by_dd;
  for (const auto& dd : dds) {
    const PathSetPB& path_set = dd->instance()->metadata()->path_set();
    uint32_t idx = -1;
    for (int i = 0; i < path_set.all_uuids_size(); i++) {
      if (path_set.uuid() == path_set.all_uuids(i)) {
        idx = i;
        break;
      }
    }
    DCHECK_NE(idx, -1); // Guaranteed by CheckIntegrity().
    if (idx > max_data_dirs) {
      return Status::NotSupported(
          Substitute("Block manager supports a maximum of $0 paths", max_data_dirs));
    }
    InsertOrDie(&dd_by_uuid_idx, idx, dd.get());
    InsertOrDie(&uuid_idx_by_dd, dd.get(), idx);
  }

  data_dirs_.swap(dds);
  data_dir_by_uuid_idx_.swap(dd_by_uuid_idx);
  uuid_idx_by_data_dir_.swap(uuid_idx_by_dd);
  return Status::OK();
}

Status DataDirManager::LoadDataDirGroupFromPB(const std::string& tablet_id,
                                              const DataDirGroupPB& pb) {
  DataDirGroup* other = InsertOrReturnExisting(&group_by_tablet_map_,
                                                tablet_id,
                                                DataDirGroup::FromPB(pb));
  if (other) {
    return Status::AlreadyPresent("Tablet already being tracked", tablet_id);
  }
  return Status::OK();
}

Status DataDirManager::CreateDataDirGroup(const CreateBlockOptions& opts, bool use_all_dirs) {
  std::lock_guard<percpu_rwlock> lock(dir_group_lock_);
  if (ContainsKey(group_by_tablet_map_, opts.tablet_id)) {
    return Status::AlreadyPresent("Tablet already being tracked", opts.tablet_id);
  }
  // Adjust the disk group size to fit within the total number of data dirs.
  int kDataDirGroupSize = FLAGS_fs_target_data_dirs_per_tablet;
  if (kDataDirGroupSize == 0 || use_all_dirs || kDataDirGroupSize > data_dirs_.size()) {
    kDataDirGroupSize = data_dirs_.size();
  }
  // Randomly select directories, giving preference to those with fewer tablets.
  // Create a new DataDirGroup and add directly to its uuids.
  LookupOrInsert(&group_by_tablet_map_, opts.tablet_id, DataDirGroup::CreateEmptyGroup());
  DataDirGroup* group_for_tablet = FindOrNull(group_by_tablet_map_, opts.tablet_id);
  CHECK(group_for_tablet);
  while(group_for_tablet->uuids()->size() < kDataDirGroupSize) {
    uint16_t uuid;
    if (!GetDirForGroup(group_for_tablet->uuids(), &uuid)) {
      break;
    }
    tablets_by_uuid_map_[uuid].insert(opts.tablet_id);
  }
  if (group_for_tablet->uuids()->empty()) {
    return Status::IOError("All data directories are full", "", ENOSPC);
  }
  return Status::OK();
}

Status DataDirManager::GetNextDataDir(const CreateBlockOptions& opts, DataDir** dir) {
  // Lock shared to not block other reads to group_by_tablet_map_.
  shared_lock<rw_spinlock>(dir_group_lock_.get_lock());
  vector<uint16_t>* group_uuids;
  vector<uint16_t> all_uuids;
  if (PREDICT_FALSE(opts.tablet_id == "")) {
    // This should only be reached by some tests; in cases where there is no
    // natural tablet_id, select a data dir randomly.
    for (auto uuid_and_dir_ptr : data_dir_by_uuid_idx_) {
      all_uuids.push_back(uuid_and_dir_ptr.first);
    }
    group_uuids = &all_uuids;
  } else {
    // Select the data dir group for the tablet.
    DataDirGroup* group = FindOrNull(group_by_tablet_map_, opts.tablet_id);
    if (group == nullptr) {
      return Status::NotFound("DataDirGroup not found for tablet", opts.tablet_id);
    }
    group_uuids = group->uuids();
  }
  vector<int> random_indexes(group_uuids->size());
  iota(random_indexes.begin(), random_indexes.end(), 0);
  random_shuffle(random_indexes.begin(), random_indexes.end());

  // Randomly select a member of the group that is not full.
  for (int i : random_indexes) {
    DataDir* candidate = data_dir_by_uuid_idx_[(*group_uuids)[i]];
    RETURN_NOT_OK(candidate->RefreshIsFull(
        DataDir::RefreshMode::EXPIRED_ONLY));
    if (!candidate->is_full()) {
      *dir = candidate;
      return Status::OK();
    }
  }
  return Status::IOError(
      "All data directories are full. Please free some disk space or "
      "consider changing the fs_data_dirs_reserved_bytes configuration "
      "parameter", "", ENOSPC);
}

void DataDirManager::UntrackTablet(const std::string& tablet_id) {
  std::lock_guard<percpu_rwlock> lock(dir_group_lock_);
  DataDirGroup* group = FindOrNull(group_by_tablet_map_, tablet_id);
  if (group == nullptr) {
    return;
  }
  // Remove the tablet_id from every data dir in its group.
  for (int16_t uuid : *(group->uuids())) {
    tablets_by_uuid_map_[uuid].erase(tablet_id);
  }
  group_by_tablet_map_.erase(tablet_id);
}

DataDirGroup* DataDirManager::GetDataDirGroupForTablet(const std::string& tablet_id) {
  shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
  return FindOrNull(group_by_tablet_map_, tablet_id);
}

bool DataDirManager::GetDirForGroup(vector<uint16_t>* group, uint16_t* uuid) {
  if (group->size() == data_dirs_.size()) {
    return false;
  }

  // Determine all potential candidates to be added to the group.
  vector<uint16_t> dir_uuids;
  unordered_set<uint16_t> data_dir_set;
  for (uint16_t uuid : *group) {
    data_dir_set.insert(uuid);
  }
  for (auto uuid_and_dir_ptr : data_dir_by_uuid_idx_) {
    // If the uuid is already in the group, ignore it.
    if (ContainsKey(data_dir_set, uuid_and_dir_ptr.first)) {
      continue;
    }
    dir_uuids.push_back(uuid_and_dir_ptr.first);
  }

  // Select two random uuids and select the one with fewer tablets on it.
  random_shuffle(dir_uuids.begin(), dir_uuids.end());
  int iter1;
  int iter2;
  uint16_t uuid1 = 0;
  uint16_t uuid2 = 0;
  DataDir* candidate;
  for(iter1 = 0; iter1 < dir_uuids.size(); iter1++) {
    candidate = data_dir_by_uuid_idx_[dir_uuids[iter1]];
    if (!candidate->is_full()) {
      uuid1 = dir_uuids[iter1];
      break;
    }
  }
  for (iter2 = iter1 + 1; iter2 < dir_uuids.size(); iter2++) {
    candidate = data_dir_by_uuid_idx_[dir_uuids[iter2]];
    if (!candidate->is_full()) {
      uuid2 = dir_uuids[iter2];
      break;
    }
  }
  if (iter1 == dir_uuids.size()) {
    return false;
  }
  if (iter2 == dir_uuids.size() ||
             tablets_by_uuid_map_[uuid1].size() > tablets_by_uuid_map_[uuid2].size()) {
    // If there is only a single available directory or if the first found is has fewer tablets
    // than the second, choose the first directory.
    group->push_back(uuid1);
    *uuid = uuid1;
  } else {
    group->push_back(uuid2);
    *uuid = uuid2;
  }
  return true;
}

DataDir* DataDirManager::FindDataDirByUuidIndex(uint16_t uuid_idx) const {
  return FindPtrOrNull(data_dir_by_uuid_idx_, uuid_idx);
}

bool DataDirManager::FindUuidIndexByDataDir(DataDir* dir, uint16_t* uuid_idx) const {
  return FindCopy(uuid_idx_by_data_dir_, dir, uuid_idx);
}

} // namespace fs
} // namespace kudu
