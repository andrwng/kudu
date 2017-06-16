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
#include "kudu/fs/block_manager_util.h"

#include <numeric>
#include <set>
#include <unordered_map>
#include <utility>

#include <gflags/gflags.h>

#include "kudu/fs/fs_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/scoped_cleanup.h"

DECLARE_bool(enable_data_block_fsync);

namespace kudu {
namespace fs {

using pb_util::CreateMode;
using std::set;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

// Evaluates 'status_expr' and if it results in a disk failure, logs a message
// and fails the instance, returning with no error.
//
// Note: if an non-disk-failure error is produced, the instance will remain
// healthy. These errors should be handled externally.
#define RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(status_expr, msg) do { \
  const Status& s_ = (status_expr).CloneAndPrepend(msg); \
  if (PREDICT_FALSE(s_.IsDiskFailure())) { \
    health_status_ = s_; \
    LOG(ERROR) << "Ignoring error with status: " << s_.ToString(); \
    return Status::OK(); \
  } \
  RETURN_NOT_OK(s_); \
} while (0);

PathInstanceMetadataFile::PathInstanceMetadataFile(Env* env,
                                                   string block_manager_type,
                                                   string filename)
    : env_(env),
      block_manager_type_(std::move(block_manager_type)),
      filename_(std::move(filename)),
      health_status_(Status::OK()) {}

PathInstanceMetadataFile::~PathInstanceMetadataFile() {
  if (lock_) {
    WARN_NOT_OK(Unlock(), Substitute("Failed to unlock file $0", filename_));
  }
}

Status PathInstanceMetadataFile::Create(const string& uuid, const vector<string>& all_uuids,
    const vector<string>& all_paths) {
  DCHECK(!lock_) << "Creating a metadata file that's already locked would release the lock";
  DCHECK(ContainsKey(set<string>(all_uuids.begin(), all_uuids.end()), uuid));
  DCHECK_EQ(all_paths.size(), all_uuids.size());

  uint64_t block_size;
  const string dir_name = DirName(filename_);
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(env_->GetBlockSize(dir_name, &block_size),
      Substitute("Failed to create metadata file. Could not get block size of $0", dir_name));

  PathInstanceMetadataPB new_instance;

  // Set up the path set.
  //
  // There is no need to set a timestamp here, as it is updated when
  // written to disk.
  PathSetPB* new_path_set = new_instance.mutable_path_set();
  new_path_set->set_timestamp_us(1);
  new_path_set->set_uuid(uuid);
  for (int i = 0; i < all_uuids.size(); i++) {
    PathPB new_path;
    new_path.set_uuid(all_uuids[i]);
    new_path.set_path(all_paths[i]);
    new_path.set_health_state(PathHealthStatePB::HEALTHY);
    *new_path_set->add_all_paths() = new_path;
  }

  // And the rest of the metadata.
  new_instance.set_block_manager_type(block_manager_type_);
  new_instance.set_filesystem_block_size_bytes(block_size);

  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(FlushMetadataToDisk(pb_util::NO_OVERWRITE, &new_instance),
      Substitute("Failed to flush newly created metadata file to $0", filename_));
  return Status::OK();
}

Status PathInstanceMetadataFile::UpdateOnDisk() const {
  return FlushMetadataToDisk(pb_util::OVERWRITE, metadata_.get());
}

Status PathInstanceMetadataFile::FlushMetadataToDisk(pb_util::CreateMode create_mode,
                                                     PathInstanceMetadataPB* pb) const {
  uint64_t original_timestamp = pb->path_set().timestamp_us();
  auto reset_timestamp = MakeScopedCleanup([&] {
    pb->mutable_path_set()->set_timestamp_us(original_timestamp);
  });
  pb->mutable_path_set()->set_timestamp_us(GetCurrentTimeMicros());
  RETURN_NOT_OK(pb_util::WritePBContainerToPath(
                env_, filename_, *pb, create_mode,
                FLAGS_enable_data_block_fsync ? pb_util::SYNC : pb_util::NO_SYNC));
  reset_timestamp.cancel();
  return Status::OK();
}

Status PathInstanceMetadataFile::LoadFromDisk() {
  DCHECK(!lock_) <<
      "Opening a metadata file that's already locked would release the lock";
  if (PREDICT_FALSE(!FsManager::SanitizePath(filename_).ok())) {
    health_status_ = Status::IOError("Failed to canonicalize; not reading instance", "", EIO);
    return Status::OK();
  }

  unique_ptr<PathInstanceMetadataPB> pb(new PathInstanceMetadataPB());
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(pb_util::ReadPBContainerFromPath(env_, filename_, pb.get()),
      Substitute("Failed to read metadata file from $0", filename_));

  if (pb->block_manager_type() != block_manager_type_) {
    return Status::IOError(Substitute(
      "existing data was written using the '$0' block manager; cannot restart "
      "with a different block manager '$1' without reformatting",
      pb->block_manager_type(), block_manager_type_));
  }

  uint64_t block_size;
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(env_->GetBlockSize(filename_, &block_size),
      Substitute("Failed to load metadata file. Could not get block size of $0", filename_));
  if (pb->filesystem_block_size_bytes() != block_size) {
    return Status::IOError("Wrong filesystem block size", Substitute(
        "Expected $0 but was $1", pb->filesystem_block_size_bytes(), block_size));
  }

  metadata_.swap(pb);
  return Status::OK();
}

Status PathInstanceMetadataFile::Lock() {
  DCHECK(!lock_);

  FileLock* lock;
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(env_->LockFile(filename_, &lock),
                                      Substitute("Could not lock $0", filename_));
  lock_.reset(lock);
  return Status::OK();
}

Status PathInstanceMetadataFile::Unlock() {
  DCHECK(lock_);

  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(env_->UnlockFile(lock_.release()),
                                      Substitute("Could not unlock $0", filename_));
  return Status::OK();
}

Status PathInstanceMetadataFile::CheckIntegrity(const vector<PathInstanceMetadataFile*>& instances,
                                                PathSetPB* main_pb, set<int>* updated_indices) {
  CHECK(!instances.empty());

  // Note: although much of this verification works at the level of UUIDs and
  // instance files, the (user-facing) error messages are reported in terms of
  // data directories, because UUIDs and instance files are internal details.

  // Map of healthy UUIDs to the instances, as seen in 'instances'.
  unordered_map<string, string> healthy_path_by_uuid;

  set<int> healthy_indices;
  // Set of all healthy instance indices.
  for (int i = 0; i < instances.size(); i++) {
    healthy_indices.insert(i);
  }
  unordered_map<string, PathInstanceMetadataFile*> healthy_instance_by_uuid;
  unordered_map<PathInstanceMetadataFile*, int> idx_by_instance;

  // Identify the instance that is the most up-to-date, and check that there
  // are no duplicate UUIDs among the healthy input instances.
  //
  // TODO(awong): in the case where we restart and the most up-to-date instance
  // has failed, we fall back on the next-most up-to-date instance, which may
  // not accurately describe the current state of the world in terms of what
  // disks are safe to use.
  int main_idx = 0;
  uint64_t max_timestamp = 0;
  for (int i = 0; i < instances.size(); i++) {
    InsertOrDie(&idx_by_instance, instances[i], i);
    if (PREDICT_TRUE(instances[i]->healthy())) {
      const PathSetPB path_set = instances[i]->metadata()->path_set();
      string* other = InsertOrReturnExisting(&healthy_path_by_uuid, path_set.uuid(),
          instances[i]->path());
      if (other) {
        return Status::IOError(
            Substitute("Data directories $0 and $1 have duplicate instance metadata UUIDs",
                       *other, instances[i]->path()), path_set.uuid());
      }
      InsertOrDie(&healthy_instance_by_uuid, path_set.uuid(), instances[i]);
      if (path_set.all_paths_size() == 0) {
        // Update instances with legacy metadata.
        updated_indices->insert(i);
      } else if (max_timestamp < path_set.timestamp_us()) {
        main_idx = i;
        max_timestamp = path_set.timestamp_us();
      }
    } else {
      healthy_indices.erase(i);
    }
  }
  if (PREDICT_FALSE(healthy_indices.empty())) {
    return Status::IOError("All data directories are marked failed due to disk failure");
  }

  PathSetPB* main_path_set = instances[main_idx]->metadata()->mutable_path_set();
  set<string> main_uuids;
  int all_size;
  if (main_path_set->all_paths_size() == 0) {
    // If only legacy metadata exists, use the main instance's all_uuids.
    main_uuids.insert(main_path_set->deprecated_all_uuids().begin(),
                      main_path_set->deprecated_all_uuids().end());
    all_size = main_path_set->deprecated_all_uuids_size();
  } else {
    all_size = main_path_set->all_paths_size();
    DCHECK_GT(all_size, 0);
    for (const auto& path : main_path_set->all_paths()) {
      main_uuids.insert(path.uuid());
    }
  }

  // Check the main path set does not contain duplicate UUIDs.
  if (main_uuids.size() != all_size) {
    vector<string> uuids;
    for (int i = 0; i < all_size; i++) {
      string uuid = main_path_set->all_paths_size() == 0 ?
          main_path_set->deprecated_all_uuids(i) : main_path_set->all_paths(i).uuid();
      uuids.emplace_back(uuid);
    }
    return Status::IOError(
        Substitute("Data directory $0 instance metadata path set contains duplicate UUIDs",
                   instances[main_idx]->path()), JoinStrings(uuids, ","));
  }

  // Upgrade the main path set if necessary and keep the depcreated all_uuids
  // for backwards compatibility.
  bool main_path_updated = false;
  if (main_path_set->all_paths_size() == 0) {
    if (instances.size() != healthy_path_by_uuid.size()) {
      return Status::IOError("Some instances failed to load; could not determine consistent "
                             "mapping of UUIDs to paths");
    }
    main_path_set->mutable_all_paths()->Reserve(main_uuids.size());
    for (int idx = 0; idx < main_uuids.size(); idx++) {
      PathPB* path_pb = main_path_set->add_all_paths();
      path_pb->set_uuid(main_path_set->deprecated_all_uuids(idx));
      path_pb->set_health_state(PathHealthStatePB::HEALTHY);
      string* path = FindOrNull(healthy_path_by_uuid, main_path_set->all_paths(idx).uuid());
      if (!path) {
        return Status::IOError(Substitute("Expected an instance with UUID $0 but none exists",
                                          main_path_set->all_paths(idx).uuid()));
      }
      path_pb->set_path(*path);
    }
    main_path_updated = true;
  }

  // Check the main path set's size aligns with that of the input instances.
  if (main_uuids.size() != instances.size()) {
    return Status::IOError(Substitute("$0 data directories provided, but expected $1",
                                      instances.size(), main_uuids.size()));
  }

  // Examine the integrity of the input instances against the state of the world
  // according to the main instance.
  for (int i = 0; i < main_path_set->all_paths_size(); i++) {
    const PathPB& pb_at_i = main_path_set->all_paths(i);
    PathInstanceMetadataFile* instance = FindPtrOrNull(healthy_instance_by_uuid,
                                                       pb_at_i.uuid());
    // If the instance failed to load, record its health state.
    // Note: Failed instances may not have metadata. Instances with unexpected
    // UUIDs will be marked FAILED.
    if (!instance || !instance->healthy()) {
      if (pb_at_i.health_state() != PathHealthStatePB::DISK_FAILED) {
        main_path_set->mutable_all_paths(i)->set_health_state(PathHealthStatePB::DISK_FAILED);
        if (instance) {
          int idx = FindOrDie(idx_by_instance, instance);
          healthy_indices.erase(idx);
        }
        main_path_updated = true;
      }
      continue;
    }

    // Check that the instance's on-disk path and UUID align with those in main path set.
    const PathSetPB& path_set = instance->metadata()->path_set();
    if (pb_at_i.path() != instance->path()) {
      return Status::IOError(Substitute("Expected instance $0 to be in $1 but it is in $2",
                                        path_set.uuid(),
                                        pb_at_i.path(), instance->path()));
    }
  }
  if (PREDICT_FALSE(healthy_indices.empty())) {
    return Status::IOError("All data directories are marked failed due to disk failure");
  }

  // Update the state of the output path set.
  main_pb->mutable_all_paths()->CopyFrom(main_path_set->all_paths());
  main_pb->set_timestamp_us(0);
  if (main_path_updated) {
    DCHECK(!healthy_indices.empty());
    updated_indices->insert(healthy_indices.begin(), healthy_indices.end());
  }

  return Status::OK();
}

} // namespace fs
} // namespace kudu
