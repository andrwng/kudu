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

#include "kudu/fs/error_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"

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

// Evaluates the expression and if there is a disk failure, invalidates the
// instance, returning with no error.
#define RETURN_NOT_OK_INVALIDATE(status_expr) do { \
  const Status& s_ = (status_expr); \
  if (PREDICT_FALSE(!s_.ok())) { \
    if (IsDiskFailure(s_)) { \
      LOG(INFO) << "DDDDDDD"; \
      valid_instance_ = false; \
      return Status::OK(); \
    } \
    return s_; \
  } \
} while (0);

PathInstanceMetadataFile::PathInstanceMetadataFile(Env* env,
                                                   string block_manager_type,
                                                   string filename)
    : env_(env),
      block_manager_type_(std::move(block_manager_type)),
      filename_(std::move(filename)),
      valid_instance_(true) {}

PathInstanceMetadataFile::~PathInstanceMetadataFile() {
  if (lock_) {
    WARN_NOT_OK(Unlock(), Substitute("Failed to unlock file $0", filename_));
  }
}

Status PathInstanceMetadataFile::Create(const string& uuid, const vector<string>& all_uuids,
    const vector<string>& all_paths) {
  DCHECK(!lock_) <<
      "Creating a metadata file that's already locked would release the lock";
  DCHECK(ContainsKey(set<string>(all_uuids.begin(), all_uuids.end()), uuid));
  DCHECK_EQ(all_paths.size(), all_uuids.size());

  uint64_t block_size;
  RETURN_NOT_OK_INVALIDATE(env_->GetBlockSize(DirName(filename_), &block_size));

  PathInstanceMetadataPB new_instance;

  // Set up the path set.
  PathSetPB* new_path_set = new_instance.mutable_path_set();
  new_path_set->set_uuid(uuid);
  new_path_set->mutable_all_uuids()->Reserve(all_uuids.size());
  new_path_set->mutable_all_disk_states()->Reserve(all_uuids.size());
  new_path_set->mutable_all_paths()->Reserve(all_paths.size());
  new_path_set->set_timestamp_us(1);
  for (int i = 0; i < all_uuids.size(); i++) {
    new_path_set->add_all_uuids(all_uuids[i]);
    new_path_set->add_all_disk_states(PathDiskStatePB::HEALTHY);
    new_path_set->add_all_paths(all_paths[i]);
  }

  // And the rest of the metadata.
  new_instance.set_block_manager_type(block_manager_type_);
  new_instance.set_filesystem_block_size_bytes(block_size);

  RETURN_NOT_OK_INVALIDATE(WriteToDiskUnlocked(pb_util::NO_OVERWRITE, &new_instance));
  return Status::OK();
}

Status PathInstanceMetadataFile::WriteToDiskUnlocked(pb_util::CreateMode create_mode,
                                                     PathInstanceMetadataPB* pb) {
  DCHECK(!lock_);
  if (pb == nullptr) {
    pb = metadata_.get();
  }
  pb->mutable_path_set()->set_timestamp_us(GetCurrentTimeMicros());
  return pb_util::WritePBContainerToPath(
      env_, filename_, *pb,
      create_mode, FLAGS_enable_data_block_fsync ? pb_util::SYNC : pb_util::NO_SYNC);
}

Status PathInstanceMetadataFile::LoadFromDisk() {
  DCHECK(!lock_) <<
      "Opening a metadata file that's already locked would release the lock";

  unique_ptr<PathInstanceMetadataPB> pb(new PathInstanceMetadataPB());
  RETURN_NOT_OK_INVALIDATE(pb_util::ReadPBContainerFromPath(env_, filename_, pb.get()));

  if (pb->block_manager_type() != block_manager_type_) {
    return Status::IOError(Substitute(
      "existing data was written using the '$0' block manager; cannot restart "
      "with a different block manager '$1' without reformatting",
      pb->block_manager_type(), block_manager_type_));
  }

  uint64_t block_size;
  RETURN_NOT_OK_INVALIDATE(env_->GetBlockSize(filename_, &block_size));
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
  RETURN_NOT_OK_PREPEND(env_->LockFile(filename_, &lock),
                        Substitute("Could not lock $0", filename_));
  lock_.reset(lock);
  return Status::OK();
}

Status PathInstanceMetadataFile::Unlock() {
  DCHECK(lock_);

  RETURN_NOT_OK_PREPEND(env_->UnlockFile(lock_.release()),
                        Substitute("Could not unlock $0", filename_));
  return Status::OK();
}

Status PathInstanceMetadataFile::CheckIntegrity(const vector<PathInstanceMetadataFile*>& instances,
    PathSetPB* main_pb, vector<uint16_t>* updated_indices) {
  CHECK(!instances.empty());

  // Note: although much of this verification works at the level of UUIDs and
  // instance files, the (user-facing) error messages are reported in terms of
  // data directories, because UUIDs and instance files are internal details.

  // Map of valid instance UUIDs to paths, as seen in 'instances'.
  unordered_map<string, string> path_by_uuid;

  // Set of instance indices that need to be updated.
  set<uint16_t> updated_set;

  // List of all indices.
  vector<uint16_t> all_indices(instances.size());
  std::iota(all_indices.begin(), all_indices.end(), 0);

  // Identify the instance that is the most up-to-date, and check that there
  // are no duplicate UUIDs among the input instances.
  uint16_t main_id = 0;
  bool has_valid_instances = false;
  uint64_t max_timestamp = 0;
  for (uint16_t i = 0; i < instances.size(); i++) {
    if (PREDICT_TRUE(instances[i]->valid_instance())) {
      has_valid_instances = true;
      LOG(INFO) << i << " IS VALID";
      const PathSetPB path_set = instances[i]->metadata()->path_set();
      string* other = InsertOrReturnExisting(&path_by_uuid, path_set.uuid(),
          instances[i]->path());
      if (other) {
        return Status::IOError(
            Substitute("Data directories $0 and $1 have duplicate instance metadata UUIDs",
                       *other, instances[i]->path()), path_set.uuid());
      }
      if (path_set.has_timestamp_us() && max_timestamp < path_set.timestamp_us()) {
        main_id = i;
        max_timestamp = path_set.timestamp_us();
      }
    }
  }
  LOG(INFO) << "HAS VALID?";
  if (PREDICT_FALSE(!has_valid_instances)) {
    return Status::IOError("All data directories are marked invalid due to disk failure");
  }
  LOG(INFO) << "HAS VALID!";

  // Ensure the main path set is in good form.
  PathSetPB* main_path_set = instances[main_id]->metadata()->mutable_path_set();
  set<string> main_uuids(main_path_set->all_uuids().begin(), main_path_set->all_uuids().end());
  string str;
  main_path_set->SerializeToString(&str);
  LOG(INFO) << "PATH SET: " << str;
  if (main_uuids.size() != main_path_set->all_uuids_size()) {
    return Status::IOError(
        Substitute("Data directory $0 instance metadata path set contains duplicate UUIDs",
                    instances[main_id]->path()),
        JoinStrings(main_path_set->all_uuids(), ","));
  }
  if (main_uuids.size() != instances.size()) {
    return Status::IOError(Substitute("$0 data directories provided, but expected $1",
                                      instances.size(), main_uuids.size()));
  }
  if (PREDICT_FALSE(main_path_set->all_disk_states_size() > 0 &&
      main_path_set->all_disk_states(main_id) == PathDiskStatePB::FAILED)) {
    return Status::IOError("Most up-to-date instance thinks it is marked failed");
  }

  // Upgrade the path set if necessary.
  if (PREDICT_FALSE(main_path_set->all_disk_states_size() == 0 ||
                    main_path_set->all_paths_size() == 0 ||
                    !main_path_set->has_timestamp_us())) {
    if (main_uuids.size() != path_by_uuid.size()) {
      return Status::IOError("Could determine consistent mapping of UUIDs to paths to upgrade "
                             "path instance");
    }
    main_path_set->set_timestamp_us(0);
    main_path_set->mutable_all_disk_states()->Reserve(main_uuids.size());
    main_path_set->mutable_all_paths()->Reserve(main_uuids.size());
    for (int idx = 0; idx < main_uuids.size(); idx++) {
      // For now, mark all disks as healthy. Failed disks will be updated later on.
      main_path_set->set_all_disk_states(idx, PathDiskStatePB::HEALTHY);
      string* path = FindOrNull(path_by_uuid, main_path_set->all_uuids(idx));
      if (!path) {
        return Status::IOError(Substitute("Expected an instance with UUID $0 but none exist",
                                          main_path_set->all_uuids(idx)));
      }
      main_path_set->add_all_paths(*path);
    }
    updated_set.insert(all_indices.begin(), all_indices.end());
  }

  // Examine the integrity of the input instances against the main instance.
  for (uint16_t idx = 0; idx < instances.size(); idx++) {
    PathInstanceMetadataFile* instance = instances[idx];
    if (PREDICT_FALSE(!instance->valid_instance())) {
      LOG(INFO) << "INVALID INSTANCE" << idx;
      if(main_path_set->all_disk_states(idx) == PathDiskStatePB::HEALTHY) {
        main_path_set->set_all_disk_states(idx, PathDiskStatePB::FAILED);
        updated_set.insert(all_indices.begin(), all_indices.end());
      }
      continue;
    }

    // Check that the instance's UUID is a member of main_uuids.
    const PathSetPB& path_set = instance->metadata()->path_set();
    if (!ContainsKey(main_uuids, path_set.uuid())) {
      return Status::IOError(
          Substitute("Data directory $0 instance metadata contains unexpected UUID",
                     instance->path()),
          path_set.uuid());
    }

    // Check that the instances' paths and UUIDs align with the main path set.
    string path = FindOrDie(path_by_uuid, main_path_set->all_uuids(idx));
    if (path != main_path_set->all_paths(idx)) {
      return Status::IOError(Substitute("Expected $0 to be in data directory $1 but it is in $2",
                                        main_path_set->all_uuids(idx),
                                        main_path_set->all_paths(idx), path));
    }
  }

  // Update the state of the output path set.
  main_pb->set_timestamp_us(0);
  main_pb->mutable_all_uuids()->CopyFrom(main_path_set->all_uuids());
  main_pb->mutable_all_disk_states()->CopyFrom(main_path_set->all_disk_states());
  main_pb->mutable_all_paths()->CopyFrom(main_path_set->all_paths());
  updated_indices->assign(updated_set.begin(), updated_set.end());

  return Status::OK();
}

} // namespace fs
} // namespace kudu
