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

#include <set>
#include <string>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util_prod.h"

namespace kudu {

class Env;
class FileLock;
class PathInstanceMetadataPB;
class PathSetPB;

namespace fs {

// Reads and writes block manager instance metadata files.
//
// Thread-unsafe; access to this object must be externally synchronized.
class PathInstanceMetadataFile {
 public:
  // 'env' must remain valid for the lifetime of this class.
  PathInstanceMetadataFile(Env* env, std::string block_manager_type,
                           std::string filename);

  ~PathInstanceMetadataFile();

  // Creates, writes, synchronizes, and closes a new instance metadata file.
  //
  // 'uuid' is this instance's UUID, 'all_uuids' is all of the UUIDs in this
  // instance's path set, and 'all_paths' are the full paths corresponding to
  // each UUID.
  Status Create(const std::string& uuid,
                const std::vector<std::string>& all_uuids,
                const std::vector<std::string>& all_paths);

  // Opens, reads, verifies, and closes an existing instance metadata file.
  //
  // On success, either 'metadata_' is overwritten with the contents of the
  // file, or, in the case of disk failure, 'health_status_' is set to not OK().
  Status LoadFromDisk();

  // Locks the instance metadata file, which must exist on-disk. Returns an
  // error if it's already locked. The lock is released when Unlock() is
  // called, when this object is destroyed, or when the process exits.
  //
  // Note: the lock is also released if any fd of the instance metadata file
  // in this process is closed. Thus, it is an error to call Create() or
  // LoadFromDisk() on a locked file.
  Status Lock();

  // Unlocks the instance metadata file. Must have been locked to begin with.
  Status Unlock();

  void SetMetadataForTests(std::unique_ptr<PathInstanceMetadataPB> metadata) {
    DCHECK(IsGTest());
    metadata_ = std::move(metadata);
  }

  void SetInstanceFailed(Status s = Status::IOError("Path instance failed")) {
    health_status_ = s;
  }

  // Whether or not the instance is healthy. If the instance file lives on a
  // disk that has failed, this should return false.
  bool healthy() const {
    return health_status_.ok();
  }

  Status health_status() const {
    return health_status_;
  }

  std::string path() const { return DirName(filename_); }
  PathInstanceMetadataPB* const metadata() const { return metadata_.get(); }

  // Check the integrity of the provided instances' path sets.
  //
  // 'all_paths' in 'main_pb' is copied from the most up-to-date instance.
  // 'updated_indices' is populated with indices within 'instances' of the
  // healthy instances that need to be rewritten to sync with 'main_pb'.
  static Status CheckIntegrity(const std::vector<PathInstanceMetadataFile*>& instances,
                               PathSetPB* main_pb,
                               std::set<int>* updated_indices);

  // Syncs the contents of the 'metadata_' to disk.
  Status UpdateOnDisk() const;

 private:
  // Flushes the contents of 'pb' to disk.
  //
  // On success, updates 'timestamp_us' in 'pb' to the current time.
  Status FlushMetadataToDisk(pb_util::CreateMode create_mode, PathInstanceMetadataPB* pb) const;

  Env* env_;
  const std::string block_manager_type_;
  const std::string filename_;
  std::unique_ptr<PathInstanceMetadataPB> metadata_;
  std::unique_ptr<FileLock> lock_;
  Status health_status_;
};

} // namespace fs
} // namespace kudu
