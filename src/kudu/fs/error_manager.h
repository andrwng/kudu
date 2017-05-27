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

#include "kudu/fs/data_dirs.h"
#include "kudu/gutil/callback_forward.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/fault_injection.h"

namespace kudu {
namespace fs {

// Evaluates the expression and handles it if it results in an error.
// Returns if the status is an error.
#define RETURN_NOT_OK_HANDLE_ERROR(status_expr) do { \
  const Status& s_ = (status_expr); \
  if (PREDICT_TRUE(s_.ok())) { \
    break; \
  } \
  HandleError(s_, mutable_data_dir()); \
  RETURN_NOT_OK(s_); \
} while (0);

// Evaluates the expression and runs 'err_handler' if it results in an error.
// Returns if the status is an error.
#define RETURN_NOT_OK_HANDLE(status_expr, err_handler) do { \
  const Status& s_ = (status_expr); \
  if (PREDICT_TRUE(s_.ok())) { \
    break; \
  } \
  (err_handler); \
  RETURN_NOT_OK(s_); \
} while (0);

// Evaluates the expression and runs 'err_handler' if it results in a disk
// failure.
#define HANDLE_DISK_FAILURE(status_expr, err_handler) do { \
  const Status& s_ = (status_expr); \
  if (PREDICT_FALSE(IsDiskFailure(s_))) { \
      (err_handler); \
  } \
} while (0);

inline bool IsDiskFailure(const Status& s) {
  switch (s.posix_code()) {
    case EIO: // Fallthrough intended
    case ENODEV: // Fallthrough intended
    case ENOSPC: // Fallthrough intended
    case ENXIO: // Fallthrough intended
    case EROFS: // Fallthrough intended
      return true;
  }
  return false;
}

// When certain operations fail, the side effects of the failure span
// multiple layers, many of which we prefer to keep separate. To avoid breaking
// the layering, the FsErrorManager is owned by the FsManager and is used by
// components of other layers (e.g. TSTabletManager, BlockManager). For
// instance, the TSTabletManager registers a callback that blocks known to the
// BlockManager can call.
class FsErrorManager {
 public:
  FsErrorManager() : dd_manager_(nullptr), shutdown_replicas_cb_(nullptr) {}

  void SetTabletsFailedCallback(Callback<void(const std::set<std::string>&)>* cb) {
    shutdown_replicas_cb_ = cb;
  }

  void SetDataDirManager(DataDirManager* dd_manager) {
    dd_manager_ = dd_manager;
  }

  // Adds the dir to the DataDirManager's list of bad directories and fails
  // all tablets that were on it.
  //
  // This will render the directory unusable; new DataDirGroups created will
  // not include this directory, and any calls to create new blocks will avoid
  // block placement on this directory.
  void FailTabletsInDataDir(DataDir* dir) {
    // The callback may be null if the TSTabletManager has been deleted during
    // server shutdown or if the TSTabletManager has not yet been initialized.
    if (!shutdown_replicas_cb_) {
      return;
    }
    CHECK(dir);
    uint16_t uuid_idx;
    if (dd_manager_->FindUuidIndexByDataDir(dir, &uuid_idx)) {
      if (dd_manager_->IsDataDirFailed(uuid_idx)) {
        return;
      }
      dd_manager_->MarkDataDirFailed(uuid_idx);

      const std::set<std::string>& tablets_on_dir =
          dd_manager_->FindTabletsByDataDirUuidIdx(uuid_idx);
      if (!tablets_on_dir.empty()) {
        shutdown_replicas_cb_->Run(tablets_on_dir);
        return;
      }
    }
    LOG(ERROR) << strings::Substitute("Dir $0 not tracked by DataDirManager", dir->dir());
  }

 private:
  DataDirManager* dd_manager_;

  // Callback that fails the TSTabletManager's tablet peers.
  // The referenced TSTabletManager may be deleted before the FsErrorManager,
  // so this may be null.
  Callback<void(const std::set<std::string>&)>* shutdown_replicas_cb_;
};

}  // namespace fs
}  // namespace kudu
