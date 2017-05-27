#pragma once

#include <set>
#include <string>

#include "kudu/gutil/callback_forward.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/util/fault_injection.h"

namespace kudu {
namespace fs {

// Evaluate the expression and if it results in an EIO, handle it.
// Return if the status is an error.
#define KUDU_RETURN_AND_HANDLE_EIO(expr) do { \
  KUDU_RETURN_AND_HANDLE(EIO, (expr), HandleEIO()); \
} while (0);

// When certain operations fail, the side effects of the failure span
// multiple layers, many of which we prefer to keep separate. To avoid breaking
// the layering, the FsErrorManager exists as an entity that is owned by the
// FsManager, and used by components of other layers (e.g. TSTabletManager,
// BlockManager). For instance, the TSTabletManager registers a callback that
// blocks known to the BlockManager can call.
class FsErrorManager {
 public:
  FsErrorManager() : dd_manager_(nullptr), shutdown_replicas_cb_(nullptr) {}

  void SetTabletsFailedCallback(Callback<void(const std::set<std::string>&)>* cb) {
    shutdown_replicas_cb_ = cb;
  }

  void SetDataDirManager(DataDirManager* dd_manager) {
    // The DataDirManager should only be set once.
    DCHECK(dd_manager_ == nullptr);
    dd_manager_ = dd_manager;
  }

  // Adds the dir to the DataDirManager's list of bad directories and fails
  // all tablets that were on it.
  //
  // This will render the directory unusable; new DataDirGroups created will
  // not include this directory, and any calls to create new blocks will avoid
  // block placement on this directory.
  void HandleDataDirFailure(DataDir* dir) {
    CHECK(dir);
    CHECK(shutdown_replicas_cb_);
    LOG(WARNING) << strings::Substitute("Dir $0 failed, marking for failure", dir->dir());
    uint16_t uuid_idx;
    if (dd_manager_->FindUuidIndexByDataDir(dir, &uuid_idx)) {
      // If the directory is registered as failed, ignore it.
      if (dd_manager_->IsDataDirFailed(uuid_idx)) {
        return;
      }
      dd_manager_->MarkDataDirFailed(uuid_idx);
      std::set<std::string> tablets_on_dir = dd_manager_->FindTabletsByDataDirUuidIdx(uuid_idx);
      if (!tablets_on_dir.empty()) {
        shutdown_replicas_cb_->Run(std::move(tablets_on_dir));
        return;
      }
      LOG(ERROR) << strings::Substitute("Dir with UUID index $0 not tracked by DataDirManager",
                                        uuid_idx);
      return;
    }
    LOG(ERROR) << strings::Substitute("Dir $0 not tracked by DataDirManager", dir->dir());
  }

 private:
  // Must outlive the eio error manager.
  DataDirManager* dd_manager_;

  // Callback that fails the TSTabletManager's tablet peers.
  // Referenced TSTabletManager must outlive the eio error manager.
  Callback<void(const std::set<std::string>&)>* shutdown_replicas_cb_;
};

}  // namespace fs
}  // namespace kudu
