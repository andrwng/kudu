#pragma once
#include "kudu/util/status.h"

namespace kudu {

class Env;
class FsManager;

namespace fs {
class DataDirManager;
class BlockManager;
}  // namespace fs

// Kudu uses numerous levels of abstraction to describe storage. To facilitate
// processes that span these multiple layers, this provides a single unit with
// which to operate.
// TODO(awong) consider that the FsManager is already privy to everything in
// this class. Hence, this class may be unecessary.
class StorageManagementContext {
 public:
  StorageManagementContext();

  void RegisterFsManager(FsManager* fs_manager) {
    CHECK(fs_manager && !fs_registered_);
    fs_registered_ = true;
    fs_manager_ = fs_manager;
  }

  void RegisterBlockManager(fs::BlockManager* block_manager) {
    CHECK(block_manager && !bm_registered_);
    bm_registered_ = true;
    block_manager_ = block_manager;
  }

  void RegisterDataDirManager(fs::DataDirManager* dd_manager) {
    CHECK(dd_manager && !dd_registered_);
    dd_registered_ = true;
    dd_manager_ = dd_manager;
  }

  void RegisterEnv(Env* env) {
    CHECK(env && !env_registered_);
    env_registered_ = true;
    env_ = env;
  }

  FsManager* fs_manager() {
    DCHECK(fs_registered_);
    return fs_manager_;
  };

  fs::BlockManager* block_manager() {
    DCHECK(bm_registered_);
    return block_manager_;
  }

  fs::DataDirManager* dd_manager() {
    DCHECK(dd_registered_);
    return dd_manager_;
  }
  
  Env* env() {
    DCHECK(env_registered_);
    return env_;
  }

 private:
  FsManager* fs_manager_;
  bool fs_registered_;

  fs::BlockManager* block_manager_;
  bool bm_registered_;

  fs::DataDirManager* dd_manager_;
  bool dd_registered_;

  Env* env_;
  bool env_registered_;
};

}  // namespace kudu
