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
#include <string>
#include <vector>

#include "kudu/common/rowid.h"
#include "kudu/fs/block_id.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/deltamemstore.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet_mem_trackers.h"
#include "kudu/util/status.h"

namespace kudu {

class FsManager;

namespace log {
class LogAnchorRegistry;
}

namespace tablet {

// XXX(awong): for the non-transactional rowset case, add constructors that
// convert redo_delta_stores_ and undo_delta_stores_ to atomics.
//
// XXX(awong): add constructor from existing transactional stores.
class AtomicRedoStores {
 public:
  static Status Open(FsManager* fs_manager,
                     const AtomicDeltaBlocks& delta_blocks,
                     log::LogAnchorRegistry* log_anchor_registry,
                     const TabletMemTrackers& mem_trackers,
                     const fs::IOContext* io_context,
                     std::unique_ptr<AtomicRedoStores>* redos);

  static Status Create(int64_t rowset_id,
                       log::LogAnchorRegistry* log_anchor_registry,
                       std::unique_ptr<AtomicRedoStores>* out);

  Status GetOrCreateDMS(int64_t rowset_id,
                        log::LogAnchorRegistry* log_anchor_registry,
                        std::shared_ptr<DeltaMemStore>* dms);

 private:
  AtomicRedoStores(int64_t last_flushed_dms_id,
                   SharedDeltaStoreVector redo_delta_stores)
      : next_dms_id_(last_flushed_dms_id + 1),
        redo_delta_stores_(std::move(redo_delta_stores)) {}

  AtomicRedoStores(std::shared_ptr<DeltaMemStore> dms)
      : next_dms_id_(dms->id() + 1), dms_(std::move(dms)) {}

  int64_t next_dms_id_;

  // The current DeltaMemStore into which updates should be written.
  std::shared_ptr<DeltaMemStore> dms_;
  // The set of tracked REDO delta stores, in increasing timestamp order.
  SharedDeltaStoreVector redo_delta_stores_;
};

class AtomicUndoStores {
 public:
 private:
  // The set of tracked UNDO delta stores, in decreasing timestamp order.
  // These track UNDOs for inserts and updates to a rowset whose base data was
  // inserted as a part of the transaction.
  SharedDeltaStoreVector undo_delta_stores_;
};

} // namespace tablet
} // namespace kudu
