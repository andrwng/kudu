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
#include "kudu/tablet/atomic_deltas.h"

#include <memory>
#include <string>
#include <vector>

#include "kudu/common/rowid.h"
#include "kudu/fs/block_id.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/deltamemstore.h"
#include "kudu/tablet/tablet_mem_trackers.h"
#include "kudu/util/status.h"

// TODO refactor
#include "kudu/cfile/cfile_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/io_context.h"
#include "kudu/tablet/delta_tracker.h"
#include "kudu/tablet/deltafile.h"

using kudu::cfile::ReaderOptions;
using kudu::fs::ReadableBlock;
using kudu::fs::IOContext;
using std::vector;
using std::string;
using std::shared_ptr;
using std::unique_ptr;

namespace kudu {
namespace tablet {

namespace {
Status OpenDeltaReaders(FsManager* fs,
                        vector<DeltaBlockIdAndStats> blocks,
                        const IOContext* io_context,
                        vector<shared_ptr<DeltaStore> >* stores,
                        DeltaType type) {
  for (auto& block_and_stats : blocks) {
    const auto& block_id = block_and_stats.first;
    unique_ptr<DeltaStats> stats = std::move(block_and_stats.second);
    unique_ptr<ReadableBlock> block;
    Status s = fs->OpenBlock(block_id, &block);
    RETURN_NOT_OK_PREPEND(s, "Failed to open atomic delta file");

    shared_ptr<DeltaFileReader> dfr;
    ReaderOptions options;
    options.parent_mem_tracker = nullptr; // TODO
    options.io_context = io_context;
    s = DeltaFileReader::OpenNoInit(std::move(block),
                                    type,
                                    std::move(options),
                                    std::move(stats),
                                    &dfr);
    RETURN_NOT_OK_PREPEND(s, "Failed to open delta file reader");
    stores->emplace_back(dfr);
  }
  return Status::OK();
}
} // anonymous namespace

Status AtomicRedoStores::Open(FsManager* fs_manager,
                              const AtomicDeltaBlocks& delta_blocks,
                              log::LogAnchorRegistry* log_anchor_registry,
                              const TabletMemTrackers& mem_trackers,
                              const fs::IOContext* io_context,
                              std::unique_ptr<AtomicRedoStores>* redos) {
  if (!delta_blocks.last_flushed_dms_id) {
    return Status::Corruption("on-disk atomic redo stores should have a flushd DMS id");
  }
  vector<DeltaBlockIdAndStats> redos_with_stats;
  for (const auto& block_id : delta_blocks.delta_blocks)  {
    redos_with_stats.emplace_back(std::make_pair(block_id, nullptr));
  }
  SharedDeltaStoreVector redo_stores;
  RETURN_NOT_OK(OpenDeltaReaders(fs_manager,
                                 std::move(redos_with_stats),
                                 io_context,
                                 &redo_stores,
                                 REDO));
  redos->reset(new AtomicRedoStores(*delta_blocks.last_flushed_dms_id, std::move(redo_stores)));
  return Status::OK();
}

Status AtomicRedoStores::Create(int64_t rowset_id,
                                log::LogAnchorRegistry* log_anchor_registry,
                                std::unique_ptr<AtomicRedoStores>* out) {
  shared_ptr<DeltaMemStore> dms;
  RETURN_NOT_OK(DeltaMemStore::Create(1, rowset_id, log_anchor_registry,
                                      /*memtracker*/nullptr, &dms));
  RETURN_NOT_OK(dms->Init(/*io_context*/nullptr));
  out->reset(new AtomicRedoStores(std::move(dms)));
  return Status::OK();
}

Status AtomicRedoStores::GetOrCreateDMS(int64_t rowset_id,
                                        log::LogAnchorRegistry* log_anchor_registry,
                                        shared_ptr<DeltaMemStore>* dms) {
  if (dms_) {
    *dms = dms_;
    return Status::OK();
  }
  return DeltaMemStore::Create(next_dms_id_++, rowset_id, log_anchor_registry,
                               /*memtracker*/nullptr, &dms_);
}

} // namespace tablet
} // namespace kudu
