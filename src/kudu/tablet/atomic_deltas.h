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
} // namespace log

namespace tablet {

// Encapsulates the state required to merge a committed atomic iterator with
// another iterator. The deltas should be ordered by (key, timestamp).
//
// A DeltaIterator can use this during its call to PrepareBatch() to mix in
// deltas from multiple atomic delta stores.
class AtomicDeltasReader {
 public:
  Status ReadCurrentDelta(DeltaKey* key, Slice* slice);

  // Reads the next delta.
  Status ReadNextDelta(DeltaKey* key, Slice* slice);

  // Seek to a particular ordinal position in the delta data. This cancels any
  // prepared block, and must be called at least once prior to PrepareBatch().
  Status SeekToOrdinal(rowid_t idx);

  // Prepares all underlying iterators to read deltas for the next 'nrows'
  // rows. Note that this doesn't read 'nrows' deltas, but rather reads deltas
  // that are relevant for the next 'nrows' rows.
  Status PrepareBatch(size_t nrows, int prepare_flags);

  // Returns true if at least one iterator has more deltas to read for the
  // current batch.
  bool HasNext();
 private:
  // Timestamp at which the deltas the deltas were committed. If interleaving
  // this reader with another iterator, the deltas in this reader will seen as
  // being written at this timestamp and should be ordered as such.
  const Timestamp commit_timestamp_;

  // The iterators that are to be read at a single timestamp.
  std::vector<std::unique_ptr<DeltaIterator> > iters_;
};

// An merging iterator that iterates over multiple delta iterators, but views
// their deltas as if they were written at a single "commit timestamp".
//
// NOTE: the underlying iterators should be either DeltaMemStoreIterators or
// DelfaFileIterators.
class MergedAtomicDeltasIterator : public DeltaIterator {
  // Constructs an iterator that has the commit timestamp and the given base
  // options. Based on opts.snap_to_exclude and opts.snap_to_include, and the
  // given delta type, construct a merged iterator that has iterators for the
  // given stores.
  //
  // Returns NotFound() the stores are not relevant, per the iteration options.
  static Status Create(const Timestamp& commit_timestamp, const RowIteratorOptions& opts,
                       const SharedDeltaStoreVector& input_stores);

  Status Init(ScanSpec* spec) override { return Status::OK(); }
  Status SeekToOrdinal(rowid_t idx) override { return Status::OK(); }
  Status PrepareBatch(size_t nrows, int prepare_flags) override { return Status::OK(); }
  Status ApplyUpdates(size_t col_to_apply, ColumnBlock* dst,
                      const SelectionVector& filter) override { return Status::OK(); }
  Status ApplyDeletes(SelectionVector* sel_vec) override { return Status::OK(); }
  Status SelectDeltas(SelectedDeltas* deltas) override { return Status::OK(); }
  Status CollectMutations(std::vector<Mutation*>* dst, Arena* arena) override { return Status::OK(); }
  Status FilterColumnIdsAndCollectDeltas(const std::vector<ColumnId>& col_ids,
                                         std::vector<DeltaKeyAndUpdate>* out,
                                         Arena* arena) override { return Status::OK(); }
  bool HasNext() override { return true; }
  bool MayHaveDeltas() const override { return true; }
  std::string ToString() const override { return ""; }
 private:
  const Timestamp commit_timestamp_;
  // The iterators that are to be read at a single timestamp.
  std::vector<std::unique_ptr<DeltaIterator> > iters_;
};

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
  AtomicRedoStores(int64_t last_flushed_dms_id, SharedDeltaStoreVector redo_delta_stores)
      : next_dms_id_(last_flushed_dms_id + 1),
        redo_delta_stores_(std::move(redo_delta_stores)) {}

  AtomicRedoStores(std::shared_ptr<DeltaMemStore> dms)
      : next_dms_id_(dms->id() + 1),
        dms_(std::move(dms)) {}

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
