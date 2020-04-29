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

#include <boost/heap/skew_heap.hpp>

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

// Iterates over a group of input delta store iterators, returning deltas in
// the order specified by the input iterators, but returning the deltas at a
// specified timestamp.
template <DeltaType Type>
class OrderedAtomicDeltaStoreMerger : public DeltaStoreIterator {
 public:
  Status Init(ScanSpec* spec) override;
  Status SeekToOrdinal(rowid_t idx) override;
  Status PrepareForBatch(size_t nrows) override;
  bool HasNext() const override;
  bool HasMoreBatches() const override;

  // Returns a delta key with the commit timestamp, and the slice for that
  // delta, which hasn't been rewritten.
  Status GetNextDelta(DeltaKey* key, Slice* slice) override;

  void IterateNext() override;
  void Finish(size_t nrows) override;
 private:
  // Timestamp at which the deltas the deltas were committed. If interleaving
  // this reader with another iterator, the deltas in this reader will seen as
  // being written at this timestamp and should be ordered as such.
  const Timestamp commit_timestamp_;

  // The iterators that are to be read at a single timestamp.
  std::vector<std::unique_ptr<DeltaStoreIterator>> iters_;

  // We'll maintain a min-heap of the next row to apply.
  typedef std::pair<const DeltaKey&, DeltaStoreIterator*> KeyAndIter;
  struct KeyComparator {
    bool operator()(const KeyAndIter& a, const KeyAndIter& b) const {
      // NOTE: boost::heap defaults to a max-heap, so the comparator must be
      // inverted to yield a min-heap.
      return a.first.CompareTo<Type>(b.first) > 0;
    }
  };
  typedef boost::heap::skew_heap<KeyAndIter, KeyComparator> KeyMinHeap;
  KeyMinHeap key_heap_;
};

// An merging iterator that iterates over multiple delta iterators, but views
// their deltas as if they were written at a single "commit timestamp".
//
// NOTE: the underlying iterators should be either DeltaMemStoreIterators or
// DelfaFileIterators.
template <DeltaType Type>
class MergedAtomicDeltasIterator : public DeltaIterator {
  // Constructs an iterator that merges the given atomic delta readers with the
  // given base delta store iterator.
  static Status Create(
      std::vector<std::unique_ptr<OrderedAtomicDeltaStoreMerger<Type>>> atomic_iters,
      std::unique_ptr<DeltaStoreIterator> base_store_iter,
      RowIteratorOptions opts,
      DeltaIterator* merged_iter);

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
  MergedAtomicDeltasIterator(
      std::vector<std::unique_ptr<OrderedAtomicDeltaStoreMerger<Type>>> atomic_iters,
      std::unique_ptr<DeltaStoreIterator> base_store_iter,
      RowIteratorOptions opts)
      : atomic_iters_(std::move(atomic_iters)),
        base_store_iter_(std::move(base_store_iter)),
        preparer_(std::move(opts)) {}

  // The iterators that are to be read at a single timestamp.
  std::vector<std::unique_ptr<OrderedAtomicDeltaStoreMerger<Type>>> atomic_iters_;
  std::unique_ptr<DeltaStoreIterator> base_store_iter_;
  DeltaPreparer<DeltaFilePreparerTraits<Type>> preparer_;
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
