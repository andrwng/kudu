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

#include "kudu/tablet/delta_store_merger.h"

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
using strings::Substitute;

namespace kudu {
namespace tablet {

template<DeltaType Type>
Status DeltaStoreIteratorMerger<Type>::Init(ScanSpec* spec) {
  for (const auto& iter : iters_) {
    RETURN_NOT_OK(iter->Init(spec));
  }
  return Status::OK();
}

template<DeltaType Type>
Status DeltaStoreIteratorMerger<Type>::SeekToOrdinal(rowid_t idx) {
  for (const auto& iter : iters_) {
    RETURN_NOT_OK(iter->SeekToOrdinal(idx));
  }
  return Status::OK();
}

template<DeltaType Type>
Status DeltaStoreIteratorMerger<Type>::PrepareForBatch(size_t nrows) {
  DCHECK(key_heap_.empty());
  for (const auto& iter : iters_) {
    RETURN_NOT_OK(iter->PrepareForBatch(nrows));
    DeltaKey key;
    Slice slice;
    RETURN_NOT_OK(iter->GetNextDelta(&key, &slice));
    key_heap_.emplace(std::make_pair(key, iter.get()));
  }
  return Status::OK();
}

template<DeltaType Type>
bool DeltaStoreIteratorMerger<Type>::HasPreparedNext() const {
  return !key_heap_.empty() && key_heap_.top().second->HasPreparedNext();
}

template<DeltaType Type>
bool DeltaStoreIteratorMerger<Type>::HasMoreDeltas() const {
  for (const auto& iter : iters_) {
    if (iter->HasMoreDeltas()) {
      return true;
    }
  }
  return true;
}

template<DeltaType Type>
Status DeltaStoreIteratorMerger<Type>::GetNextDelta(DeltaKey* key, Slice* slice) {
  DCHECK(HasPreparedNext());
  RETURN_NOT_OK(key_heap_.top().second->GetNextDelta(key, slice));
  return Status::OK();
}

template<DeltaType Type>
void DeltaStoreIteratorMerger<Type>::IterateNext() {
  DCHECK(HasPreparedNext());
  // Move the front iterator forward and refresh its place in the heap.
  auto* iter = key_heap_.top().second;
  iter->IterateNext();
  if (!iter->HasPreparedNext()) {
    key_heap_.pop();
    return;
  }
  DeltaKey key;
  Slice slice;
  Status s = iter->GetNextDelta(&key, &slice);
  if (!s.ok()) {
    // The next time GetNextDelta() gets called, it will fail.
    return;
  }
  key_heap_.pop();
  key_heap_.emplace(std::make_pair(key, iter));
}
template<DeltaType Type>
void DeltaStoreIteratorMerger<Type>::Finish(size_t nrows) {
  for (auto& iter : iters_) {
    iter->Finish(nrows);
  }
  // Reset the heap -- the next batch iteration will repopulate and reorder
  // based on the next batche's worth in each store iterator.
  key_heap_.clear();
}

template struct MergedDeltaPreparerTraits<UNDO>;
template struct MergedDeltaPreparerTraits<REDO>;
template class DeltaStoreIteratorMerger<UNDO>;
template class DeltaStoreIteratorMerger<REDO>;

template class OrderedDeltaIteratorMerger<UNDO>;
template class OrderedDeltaIteratorMerger<REDO>;

template<DeltaType Type>
OrderedDeltaIteratorMerger<Type>::OrderedDeltaIteratorMerger(
    vector<unique_ptr<DeltaStoreIterator>> store_iters,
    const RowIteratorOptions& opts)
  : merged_store_iter_(std::move(store_iters)),
    preparer_(opts) {}

template<DeltaType Type>
Status OrderedDeltaIteratorMerger<Type>::Create(vector<shared_ptr<DeltaStore>> stores,
                                                const RowIteratorOptions& opts,
                                                unique_ptr<DeltaIterator>* out) {
  vector<unique_ptr<DeltaStoreIterator>> iterators;
  for (const auto& store : stores) {
    unique_ptr<DeltaStoreIterator> iter;
    Status s = store->NewDeltaStoreIterator(opts, &iter);
    if (s.IsNotFound()) {
      continue;
    }
    RETURN_NOT_OK_PREPEND(s, Substitute("Could not create iterator for store $0",
                                       store->ToString()));
    iterators.emplace_back(std::move(iter));
  }
  out->reset(new OrderedDeltaIteratorMerger(std::move(iterators)), opts);
}

} // namespace tablet
} // namespace kudu

