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

#include "kudu/tablet/delta_store.h"

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
namespace tablet {

// Combines a group of input delta stores, returning deltas in the order
// specified for the given delta type.
template <DeltaType Type>
class DeltaStoreIteratorMerger : public DeltaStoreIterator {
 public:
  DeltaStoreIteratorMerger(std::vector<std::unique_ptr<DeltaStoreIterator>> iters)
      : iters_(std::move(iters)) {}

  Status Init(ScanSpec* spec) override;
  Status SeekToOrdinal(rowid_t idx) override;
  Status PrepareForBatch(size_t nrows) override;
  bool HasNext() const override;
  bool HasMoreBatches() const override;
  Status GetNextDelta(DeltaKey* key, Slice* slice) override;
  void IterateNext() override;
  void Finish(size_t nrows) override;

 private:
  // The store iteratores to merge.
  std::vector<std::unique_ptr<DeltaStoreIterator>> iters_;

  // We'll maintain a min-heap of the next row delta to apply.
  typedef std::pair<const DeltaKey&, DeltaStoreIterator*> KeyAndIter;
  template <DeltaType ComparatorDeltaType>
  struct KeyComparator {
    bool operator()(const KeyAndIter& a, const KeyAndIter& b) const {
      // NOTE: boost::heap defaults to a max-heap, so the comparator must be
      // inverted to yield a min-heap.
      return a.first.CompareTo<ComparatorDeltaType>(b.first) > 0;
    }
  };
  typedef boost::heap::skew_heap<KeyAndIter, boost::heap::compare<KeyComparator<Type>>> KeyMinHeap;
  KeyMinHeap key_heap_;
};

template <DeltaType Type>
class OrderedDeltaIteratorMerger :
    public DeltaPreparingIterator<DeltaPreparer<MergedDeltaPreparerTraits<Type>>,
                                  DeltaStoreIteratorMerger<Type>> {
 public:
  std::string ToString() const override {
    return "OrderedDeltaIteratorMerger";
  }
  const DeltaStoreIteratorMerger<Type>* store_iter() const override {
    return &store_iter_;
  }
  DeltaStoreIteratorMerger<Type>* store_iter() override {
    return &store_iter_;
  }
  const DeltaPreparer<MergedDeltaPreparerTraits<Type>>* preparer() const override {
    return &preparer_;
  }
  DeltaPreparer<MergedDeltaPreparerTraits<Type>>* preparer() override {
    return &preparer_;
  }
 private:
  DISALLOW_COPY_AND_ASSIGN(OrderedDeltaIteratorMerger<Type>);

  DeltaStoreIteratorMerger<Type> store_iter_;
  DeltaPreparer<MergedDeltaPreparerTraits<Type>> preparer_;
};

} // namespace tablet
} // namespace kudu
