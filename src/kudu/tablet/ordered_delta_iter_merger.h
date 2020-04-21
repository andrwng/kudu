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

#include <string>

#include "kudu/common/rowid.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/util/status.h"

namespace kudu {

class ColumnBlock;
class ScanSpec;
class Schema;
class SelectionVector;
struct ColumnId;

namespace tablet {

class OrderedDeltaIteratorMerger : public DeltaIterator {
 public:
  static Status Create(
      const std::vector<std::shared_ptr<DeltaStore>>& stores,
      const RowIteratorOptions& opts,
      std::unique_ptr<DeltaIterator>* out);
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
  explicit OrderedDeltaIteratorMerger(std::vector<std::unique_ptr<DeltaIterator> > iters) {}

  // The iterators to merge, not necessarily ordered.
  std::vector<std::unique_ptr<DeltaIterator> > iters_;
};

} // namespace tablet
} // namespace kudu

