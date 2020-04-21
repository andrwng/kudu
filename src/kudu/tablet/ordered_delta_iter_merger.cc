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

#include "kudu/tablet/ordered_delta_iter_merger.h"

#include <string>
#include <memory>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/rowset.h"

using std::shared_ptr;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

// XXX(awong): Also take as input the atomic store lists.
//
// Only stores that definitely aren't a part of the snapshot can be removed
// here. This is called while the tablet's component lock, the DRS's component
// lock, and the delta tracker's component lock are held.
//
// If we're scanning, we've already waited for in-flights from before the
// snapshot to become committed.
//
// I.e. we have waited for anything that may have committed before the given
// timestamp to become committed. As such, we can look for any state that is
// apparently committed. Since the wait is applied across all tablet state,
// this should be consistent across rowsets.
//
// I.e. this should take as input some committed atomic delta stores.
Status OrderedDeltaIteratorMerger::Create(
    const vector<shared_ptr<DeltaStore>>& stores,
    const RowIteratorOptions& opts,
    unique_ptr<DeltaIterator>* out) {
  vector<unique_ptr<DeltaIterator> > delta_iters;
  for (const shared_ptr<DeltaStore> &store : stores) {
    unique_ptr<DeltaIterator> iter;
    Status s = store->NewDeltaIterator(opts, &iter);
    if (s.IsNotFound()) {
      continue;
    }
    RETURN_NOT_OK_PREPEND(s, Substitute("Could not create iterator for store $0",
                                        store->ToString()));

    delta_iters.emplace_back(std::move(iter));
  }

  if (delta_iters.size() == 1) {
    // If we only have one input to the "merge", we can just directly
    // return that iterator.
    *out = std::move(delta_iters[0]);
  } else {
    out->reset(new OrderedDeltaIteratorMerger(std::move(delta_iters)));
  }
  return Status::OK();
}

} // namespace tablet
} // namespace kudu
