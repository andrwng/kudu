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

#include <string>
#include <thread>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_metadata_manager.h"
#include "kudu/util/barrier.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace kudu {

using strings::Substitute;

namespace tablet {

static const string kTabletId = "tablet";

class TabletMetadataStressTest : public KuduTest {
 public:
  TabletMetadataStressTest()
    : simple_schema_(SchemaBuilder(GetSimpleTestSchema()).Build()) {}

  void SetUp() override {
    LOG(INFO) << simple_schema_.ToString();
    TabletHarness::Options opts(GetTestPath("fs_root"));
    opts.tablet_id = kTabletId;
    tablet_harness_.reset(new TabletHarness(simple_schema_, std::move(opts)));
    CHECK_OK(tablet_harness_->Create(true));
    CHECK_OK(tablet_harness_->Open());

    fs_manager_ = tablet_harness_->fs_manager();
    tmeta_manager_ = tablet_harness_->tmeta_manager();
  }

  // Creates a rowset containing a single block.
  Status CreateRowSetMetadata(const string& rowset_id);

 protected:
  const Schema simple_schema_;

  unique_ptr<TabletHarness> tablet_harness_;
  FsManager* fs_manager_;
  TabletMetadataManager* tmeta_manager_;
};


// Test that makes many updates to a single tablet metadata from multiple
// threads. This is analagous to numerous compactions for a single tablet being
// run concurrently.
TEST_F(TabletMetadataStressTest, TestConcurrentBlockReplacement) {
  const int kNumThreads = 8;
  const int kNumRowSetsPerThread = 100;

  scoped_refptr<TabletMetadata> tmeta;
  ASSERT_OK(tmeta_manager_->Load(kTabletId, &tmeta));

  LOG_TIMING(INFO, Substitute("incrementally flushing to $0 rowsets in each of $1 threads",
                              kNumRowSetsPerThread, kNumThreads)) {
    // Create a bunch of rowsets.
    vector<thread> create_threads;
    Barrier create_barrier(kNumThreads);
    for (int thread_num = 0; thread_num < kNumThreads; thread_num++) {
      create_threads.emplace_back([tmeta, &create_barrier] {
        create_barrier.Wait();

        for (int rs = 0; rs < kNumRowSetsPerThread; rs++) {
          // Create a new RSM for the tablet.
          shared_ptr<RowSetMetadata> new_rsm;
          CHECK_OK(tmeta->CreateRowSet(&new_rsm));

          // Add the RSM to the metadata and flush it.
          RowSetMetadataVector to_add = { new_rsm };
          RowSetMetadataIds to_remove({});

          // For the purposes of this test, we're not going to synchronize the
          // flushes, so pass in kNoMrsFlushed for the last durable id.
          CHECK_OK(tmeta->UpdateAndFlush(to_remove, to_add, TabletMetadata::kNoMrsFlushed));
        }
      });
    }
    for (int thread_num = 0; thread_num < kNumThreads; thread_num++) {
      create_threads[thread_num].join();
    }
  }
  LOG_TIMING(INFO, Substitute("incrementally deleting $0 rowsets in each of $1 threads",
                              kNumRowSetsPerThread, kNumThreads)) {
    // Delete all of the rowsets.
    vector<thread> delete_threads;
    Barrier delete_barrier(kNumThreads);
    for (int thread_num = 0; thread_num < kNumThreads; thread_num++) {
      delete_threads.emplace_back([tmeta, thread_num, &delete_barrier] {
        delete_barrier.Wait();

        for (int rs = 0; rs < kNumRowSetsPerThread; rs++) {
          // Remove a RSM from the metadata and flush it. Note that because the
          // RSMs are empty, this doesn't trigger any block deletion. This
          // gives us slightly better insight into metadata-only IO.
          RowSetMetadataVector to_add = {};
          RowSetMetadataIds to_remove({ thread_num * kNumRowSetsPerThread + rs });
          CHECK_OK(tmeta->UpdateAndFlush(to_remove, to_add, TabletMetadata::kNoMrsFlushed));
        }
      });
    }
    for (int thread_num = 0; thread_num < kNumThreads; thread_num++) {
      delete_threads[thread_num].join();
    }
  }
}

TEST_F(TabletMetadataStressTest, TestConcurrentTabletUpdates) {

}

TEST_F(TabletMetadataStressTest, TestBootstrapWithManyAlters) {

}

// Test that attempts to flush and delete tablet metadata from multiple
// threads. This shouldn't happen because we a tablet before deleting it, thus
// preventing maintenance ops from coinciding with deletes.
TEST_F(TabletMetadataStressTest, TestManyFlushesAndDeletes) {

}


}  // namespace tablet
}  // namepscae kudu
