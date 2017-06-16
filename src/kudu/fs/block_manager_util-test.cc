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
#include "kudu/fs/block_manager_util.h"

#include <set>
#include <string>
#include <vector>

#include <google/protobuf/repeated_field.h>
#include <gtest/gtest.h>

#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/path_util.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace fs {

using google::protobuf::RepeatedField;
using google::protobuf::RepeatedPtrField;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

TEST_F(KuduTest, Lifecycle) {
  string kType = "asdf";
  string kFileName = GetTestPath("foo");
  string kUuid = "a_uuid";
  string kPath = "a_path";

  // Test that the metadata file was created.
  {
    PathInstanceMetadataFile file(env_, kType, kFileName);
    ASSERT_OK(file.Create(kUuid, { kUuid }, { kPath }));
  }
  ASSERT_TRUE(env_->FileExists(kFileName));

  // Test that we could open and parse it.
  {
    PathInstanceMetadataFile file(env_, kType, kFileName);
    ASSERT_OK(file.LoadFromDisk());
    PathInstanceMetadataPB* md = file.metadata();
    ASSERT_EQ(kType, md->block_manager_type());
    const PathSetPB& path_set = md->path_set();
    ASSERT_EQ(kUuid, path_set.uuid());
    ASSERT_EQ(1, path_set.all_paths_size());
    ASSERT_EQ(kUuid, path_set.all_paths(0).uuid());
    ASSERT_EQ(PathHealthStatePB::HEALTHY, path_set.all_paths(0).health_state());
    ASSERT_EQ(kPath, path_set.all_paths(0).path());
  }

  // Test that expecting a different type of block manager fails.
  {
    PathInstanceMetadataFile file(env_, "other type", kFileName);
    PathInstanceMetadataPB pb;
    ASSERT_TRUE(file.LoadFromDisk().IsIOError());
  }
}

TEST_F(KuduTest, Locking) {
  string kType = "asdf";
  string kFileName = GetTestPath("foo");
  string kUuid = "a_uuid";
  string kPath = "a_path";

  PathInstanceMetadataFile file(env_, kType, kFileName);
  ASSERT_OK(file.Create(kUuid, { kUuid }, { kPath }));

  PathInstanceMetadataFile first(env_, kType, kFileName);
  ASSERT_OK(first.LoadFromDisk());
  ASSERT_OK(first.Lock());

  ASSERT_DEATH({
    PathInstanceMetadataFile second(env_, kType, kFileName);
    CHECK_OK(second.LoadFromDisk());
    CHECK_OK(second.Lock());
  }, "Could not lock");

  ASSERT_OK(first.Unlock());
  ASSERT_DEATH({
    PathInstanceMetadataFile second(env_, kType, kFileName);
    CHECK_OK(second.LoadFromDisk());
    Status s = second.Lock();
    if (s.ok()) {
      LOG(FATAL) << "Lock successfully acquired";
    } else {
      LOG(FATAL) << "Could not lock: " << s.ToString();
    }
  }, "Lock successfully acquired");
}

static void RunCheckIntegrityTest(Env* env,
                                  const vector<PathSetPB>& path_sets,
                                  const string& expected_status_string,
                                  const set<int>& invalid_indices = {}) {
  vector<PathInstanceMetadataFile*> instances;
  ElementDeleter deleter(&instances);
  PathSetPB main_path_set;
  main_path_set.set_uuid("test_path_set");

  int i = 0;
  for (const PathSetPB& ps : path_sets) {
    unique_ptr<PathInstanceMetadataFile> instance(
        new PathInstanceMetadataFile(env, "asdf", Substitute("/tmp/$0/instance", i)));
    unique_ptr<PathInstanceMetadataPB> metadata(new PathInstanceMetadataPB());
    metadata->set_block_manager_type("asdf");
    metadata->set_filesystem_block_size_bytes(1);
    metadata->mutable_path_set()->CopyFrom(ps);
    instance->SetMetadataForTests(std::move(metadata));
    if (ContainsKey(invalid_indices, i)) {
      instance->SetValidInstance(false);
    }
    instances.push_back(instance.release());
    i++;
  }

  vector<int> updated_indices;
  EXPECT_EQ(expected_status_string,
            PathInstanceMetadataFile::CheckIntegrity(instances, &main_path_set,
                &updated_indices).ToString());
}

TEST_F(KuduTest, CheckIntegrity) {
  vector<string> uuids = { "fee", "fi", "fo", "fum" };
  vector<PathHealthStatePB> hs(4, PathHealthStatePB::HEALTHY);
  vector<string> paths = { "/tmp/0", "/tmp/1", "/tmp/2", "/tmp/3" };
  vector<PathPB> path_pbs;
  for (int i = 0; i < paths.size(); i++) {
    PathPB pb;
    pb.set_uuid(uuids[i]);
    pb.set_path(paths[i]);
    pb.set_health_state(hs[i]);
    path_pbs.emplace_back(pb);
  }
  RepeatedPtrField<PathPB> kAllPaths(path_pbs.begin(), path_pbs.end());

  // Initialize path_sets to be fully healthy and consistent.
  vector<PathSetPB> path_sets(uuids.size());
  for (int i = 0; i < path_sets.size(); i++) {
    PathSetPB& ps = path_sets[i];
    ps.mutable_all_paths()->Reserve(path_sets.size());
    ps.set_uuid(uuids[i]);
    ps.mutable_all_paths()->CopyFrom(kAllPaths);
    ps.set_timestamp_us(1);
  }

  {
    // Test consistent path sets.
    EXPECT_NO_FATAL_FAILURE(RunCheckIntegrityTest(env_, path_sets, "OK"));
  }
  {
    // Test where two path sets claim the same UUID.
    vector<PathSetPB> path_sets_copy(path_sets);
    path_sets_copy[1].set_uuid(path_sets_copy[0].uuid());
    EXPECT_NO_FATAL_FAILURE(RunCheckIntegrityTest(
        env_, path_sets_copy,
        "IO error: Data directories /tmp/0 and /tmp/1 have duplicate instance metadata UUIDs: "
        "fee"));
  }
  {
    // Test where the path sets have duplicate UUIDs.
    vector<PathSetPB> path_sets_copy(path_sets);
    PathPB extra_pb;
    extra_pb.set_uuid("fee");
    extra_pb.set_path("/tmp/4");
    extra_pb.set_health_state(PathHealthStatePB::HEALTHY);
    for (PathSetPB& ps : path_sets_copy) {
      ps.add_all_paths();
      *ps.mutable_all_paths(path_sets.size()) = extra_pb;
    }
    EXPECT_NO_FATAL_FAILURE(RunCheckIntegrityTest(
        env_, path_sets_copy,
        "IO error: Data directory /tmp/0 instance metadata path set contains duplicate UUIDs: "
        "fee,fi,fo,fum,fee"));
  }
  {
    // Test where a path set claims a UUID that isn't in all_paths.
    vector<PathSetPB> path_sets_copy(path_sets);
    path_sets_copy[1].set_uuid("something_else");
    EXPECT_NO_FATAL_FAILURE(RunCheckIntegrityTest(
        env_, path_sets_copy,
        "IO error: Data directory /tmp/1 instance metadata contains unexpected UUID: "
        "something_else"));
  }
  {
    // Test removing a path from the set.
    vector<PathSetPB> path_sets_copy(path_sets);
    path_sets_copy.resize(1);
    EXPECT_NO_FATAL_FAILURE(RunCheckIntegrityTest(
        env_, path_sets_copy,
        "IO error: 1 data directories provided, but expected 4"));
  }
  // Disk failure limitations:
  // If there is a restart during which the only healthy directories become
  // failed and some failed directories become healthy, there is a risk of
  // inconsistency. In this case, the tablets on the disk may try to start up.
  {
    // Test startup with all invalid instances. This emulates every disk
    // failing while trying to read its instance.
    const vector<PathSetPB>& path_sets_copy(path_sets);
    EXPECT_NO_FATAL_FAILURE(RunCheckIntegrityTest(
      env_, path_sets_copy,
      "IO error: All data directories are marked invalid due to disk failure",
      {0, 1, 2, 3}));
  }
  {
    // Test startup with some invalid instances. This emulates some disks
    // failing while trying to read its instance.
    const vector<PathSetPB>& path_sets_copy(path_sets);
    EXPECT_NO_FATAL_FAILURE(RunCheckIntegrityTest(
      env_, path_sets_copy, "OK", {0, 1}));
  }
}

} // namespace fs
} // namespace kudu
