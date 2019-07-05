// Copyright (c) 2014, Vlad Balan (vlad.gm@gmail.com).  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++
// for rocksdb::MergeOperator.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory>
#include <string>

#include "kudu/rocksdb/include/org_rocksdb_StringAppendOperator.h"
#include "kudu/rocksdb/include/org_rocksdb_UInt64AddOperator.h"
#include "kudu/rocksdb/db.h"
#include "kudu/rocksdb/memtablerep.h"
#include "kudu/rocksdb/merge_operator.h"
#include "kudu/rocksdb/options.h"
#include "kudu/rocksdb/slice_transform.h"
#include "kudu/rocksdb/statistics.h"
#include "kudu/rocksdb/table.h"
#include "kudu/rocksdb/rocksjni/portal.h"
#include "kudu/rocksdb/utilities/merge_operators.h"

/*
 * Class:     org_rocksdb_StringAppendOperator
 * Method:    newSharedStringAppendOperator
 * Signature: (C)J
 */
jlong Java_org_rocksdb_StringAppendOperator_newSharedStringAppendOperator(
    JNIEnv* /*env*/, jclass /*jclazz*/, jchar jdelim) {
  auto* sptr_string_append_op = new std::shared_ptr<rocksdb::MergeOperator>(
      rocksdb::MergeOperators::CreateStringAppendOperator((char)jdelim));
  return reinterpret_cast<jlong>(sptr_string_append_op);
}

/*
 * Class:     org_rocksdb_StringAppendOperator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_StringAppendOperator_disposeInternal(JNIEnv* /*env*/,
                                                           jobject /*jobj*/,
                                                           jlong jhandle) {
  auto* sptr_string_append_op =
      reinterpret_cast<std::shared_ptr<rocksdb::MergeOperator>*>(jhandle);
  delete sptr_string_append_op;  // delete std::shared_ptr
}

/*
 * Class:     org_rocksdb_UInt64AddOperator
 * Method:    newSharedUInt64AddOperator
 * Signature: ()J
 */
jlong Java_org_rocksdb_UInt64AddOperator_newSharedUInt64AddOperator(
    JNIEnv* /*env*/, jclass /*jclazz*/) {
  auto* sptr_uint64_add_op = new std::shared_ptr<rocksdb::MergeOperator>(
      rocksdb::MergeOperators::CreateUInt64AddOperator());
  return reinterpret_cast<jlong>(sptr_uint64_add_op);
}

/*
 * Class:     org_rocksdb_UInt64AddOperator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_UInt64AddOperator_disposeInternal(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong jhandle) {
  auto* sptr_uint64_add_op =
      reinterpret_cast<std::shared_ptr<rocksdb::MergeOperator>*>(jhandle);
  delete sptr_uint64_add_op;  // delete std::shared_ptr
}
