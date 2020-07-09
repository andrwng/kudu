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

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <sparsehash/dense_hash_set>

#include "kudu/client/client.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/atomic.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

namespace tserver {
class WriteResponsePB;
} // namespace tserver
namespace client {

class KuduStatusCallback;

namespace internal {

class ErrorCollector;
class RemoteTablet;

template <typename KuduOpType> struct InFlightOp;

template <typename KuduOpType, class ResponsePB>
class BatchedRpcResponseContainer {
 public:
  virtual const std::vector<InFlightOp<KuduOpType>*>& ops() const = 0;
  virtual const ResponsePB& resp() const = 0;
  virtual const std::string& tablet_id() const = 0;
};

// Handles a response from a batched RPC, handling the per-op errors from the
// given response type.
template <typename KuduOpType, class ResponsePB>
class BatchedRpcResponseHandler {
 public:
  typedef BatchedRpcResponseContainer<KuduOpType, ResponsePB> RpcResp;
  virtual void ProcessResponse(const RpcResp& rpc, const Status& s) = 0;
  virtual KuduClient* client() = 0;
  virtual kudu::client::KuduSession::ExternalConsistencyMode external_consistency_mode() const = 0;
};

// A Batcher is the class responsible for collecting row operations, routing them to the
// correct tablet server, and possibly batching them together for better efficiency.
//
// It is a reference-counted class: the client session creating the batch holds one
// reference, and all of the in-flight operations hold others. This allows the client
// session to be destructed while ops are still in-flight, without the async callbacks
// attempting to access a destructed Batcher.
template <typename KuduOpType>
class RpcBatcher {
 public:
  typedef std::function<void(InFlightOp<KuduWriteOperation>*, const Status&)> AddErrorFunc;

  // Create a new batcher associated with the given session.
  //
  // Any errors which come back from operations performed by this batcher
  // are posted to the provided ErrorCollector.
  //
  // Takes a reference on error_collector. Takes a weak_ptr to session -- this
  // is to break circular dependencies (a session keeps a reference to its
  // current batcher) and make it possible to call notify a session
  // (if it's around) from a batcher which does its job using other threads.
  RpcBatcher(KuduClient* client,
             AddErrorFunc add_error_func,
             client::sp::weak_ptr<KuduSession> session,
             kudu::client::KuduSession::ExternalConsistencyMode consistency_mode);

  // Abort the current batch. Any writes that were buffered and not yet sent are
  // discarded. Those that were sent may still be delivered.  If there is a pending Flush
  // callback, it will be called immediately with an error status.
  virtual void Abort();

  // Set the timeout for this batcher.
  //
  // The timeout is currently set on all of the RPCs, but in the future will be relative
  // to when the Flush call is made (eg even if the lookup of the TS takes a long time, it
  // may time out before even sending an op). TODO: implement that
  virtual void SetTimeout(const MonoDelta& timeout);

  // Add a new operation to the batch. Requires that the batch has not yet been flushed.
  //
  // NOTE: If this returns not-OK, does not take ownership of 'op'.
  virtual Status Add(KuduOpType* op) WARN_UNUSED_RESULT;

  // Return true if any operations are still pending. An operation is no longer considered
  // pending once it has either errored or succeeded.  Operations are considering pending
  // as soon as they are added, even if Flush has not been called.
  virtual bool HasPendingOperations() const;

  // Return the number of buffered operations. These are only those operations which are
  // "corked" (i.e not yet flushed). Once Flush has been called, this returns 0.
  virtual int CountBufferedOperations() const;

  // Flush any buffered operations. The callback will be called once there are no
  // more pending operations from this Batcher. If all of the operations succeeded,
  // then the callback will receive Status::OK. Otherwise, it will receive IOError,
  // and the caller must inspect the ErrorCollector to retrieve more detailed
  // information on which operations failed.
  virtual void FlushAsync(KuduStatusCallback* cb);

  // Get time of the first operation in the batch.  If no operations are in
  // there yet, the returned MonoTime object is not initialized
  // (i.e. MonoTime::Initialized() returns false).
  virtual const MonoTime& first_op_time() const {
    std::lock_guard<simple_spinlock> l(lock_);
    return first_op_time_;
  }

  // Return the total size (number of bytes) of all pending write operations
  // accumulated by the batcher.
  virtual int64_t buffer_bytes_used() const {
    return buffer_bytes_used_.Load();
  }

  // Compute in-buffer size for the given write operation.
  static int64_t GetOperationSizeInBuffer(KuduOpType* write_op) {
    return write_op->SizeInBuffer();
  }

 protected:
  template <class BatcherType, typename OpType, class RequestPB, class ResponsePB>
      friend class BatchedRpc;

  virtual void FlushBuffer(RemoteTablet* tablet, const std::vector<InFlightOp<KuduOpType>*>& ops)
      = 0;
  virtual void AsyncLookup(const KuduTable* table,
                           std::string partition_key,
                           const MonoTime& deadline,
                           MetaCache::LookupType lookup_type,
                           InFlightOp<KuduOpType>* in_flight) = 0;

  ~RpcBatcher();

  // Add an op to the in-flight set and increment the ref-count.
  void AddInFlightOp(InFlightOp<KuduOpType>* op);

  void RemoveInFlightOp(InFlightOp<KuduOpType>* op);

  // Return true if the batch has been aborted, and any in-flight ops should stop
  // processing wherever they are.
  bool IsAbortedUnlocked() const;

  // Mark the fact that errors have occurred with this batch. This ensures that
  // the flush callback will get a bad Status.
  void MarkHadErrors();

  // Remove an op from the in-flight op list, and delete the op itself.
  // The operation is reported to the ErrorReporter as having failed with the
  // given status.
  void MarkInFlightOpFailedUnlocked(InFlightOp<KuduOpType>* op, const Status& s);

  void CheckForFinishedFlush();
  void FlushBuffersIfReady();

  // Async Callbacks.
  void TabletLookupFinished(InFlightOp<KuduOpType>* op, const Status& s);

  // Compute a new deadline based on timeout_. If no timeout_ has been set,
  // uses a hard-coded default and issues periodic warnings.
  MonoTime ComputeDeadlineUnlocked() const;

  // See note about lock ordering in batcher.cc
  mutable simple_spinlock lock_;

  enum State {
    kGatheringOps,
    kFlushing,
    kFlushed,
    kAborted
  };
  State state_;

  KuduClient* const client_;
  client::sp::weak_ptr<KuduSession> weak_session_;

  // The consistency mode set in the session.
  kudu::client::KuduSession::ExternalConsistencyMode consistency_mode_;

  const AddErrorFunc add_error_func_unlocked_;

  // The time when the very first operation was added into the batcher.
  MonoTime first_op_time_;

  // Set to true if there was at least one error from this Batcher.
  // Protected by lock_
  bool had_errors_;

  // If state is kFlushing, this member will be set to the user-provided
  // callback. Once there are no more in-flight operations, the callback
  // will be called exactly once (and the state changed to kFlushed).
  KuduStatusCallback* flush_callback_;

  // All buffered or in-flight ops.
  google::dense_hash_set<InFlightOp<KuduOpType>*> ops_;
  // Each tablet's buffered ops.
  std::unordered_map<RemoteTablet*, std::vector<InFlightOp<KuduOpType>*> > per_tablet_ops_;

  // When each operation is added to the batcher, it is assigned a sequence number
  // which preserves the user's intended order. Preserving order is critical when
  // a batch contains multiple operations against the same row key. This member
  // assigns the sequence numbers.
  // Protected by lock_.
  int next_op_sequence_number_;

  // Amount of time to wait for a given op, from start to finish.
  //
  // Set by SetTimeout().
  MonoDelta timeout_;

  // After flushing, the absolute deadline for all in-flight ops.
  MonoTime deadline_;

  // Number of outstanding lookups across all in-flight ops.
  //
  // Note: _not_ protected by lock_!
  Atomic32 outstanding_lookups_;

  // The number of bytes used in the buffer for pending operations.
  AtomicInt<int64_t> buffer_bytes_used_;
};

class Batcher :
    public RpcBatcher<KuduWriteOperation>,
    public BatchedRpcResponseHandler<KuduWriteOperation, tserver::WriteResponsePB>,
    public RefCountedThreadSafe<Batcher> {
 public:
  Batcher(KuduClient* client,
          scoped_refptr<ErrorCollector> error_collector,
          sp::weak_ptr<KuduSession> session,
          kudu::client::KuduSession::ExternalConsistencyMode consistency_mode);

  // Processes the given response with the given RPC-wide status 's'. If 's' is
  // OK, processes the per-row errors, if any; otherwise, marks all rows
  // operations as failed.
  //
  // Frees the ops from the underlying RpcBatcher's 'ops_'.
  void ProcessResponse(const RpcResp& rpc, const Status& s) override;

  KuduClient* client() override { return client_; }

  // Returns the consistency mode set on the batcher by the session when it was initially
  // created.
  kudu::client::KuduSession::ExternalConsistencyMode external_consistency_mode() const override {
    return consistency_mode_;
  }

  void FlushBuffer(RemoteTablet* tablet,
                   const std::vector<InFlightOp<KuduWriteOperation>*>& ops) override;
  void AsyncLookup(const KuduTable* table,
                   std::string partition_key,
                   const MonoTime& deadline,
                   MetaCache::LookupType lookup_type,
                   InFlightOp<KuduWriteOperation>* in_flight) override;

 private:
  friend class RefCountedThreadSafe<Batcher>;
  virtual ~Batcher() {}

  // Function to run to indicate that the given op failed with the given error.
  void MarkInFlightOpFailed(InFlightOp<KuduWriteOperation>* op, const Status& s);

  // Errors are reported into this error collector.
  scoped_refptr<ErrorCollector> error_collector_;
  DISALLOW_COPY_AND_ASSIGN(Batcher);
};

} // namespace internal
} // namespace client
} // namespace kudu
