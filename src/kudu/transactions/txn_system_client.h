// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/table_creator-internal.h"
#include "kudu/util/locks.h"
#include "kudu/util/status_callback.h"

namespace kudu {
namespace tserver {
class CoordinatorOpPB;
} // namespace tserver

namespace transactions {

// Wrapper around a KuduClient used by Kudu for making transaction-related
// calls to various servers.
class TxnSystemClient {
 public:
  static Status Create(const std::vector<std::string>& master_addrs,
                       std::unique_ptr<TxnSystemClient>* sys_client);

  // Creates the transaction status table with a single range partition of the
  // given upper bound.
  Status CreateTxnStatusTable(int64_t initial_upper_bound, int num_replicas = 1);

  // Adds a new range to the transaction status table with the given bounds.
  Status AddTxnStatusTableRange(int64_t lower_bound, int64_t upper_bound);

  // Attempts to create a transaction with the given 'txn_id'.
  Status BeginTransaction(int64_t txn_id, const std::string& user);
  Status RegisterParticipant(int64_t txn_id, const std::string& participant_id,
                             const std::string& user);

  // Opens the transaction status table, refreshing metadata with that from the
  // masters.
  Status OpenTxnStatusTable();

  client::sp::shared_ptr<client::KuduTable> txn_status_table() {
    std::lock_guard<simple_spinlock> l(table_lock_);
    return txn_status_table_;
  }

 private:
  TxnSystemClient(client::sp::shared_ptr<client::KuduClient> client)
      : client_(std::move(client)) {}

  Status CoordinateTransactionAsync(tserver::CoordinatorOpPB coordinate_txn_op,
                                    const MonoDelta& timeout,
                                    StatusCallback cb);


  client::sp::shared_ptr<client::KuduClient> client_;

  simple_spinlock table_lock_;
  client::sp::shared_ptr<client::KuduTable> txn_status_table_;
};

} // namespace transactions
} // namespace kudu

