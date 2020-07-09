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
#include <string>
#include <vector>

#include "kudu/client/batcher.h"
#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/table_creator-internal.h"
#include "kudu/tserver/tserver_admin.pb.h"

namespace kudu {
namespace transactions {

// Wrapper around a KuduClient used by Kudu for making transaction-related
// calls to various servers.
class TxnSystemClient {
 public:
  static Status Create(const std::vector<std::string>& master_addrs,
                       std::unique_ptr<TxnSystemClient>* sys_client);

  // Creates the transaction status table with a single range partition of the
  // given upper bound.
  Status CreateTxnStatusTable(int64_t initial_upper_bound);

  // Adds a new range to the transaction status table with the given bounds.
  Status AddTxnStatusTableRange(int64_t lower_bound, int64_t upper_bound);

  // Returns an error if there were issues communicating with the transaction
  // status table.
  Status BeginTransaction();

 private:
  TxnSystemClient(client::sp::shared_ptr<client::KuduClient> client)
      : client_(std::move(client)) {}

  client::sp::shared_ptr<client::KuduClient> client_;
};

} // namespace transactions
} // namespace kudu

