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

#include <memory>
#include <string>

#include "kudu/client/client.h"
#include "kudu/client/table_creator-internal.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/transactions/txn_status_tablet.h"
#include "kudu/transactions/txn_system_client.h"

using kudu::client::KuduClient;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace transactions {

Status TxnSystemClient::Create(const vector<string>& master_addrs,
                               unique_ptr<TxnSystemClient>* sys_client) {
  KuduClientBuilder builder;
  builder.master_server_addrs(master_addrs);
  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(builder.Build(&client));
  sys_client->reset(new TxnSystemClient(std::move(client)));
  return Status::OK();
}

Status TxnSystemClient::CreateTxnStatusTable(int64_t initial_upper_bound) {

  const auto& schema = TxnStatusTablet::GetSchema();
  const auto kudu_schema = KuduSchema::FromSchema(schema);

  // Add range partitioning to the transaction status table with an initial
  // upper bound, allowing us to add and drop ranges in the future.
  unique_ptr<KuduPartialRow> lb(new KuduPartialRow(&schema));
  unique_ptr<KuduPartialRow> ub(new KuduPartialRow(&schema));
  RETURN_NOT_OK(lb->SetInt64(TxnStatusTablet::kTxnIdColName, 0));
  RETURN_NOT_OK(ub->SetInt64(TxnStatusTablet::kTxnIdColName, initial_upper_bound));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->data_->table_type_ = TableTypePB::TXN_STATUS_TABLE;
  // TODO(awong): ensure that transaction status managers only accept requests
  // when their replicas are leader. For now, ensure this is the case by making
  // them non-replicated.
  return table_creator->schema(&kudu_schema)
      .set_range_partition_columns({ TxnStatusTablet::kTxnIdColName })
      .add_range_partition(lb.release(), ub.release())
      .table_name(TxnStatusTablet::kTxnStatusTableName)
      .num_replicas(1)
      .wait(true)
      .Create();
}

Status TxnSystemClient::AddTxnStatusTableRange(int64_t lower_bound, int64_t upper_bound) {
  const auto& schema = TxnStatusTablet::GetSchema();
  unique_ptr<KuduPartialRow> lb(new KuduPartialRow(&schema));
  unique_ptr<KuduPartialRow> ub(new KuduPartialRow(&schema));
  RETURN_NOT_OK(lb->SetInt64(TxnStatusTablet::kTxnIdColName, lower_bound));
  RETURN_NOT_OK(ub->SetInt64(TxnStatusTablet::kTxnIdColName, upper_bound));
  unique_ptr<KuduTableAlterer> alterer(
      client_->NewTableAlterer(TxnStatusTablet::kTxnStatusTableName));
  return alterer->AddRangePartition(lb.release(), ub.release())
      ->modify_external_catalogs(false)
      ->wait(true)
      ->Alter();
}

Status TxnSystemClient::BeginTransaction() {
  shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client_->OpenTable(TxnStatusTablet::kTxnStatusTableName, &table));

  // TODO(awong): consider using a single session per system client and using
  // async callbacks to return errors back.
  shared_ptr<KuduSession> session = client_->NewSession();
  return Status::OK();
}

} // namespace transactions
} // namespace kudu
