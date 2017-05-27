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

#include <map>
#include <string>

#include "kudu/fs/data_dirs.h"
#include "kudu/gutil/callback_forward.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/fault_injection.h"

namespace kudu {
namespace fs {

// Callback to error-handling code.
typedef Callback<void(const std::string&)> ErrorNotificationCb;
static void DoNothingErrorNotification(const std::string& /* uuid */) {}

// When certain operations fail, the side effects of the error can span
// multiple layers, many of which we prefer to keep separate. The FsErrorManager
// registers and runs error handlers without adding cross-layer dependencies.
//
// e.g. the TSTabletManager registers a callback to handle disk failure by
// shutting down the tablets on that disk. Blocks and other entities that may
// hit disk failures can call it without knowing about the TSTabletManager.
class FsErrorManager {
 public:
  FsErrorManager()
    : notify_cb_(Bind(DoNothingErrorNotification)) {}

  void SetErrorNotificationCb(ErrorNotificationCb cb) {
    notify_cb_ = std::move(cb);
  }

  void UnsetErrorNotificationCb() {
    notify_cb_ = Bind(DoNothingErrorNotification);
  }

  void RunErrorNotificationCb(const std::string& uuid) const {
    notify_cb_.Run(uuid);
  }

 private:

   // Callback to be run when an error occurs.
   ErrorNotificationCb notify_cb_;
};

}  // namespace fs
}  // namespace kudu
