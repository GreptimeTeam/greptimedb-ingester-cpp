// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <condition_variable>  // std::condition_variable
#include <memory>              // std::unique_ptr
#include <mutex>               // std::mutex
#include <queue>               // std::queue
#include <string>              // std::string
#include <thread>              // std::thread
#include <vector>              // std::vector

#include "greptime/v1/database.grpc.pb.h"  // greptime database
#include "greptime/v1/database.pb.h"       // greptime request, greptime response, row insert request
#include "grpc/status.h"                   // status
#include "grpcpp/client_context.h"         // client context
#include "grpcpp/support/sync_stream.h"    // client writer

namespace greptime {

using greptime::v1::GreptimeDatabase;
using greptime::v1::GreptimeRequest;
using greptime::v1::GreptimeResponse;
using greptime::v1::RowInsertRequest;

class StreamInserter {
  // FIXME(niebayes): prove the reasonableness of these two magic numbers.
  static constexpr size_t PENDING_QUEUE_CAPACITY = 1000000;
  static constexpr size_t MAX_BATCH_SIZE = 2981328;

  const std::string db_name_;

  std::unique_ptr<grpc::ClientContext> grpc_clt_ctx_ = nullptr;
  std::unique_ptr<grpc::ClientWriter<GreptimeRequest>> writer_ = nullptr;

  std::queue<RowInsertRequest> pending_que_;
  std::mutex mutex_;
  std::condition_variable not_full_cv_;
  std::condition_variable not_empty_cv_;

  std::thread sender_thread_;
  bool done_ = false;

 public:
  StreamInserter(const GreptimeResponse* response, const std::string& db_name, const GreptimeDatabase::Stub* stub);

  void feed_one(RowInsertRequest& row_insert_request);
  // try to feed a batch of requests into the pending queue.
  // if no enough space, degrade to feeding requests one by one.
  void feed_batch(std::vector<RowInsertRequest>& row_insert_request_batch);
  // FIXME(niebayes): should be static?
  void sender();
  bool writes_done();
  grpc::Status finish();
};

}  // namespace greptime
