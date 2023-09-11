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

#include "stream_inserter.h"

#include <cassert>   // assert
#include <iostream>  // std::cerr, std::endl
#include <memory>    // std::unique_ptr, std::make_unique
#include <mutex>     // std::mutex, std::unique_lock
#include <string>    // std::string
#include <thread>    // std::thread
#include <vector>    // std::vector

#include "greptime/v1/common.pb.h"         // request header
#include "greptime/v1/database.grpc.pb.h"  // greptime database
#include "greptime/v1/database.pb.h"       // greptime request, greptime response, row insert request/requests
#include "grpc/status.h"                   // status
#include "grpcpp/client_context.h"         // client context
#include "grpcpp/support/sync_stream.h"    // client writer

namespace greptime {

using greptime::v1::GreptimeDatabase;
using greptime::v1::GreptimeRequest;
using greptime::v1::GreptimeResponse;
using greptime::v1::RequestHeader;
using greptime::v1::RowInsertRequest;
using greptime::v1::RowInsertRequests;

// forward declaration.
namespace util {
static GreptimeRequest make_greptime_request(const std::string& db_name, RowInsertRequests* row_insert_request_batch);
}  // namespace util

StreamInserter::StreamInserter(GreptimeResponse* response, const std::string& db_name, GreptimeDatabase::Stub* stub)
    : db_name_{db_name} {
  grpc_clt_ctx_ = std::make_unique<grpc::ClientContext>();
  grpc_clt_ctx_->set_wait_for_ready(true);
  writer_ = std::move(stub->HandleRequests(grpc_clt_ctx_.get(), response));

  sender_thread_ = std::thread(&StreamInserter::sender, this);
}

void StreamInserter::feed_one(const RowInsertRequest& row_insert_request) {
  std::unique_lock<std::mutex> lock(mutex_);

  while (pending_que_.size() >= StreamInserter::PENDING_QUEUE_CAPACITY) {
    not_full_cv_.wait(lock, [this] { return this->pending_que_.size() < StreamInserter::PENDING_QUEUE_CAPACITY; });
  }

  pending_que_.push(row_insert_request);
  not_empty_cv_.notify_one();
}

// try to feed a batch of requests into the pending queue.
// if no enough space, degrade to feeding requests one by one.
void StreamInserter::feed_batch(const std::vector<RowInsertRequest>& row_insert_request_batch) {
  std::unique_lock<std::mutex> lock(mutex_);

  const size_t delta = row_insert_request_batch.size();
  if (pending_que_.size() + delta <= StreamInserter::PENDING_QUEUE_CAPACITY) {
    for (const RowInsertRequest& row_insert_request : row_insert_request_batch) {
      pending_que_.push(row_insert_request);
    }
    not_empty_cv_.notify_one();

  } else {
    lock.unlock();

    for (const RowInsertRequest& row_insert_request : row_insert_request_batch) {
      this->feed_one(row_insert_request);
    }
  }
}

void StreamInserter::sender() {
  for (;;) {
    std::unique_lock<std::mutex> lock(mutex_);

    while (!pending_que_.empty() && !done_) {
      not_empty_cv_.wait(lock, [this] { return !this->pending_que_.empty() || this->done_; });
    }

    if (!pending_que_.empty()) {
      RowInsertRequests row_insert_request_batch;
      size_t batch_size = 0;

      while (!pending_que_.empty() && batch_size + pending_que_.front().ByteSizeLong() <= MAX_BATCH_SIZE) {
        RowInsertRequest row_insert_request = pending_que_.front();
        pending_que_.pop();

        batch_size += row_insert_request.ByteSizeLong();
        row_insert_request_batch.add_inserts()->Swap(&row_insert_request);
      }

      lock.unlock();

      if (row_insert_request_batch.inserts_size() == 0) {
        continue;
      }
      not_full_cv_.notify_all();

      const GreptimeRequest greptime_request = util::make_greptime_request(db_name_, &row_insert_request_batch);
      if (!writer_->Write(greptime_request)) {
        std::cerr << "error in sending greptime request. request_size = " << greptime_request.ByteSizeLong()
                  << std::endl;
      }

    } else if (done_) {
      break;
    }
  }
}

bool StreamInserter::writes_done() {
  std::unique_lock<std::mutex> lock(mutex_);
  done_ = true;
  lock.unlock();

  sender_thread_.join();

  return writer_->WritesDone();
}

grpc::Status StreamInserter::finish() { return writer_->Finish(); }

namespace util {

GreptimeRequest make_greptime_request(const std::string& db_name, RowInsertRequests* row_insert_request_batch) {
  RequestHeader request_header;
  request_header.set_dbname(db_name);

  GreptimeRequest greptime_request;
  greptime_request.mutable_header()->Swap(&request_header);
  greptime_request.mutable_row_inserts()->Swap(row_insert_request_batch);

  return greptime_request;
}

}  // namespace util
}  // namespace greptime
