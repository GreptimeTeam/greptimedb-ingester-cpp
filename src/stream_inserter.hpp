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

#include <cassert>             // assert
#include <condition_variable>  // std::condition_variable
#include <iostream>            // std::cerr, std::endl
#include <memory>              // std::unique_ptr, std::shared_ptr
#include <mutex>               // std::mutex
#include <queue>               // std::queue
#include <string>              // std::string
#include <thread>              // std::thread
#include <vector>              // std::vector

namespace greptime {

using greptime::v1::GreptimeRequest;
using greptime::v1::GreptimeResponse;
using greptime::v1::RequestHeader;
using greptime::v1::RowInsertRequest;
using greptime::v1::RowInsertRequests;

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
  StreamInserter(const GreptimeResponse* response, const std::string& db_name, const GreptimeDatabase::Stub* stub)
      : db_name_{db_name} {
    grpc_clt_ctx_ = make_unique<grpc::ClientContext>();
    grpc_clt_ctx_->set_wait_for_ready(true);
    writer_ = std::move(stub->HandleRequests(grpc_clt_ctx_->get(), &response));

    pending_que_ = ConcurrentQueue(StreamInserter::PENDING_QUEUE_CAPACITY);
    sender_thread_ = std::thread(&StreamInserter::sender, this);
  }

  void feed_one(RowInsertRequest& row_insert_request) {
    std::unique_lock<std::mutex> lock(mutex_);

    while (pending_que_.size() >= StreamInserter::PENDING_QUEUE_CAPACITY) {
      not_full_cv_.wait(lock, [this] { return this->pending_que_.size() < StreamInserter::PENDING_QUEUE_CAPACITY; })
    }
    pending_que_.push(std::move(row_insert_request));

    not_empty_cv_.notify_one();
  }

  // try to feed a batch of requests into the pending queue.
  // if no enough space, degrade to feeding requests one by one.
  void feed_batch(std::vector<RowInsertRequest>& row_insert_request_batch) {
    std::unique_lock<std::mutex> lock(mutex_);

    const size_t delta = row_insert_request_batch.size();
    if (pending_que_.size() + delta <= StreamInserter::PENDING_QUEUE_CAPACITY) {
      for (RowInsertRequest& row_insert_request : row_insert_request_batch) {
        pending_que_.push(std::move(row_insert_request));
      }

    } else {
      lock.unlock();

      for (RowInsertRequest& row_insert_request : row_insert_request_batch) {
        this->feed_one(row_insert_request);
      }
    }
  }

  void sender() {
    for (;;) {
      std::unique_lock<std::mutex> lock(mutex_);

      while (!pending_que_.empty() && !done_) {
        not_empty_cv_.wait(lock, [this] { return !this->pending_que_.empty() || this->done_; })
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

        if (row_insert_request_batch.empty()) {
          continue;
        }
        not_full_cv_.notify_all();

        const GreptimeRequest greptime_request = util::make_greptime_request(db_name_, row_insert_request_batch);
        if (!writer_->Write(greptime_request)) {
          std::cerr << "error in sending greptime request. request_size = " << greptime_request.ByteSizeLong()
                    << std::endl;
        }

      } else if (done_) {
        break;
      }
    }
  }

  bool writes_done() {
    std::unique_lock<std::mutex> lock(mutex_);
    done_ = true;
    lock.unlock();

    sender_thread_.join();

    return writer->WritesDone();
  }

  grpc::Status finish() { return writer->Finish(); }
};

namespace util {

static GreptimeRequest make_greptime_request(const std::string& db_name, const RowInsertRequests& row_insert_request_batch) {
  RequestHeader request_header;
  request_header.set_dbname(dbname);

  GreptimeRequest greptime_request;
  greptime_request.mutable_header()->Swap(&request_header);
  greptime_request.mutable_inserts()->Swap(&row_insert_request_batch);

  return greptime_request;
}

}  // namespace util
}  // namespace greptime
