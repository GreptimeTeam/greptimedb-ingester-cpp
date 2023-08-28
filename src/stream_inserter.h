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

#include <greptime/v1/database.grpc.pb.h>
#include <grpcpp/channel.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include "greptime/v1/database.pb.h"
#include "grpcpp/client_context.h"
#include <thread>
#include <queue>
#include <condition_variable>
#include <mutex>
namespace greptime {

using String = std::string;
using greptime::v1::GreptimeDatabase;
using greptime::v1::GreptimeRequest;
using greptime::v1::GreptimeResponse;
using greptime::v1::InsertRequest;
using greptime::v1::InsertRequests;
using grpc::ClientContext;
using grpc::Status;
using grpc::Channel;

class StreamInserter {
public:
    StreamInserter(String dbname_, std::shared_ptr<Channel> channel_, std::shared_ptr<GreptimeDatabase::Stub> stub_) :
    dbname(dbname_), channel(channel_), stub(stub_){
        context = std::make_shared<ClientContext>();
        context->set_wait_for_ready(true);
        writer = std::move(stub_->HandleRequests(context.get(), &response));
        scheduler_thread_is_running = true;
        scheduler_thread = new std::thread(&StreamInserter::RunHandleRequest, this);
    }

    ~StreamInserter() {
        delete scheduler_thread;
    }

    void Write(std::vector<InsertRequest> insert_request_vec);

    bool Send(GreptimeRequest &greptime_request);

    void RunHandleRequest();

    bool WriteDone() {
        scheduler_thread_is_running = false;
        cv.notify_one();
        scheduler_thread->join();
        return writer->WritesDone();
    }

    grpc::Status Finish() { return writer->Finish(); }

    GreptimeResponse GetResponse() { return response; }

private:
    String dbname;
    GreptimeResponse response;
    std::shared_ptr<ClientContext> context;
    std::shared_ptr<Channel> channel;
    std::shared_ptr<GreptimeDatabase::Stub> stub;
    std::unique_ptr<grpc::ClientWriter<GreptimeRequest>> writer;
    
    ///background thread handle insert request
    std::thread *scheduler_thread;
    std::queue<InsertRequest> buffer; 
    std::mutex mtx;
    std::condition_variable cv;
    bool scheduler_thread_is_running;
};

};  // namespace greptime
