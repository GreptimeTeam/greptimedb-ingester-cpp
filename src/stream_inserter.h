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

namespace greptime {

using String = std::string;
using greptime::v1::GreptimeDatabase;
using greptime::v1::GreptimeRequest;
using greptime::v1::GreptimeResponse;
using greptime::v1::InsertRequest;
using greptime::v1::InsertRequests;
using grpc::ClientContext;
using grpc::Status;

class StreamInserter {
public:
    StreamInserter(String dbname_, std::shared_ptr<GreptimeDatabase::Stub> stub_) : dbname(dbname_) {
        writer = std::move(stub_->HandleRequests(&context, &response));
    }
    bool Write(InsertRequests &insert_requests);

    bool WriteDone() { return writer->WritesDone();}

    grpc::Status Finish() { return writer->Finish(); }

    GreptimeResponse GetResponse() { return response; }

private:
    String dbname;
    GreptimeResponse response;
    ClientContext context;
    std::unique_ptr<grpc::ClientWriter<GreptimeRequest>> writer;
};

}  // namespace greptime
