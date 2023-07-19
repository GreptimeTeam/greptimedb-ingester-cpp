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

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <valarray>
#include <vector>

#include <google/protobuf/extension_set.h>
#include <google/protobuf/stubs/common.h>
#include <grpcpp/grpcpp.h>
#include <greptime/v1/column.pb.h>
#include <greptime/v1/common.pb.h>
#include <greptime/v1/database.pb.h>
#include <greptime/v1/database.grpc.pb.h>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>

#include <grpcpp/client_context.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/support/async_stream.h>

#include <grpcpp/channel.h>
#include <grpcpp/support/status.h>

namespace greptime {

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using greptime::v1::Column;
using greptime::v1::GreptimeDatabase;
using greptime::v1::GreptimeRequest;
using greptime::v1::GreptimeResponse;
using greptime::v1::InsertRequest;
using greptime::v1::InsertRequests;
using greptime::v1::Column_SemanticType;
using greptime::v1::ColumnDataType;
using greptime::v1::RequestHeader;
using String = std::string;


class GreptimeStreamClient {
public:
    GreptimeStreamClient(std::shared_ptr<Channel> channel);

    bool Write(GreptimeRequest greptime_request);
    
    bool WritesDone();

    grpc::Status Finish();

    GreptimeResponse GetResponse() {
        return response;
    }


private:
    std::unique_ptr<GreptimeDatabase::Stub> stub_;
    GreptimeResponse response;
    ClientContext context;
    std::unique_ptr<grpc::ClientWriter<GreptimeRequest>> writer;
};

};
