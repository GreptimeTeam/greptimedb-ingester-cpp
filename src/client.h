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

#include <string>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/client_context.h>
#include <greptime/v1/database.grpc.pb.h>

namespace greptime {

using greptime::v1::GreptimeDatabase;
using grpc::Channel;

class GreptimeClient {
   public:
    GreptimeClient(std::string greptimedb_endpoint_);

    std::shared_ptr<Channel> channel;
    std::shared_ptr<GreptimeDatabase::Stub> stub;

};

}  // namespace greptime
