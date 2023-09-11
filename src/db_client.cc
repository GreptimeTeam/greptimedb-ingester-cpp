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

#include "db_client.h"

#include <cassert>  // assert
#include <memory>   // std::shared_ptr
#include <string>   // std::string

#include "greptime/v1/database.grpc.pb.h"  // greptime database
#include "greptime/v1/database.pb.h"       // greptime response
#include "grpcpp/create_channel.h"         // create channel
#include "grpcpp/security/credentials.h"   // insecure channel credentials
#include "stream_inserter.h"

namespace greptime {

using greptime::v1::GreptimeDatabase;
using greptime::v1::GreptimeResponse;

DbClient::DbClient(const std::string& db_name, const std::string& db_grpc_endpoint)
    : db_name_{db_name},
      channel_{grpc::CreateChannel(db_grpc_endpoint, grpc::InsecureChannelCredentials())},
      stub_{GreptimeDatabase::NewStub(channel_)} {}

StreamInserter DbClient::new_stream_inserter(GreptimeResponse* response) {
  assert(response != nullptr && "invalid arg: response = nullptr");
  return StreamInserter(response, db_name_, this->stub_.get());
}

}  // namespace greptime
