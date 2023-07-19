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

#include "database.h"
#include <greptime/v1/column.pb.h>
#include <grpcpp/client_context.h>

namespace greptime {

Database::Database(String dbname_, std::shared_ptr<Channel> channel_)
        : dbname(std::move(dbname_)),
          client(channel_) {

} 

bool Database::Insert(InsertRequests insert_requests) {
    RequestHeader request_header;
    request_header.set_dbname(dbname);
    GreptimeRequest greptime_request;
    greptime_request.mutable_header()->CopyFrom(request_header);
    greptime_request.mutable_inserts()->CopyFrom(insert_requests);

    return client.Write(greptime_request);
}

};
