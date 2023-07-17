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


#ifndef GREPTIMEDB_CLIENT_CPP_DATABASE_H
#define GREPTIMEDB_CLIENT_CPP_DATABASE_H

#include <iostream>
#include <memory>
#include <greptime/v1/column.pb.h>
#include <greptime/v1/common.pb.h>
#include <greptime/v1/database.pb.h>
#include <greptime/v1/database.grpc.pb.h>

namespace greptime {

using String = std::string;
using v1::AuthHeader;

class Database {
public:
    void hello() {
        std::cout << "Hello Database" << std::endl;
        auto InsertRequests  = new greptime::v1::InsertRequest();
        // auto x = new v1::GreptimeResponse();

    }
    // Database(String dbname_, Client client_):dbname(dbname_), client(std::make_unique<Client>(std::move(client_))){};
private:
    String dbname;
    // ClientPtr client;
    // AuthHeader auth_header = {};

};
}


#endif //GREPTIMEDB_CLIENT_CPP_DATABASE_H
