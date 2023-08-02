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

#include <iostream>
#include "client.h"
#include "stream_inserter.h"

#ifndef GREPTIMEDB_CLIENT_CPP_DATABASE_H
#define GREPTIMEDB_CLIENT_CPP_DATABASE_H

namespace greptime {

using String = std::string;
using greptime::v1::InsertRequests;

class Database {
public:
    Database(String dbname_, String greptimedb_endpoint_);
    
    StreamInserter CreateStreamInserter(); 

private:
    String dbname;
    GreptimeClient client;
};

}  // namespace greptime

#endif  // GREPTIMEDB_CLIENT_CPP_DATABASE_H
