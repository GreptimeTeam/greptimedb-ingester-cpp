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
#include <memory>
#include <grpcpp/client_context.h>

namespace greptime {
    
auto to_insert_requests(std::vector<InsertRequest>& vec_insert_requests) -> InsertRequests {
    InsertRequests insert_requests;
    for (auto &insert_request : vec_insert_requests) {
        insert_requests.add_inserts()->Swap(&insert_request);
    }
    return std::move(insert_requests);
}


};
