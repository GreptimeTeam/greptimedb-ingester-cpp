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

namespace greptime {
    
using greptime::v1::RequestHeader;

bool StreamInserter::Write(InsertRequests &insert_requests) {
    RequestHeader request_header;
    request_header.set_dbname(dbname);
    // avoid repetitive construction and destruction
    thread_local GreptimeRequest greptime_request;
    greptime_request.mutable_header()->Swap(&request_header);
    greptime_request.mutable_inserts()->Swap(&insert_requests);
    
    return writer->Write(greptime_request);
}

};