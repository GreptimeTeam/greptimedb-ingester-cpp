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

void StreamInserter::Write(InsertRequest insert_request) {
    std::unique_lock<std::mutex> lk(mtx);
    cv.wait(lk, [this]{
        return this->buffer.size() + 1 < BUFFER_SIZE;
    });
    buffer.push(std::move(insert_request));
    cv.notify_one();
}

void StreamInserter::WriteBatch(std::vector<InsertRequest> insert_request_vec) {
    size_t cnt = insert_request_vec.size();
    if (cnt > BUFFER_SIZE) {
        std::cout << "fail: data too many" << std::endl;
        return;
    }
    std::unique_lock<std::mutex> lk(mtx);
    cv.wait(lk, [this, cnt]{
        return this->buffer.size() + cnt < BUFFER_SIZE;
    }); 
    for (auto &insert_request : insert_request_vec) {
        buffer.push(std::move(insert_request));
    }
    cv.notify_one();
}

bool StreamInserter::Send(const GreptimeRequest &greptime_request) {
    return writer->Write(greptime_request);
}

void StreamInserter::RunHandleRequest() {
    while (true) {
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [this]{
            return !this->buffer.empty() || !this->scheduler_thread_is_running;
        });
        if (buffer.empty() && !scheduler_thread_is_running) {
            break;
        }

        if (!buffer.empty()) {
            size_t batch_byte = 0;
            InsertRequests insert_requests;
            while (!buffer.empty() && batch_byte < BATCH_BYTE_LIMIT) {
                if (batch_byte + buffer.front().ByteSizeLong() > BATCH_BYTE_LIMIT) {
                    break;
                } 
                auto insert_request = std::move(buffer.front());
                buffer.pop();
                batch_byte += insert_request.ByteSizeLong();
                insert_requests.add_inserts()->Swap(&insert_request);
            }
            lk.unlock();
            RequestHeader request_header;
            request_header.set_dbname(dbname);

            GreptimeRequest greptime_request;
            greptime_request.mutable_header()->Swap(&request_header);
            greptime_request.mutable_inserts()->Swap(&insert_requests); 
            
            if (!Send(greptime_request)) {
                std::cout << "Greptime Request to large: " << greptime_request.ByteSizeLong() << std::endl;
            }
        }
    }
}

}  // namespace greptime
