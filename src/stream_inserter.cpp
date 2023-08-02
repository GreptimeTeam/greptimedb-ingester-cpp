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

uint32_t InsertEntry::get_point_num() {
    uint32_t ret = 0;
    for (const auto & table : tables) 
        ret += table.second.size();
    return ret;
}

void LineWriter::add_row(TableName table_name, Timestamp ts, const std::unordered_map<FieldName, FieldVal> &fields) {
    if (mp.find(table_name) == mp.end()) {
        mp.emplace(table_name, std::make_tuple(std::unordered_map<FieldName, std::vector<FieldVal>>(),
                                                    std::vector<Timestamp>(),
                                                    0));
    }
    auto &[field_map, ts_vec, cur_row] = mp[table_name];
    // 如果前面 insert 的没有对应对 field
    // 那么说明该 field 前面都是 null
    for(const auto &[name, val] : fields) {
        auto it = field_map.find(name);
        if (it == field_map.end()) {
            // 使用emplace_hint插入元素
            it = field_map.emplace_hint(field_map.end(), name, std::vector<FieldVal>(cur_row, 0.0));
        }
        it->second.emplace_back(val);
    }
    cur_row += 1;
    ts_vec.emplace_back(ts);

    for (auto& field : field_map) {
        auto& val_vec = field.second;
        if (val_vec.size() < cur_row) {
            val_vec.resize(cur_row, 0.0);
    }
    }
}

auto LineWriter::to_insert_request_vec() -> std::vector<InsertRequest> {
    std::vector<InsertRequest> insert_vec;
    insert_vec.reserve(mp.size());
    Column column;
    for (auto &[table_name, t3] : mp) {
        InsertRequest insert_request;
        auto &[field_map, ts_vec, row_count] = t3;
        insert_request.set_table_name(table_name);
        insert_request.set_row_count(row_count);
        // timestamp
        {
            column.Clear();
            column.set_column_name("ts");
            column.set_semantic_type(Column_SemanticType::Column_SemanticType_TIMESTAMP);
            column.set_datatype(ColumnDataType::TIMESTAMP_MILLISECOND);
            auto values = column.mutable_values();
            for (const auto& ts : ts_vec) {
                values->add_ts_millisecond_values(ts);
            }
            insert_request.add_columns()->Swap(&column);
        }
        // field
        for (const auto&[field_name, field_vals] : field_map) {
            column.Clear();
            column.set_column_name(field_name);
            column.set_semantic_type(Column_SemanticType::Column_SemanticType_FIELD);
            column.set_datatype(ColumnDataType::FLOAT32);
            auto values = column.mutable_values();
            for (const auto& field_val : field_vals) {
                values->add_f32_values(field_val);
            }
            insert_request.add_columns()->Swap(&column);
        }
        insert_vec.emplace_back(std::move(insert_request));
    }
    return insert_vec;
}

bool StreamInserter::Write(InsertEntry &insert_entry) {
    cache.emplace_back(std::move(insert_entry));
    if (cache.size() >= batch_num) {
        LineWriter line_writer;

        for (auto &insert_entry : cache) {
            auto ts = insert_entry.ts;
            for (auto &[table_name, fields] : insert_entry.tables) {
                line_writer.add_row(table_name, ts, fields);
            }
        }
        cache.clear();

        auto insert_vec = line_writer.to_insert_request_vec();
        auto insert_requests = to_insert_requests(insert_vec);
        return WriteInner(insert_requests);
    }
    return true;
}

bool StreamInserter::WriteInner(InsertRequests &insert_requests) {
    RequestHeader request_header;
    request_header.set_dbname(dbname);
    // avoid repetitive construction and destruction
    thread_local GreptimeRequest greptime_request;
    greptime_request.mutable_header()->Swap(&request_header);
    greptime_request.mutable_inserts()->Swap(&insert_requests);

    while(true) {
        if (writer->Write(greptime_request)) {
            return true; // Write successful
        } else {
            if (channel->GetState(true) == GRPC_CHANNEL_TRANSIENT_FAILURE) {
                context = std::make_shared<grpc::ClientContext>();
                context->set_wait_for_ready(true);
                writer = stub->HandleRequests(context.get(), &response);
            } else {
                return false; // Write failed
            }
        }
    }
}


bool StreamInserter::WriteDone() { 
    if (!cache.empty()) {
        LineWriter line_writer;

        for (auto &insert_entry : cache) {
            auto ts = insert_entry.ts;
            for (auto &[table_name, fields] : insert_entry.tables) {
                line_writer.add_row(table_name, ts, fields);
            }
        }
        cache.clear();
        auto insert_vec = line_writer.to_insert_request_vec();
        auto insert_requests = to_insert_requests(insert_vec);
        WriteInner(insert_requests);
    }
    return writer->WritesDone();
}


};
