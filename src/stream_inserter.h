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

#include <greptime/v1/database.grpc.pb.h>
#include <grpcpp/channel.h>
#include <cmath>
#include <cstdint>
#include <memory>
#include <vector>
#include <grpcpp/client_context.h>
#include <memory>


namespace greptime {

using String = std::string;
using greptime::v1::GreptimeDatabase;
using greptime::v1::GreptimeRequest;
using greptime::v1::GreptimeResponse;
using greptime::v1::InsertRequest;
using greptime::v1::InsertRequests;
using greptime::v1::RequestHeader;

using grpc::ClientContext;
using grpc::Status;
using grpc::Channel;


using TableName = std::string;
using Timestamp = int64_t;
using FieldName = std::string;

template<typename FieldVal = float_t>
struct InsertEntry {
    Timestamp ts;
    std::unordered_map<TableName, std::unordered_map<FieldName, FieldVal>> tables;

    InsertEntry():ts(-1) {
        for(auto&[_, fields] : tables) {
            fields.clear();
        }
        tables.clear();
    }

    InsertEntry(Timestamp ts_):ts(ts_) {
        for(auto&[_, fields] : tables) {
            fields.clear();
        }
        tables.clear();
    }

    void add_point(TableName table_name, FieldName field_name, FieldVal field_val) {
        tables[table_name].emplace(field_name, field_val);
    }

};

/// 攒批
template<typename T>
using InsertBatch = std::vector<InsertEntry<T>>;

using greptime::v1::Column;
using greptime::v1::Column_SemanticType;
using greptime::v1::ColumnDataType;
using greptime::v1::InsertRequest;
using greptime::v1::InsertRequests;

template<typename FieldVal = float_t>
struct LineWriter {
    // 每个 Table 是一个 InsertRequest
    std::unordered_map<TableName, 
                       std::tuple<std::unordered_map<FieldName, std::vector<FieldVal>>,
                                  std::vector<Timestamp>, 
                                  uint32_t>
                                  > mp;
    LineWriter() = default;

    void add_row(TableName table_name, Timestamp ts, const std::unordered_map<FieldName, FieldVal> &fields) {
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

    auto build() -> std::vector<InsertRequest> {
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
    
};

auto to_insert_requests(std::vector<InsertRequest>& vec_insert_requests) -> InsertRequests;



const uint32_t BATCH_NUMBER = 1;
template<typename FieldVal = float_t>
class StreamInserter {
public:
    StreamInserter(String dbname_, std::shared_ptr<Channel> channel_, std::shared_ptr<GreptimeDatabase::Stub> stub_) :
    dbname(dbname_), channel(channel_), stub(stub_) {
        context = std::make_shared<ClientContext>();
        context->set_wait_for_ready(true);
        writer = std::move(stub_->HandleRequests(context.get(), &response));
        cache.reserve(BATCH_NUMBER);
    }

    bool Write(InsertEntry<FieldVal> &insert_entry) {
        cache.emplace_back(std::move(insert_entry));
        if (cache.size() >= BATCH_NUMBER) {
            LineWriter line_writer;

            for (auto &insert_entry : cache) {
                auto ts = insert_entry.ts;
                for (auto &[table_name, fields] : insert_entry.tables) {
                    line_writer.add_row(table_name, ts, fields);
                }
            }
            cache.clear();
            auto insert_vec = line_writer.build();
            auto insert_requests = to_insert_requests(insert_vec);
            return WriteInner(insert_requests);
        }
        return true;
    }

    bool WriteInner(InsertRequests &insert_requests) {
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


    bool WriteDone() { 
        if (!cache.empty()) {
            LineWriter line_writer;

            for (auto &insert_entry : cache) {
                auto ts = insert_entry.ts;
                for (auto &[table_name, fields] : insert_entry.tables) {
                    line_writer.add_row(table_name, ts, fields);
                }
            }
            cache.clear();
            auto insert_vec = line_writer.build();
            auto insert_requests = to_insert_requests(insert_vec);
            WriteInner(insert_requests);
        }
        return writer->WritesDone();
    }

    grpc::Status Finish() { return writer->Finish(); }

    GreptimeResponse GetResponse() { return response; }

private:
    String dbname;
    GreptimeResponse response;
    std::shared_ptr<ClientContext> context;
    std::shared_ptr<Channel> channel;
    std::shared_ptr<GreptimeDatabase::Stub> stub;
    std::unique_ptr<grpc::ClientWriter<GreptimeRequest>> writer;
    InsertBatch<FieldVal> cache = {};
};

};  // namespace greptime
