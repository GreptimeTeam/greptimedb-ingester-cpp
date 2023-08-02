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
using FieldVal = float_t;

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

    void set_ts(Timestamp ts_) {
        ts = ts_;
    }

    uint32_t get_point_num();

};

/// 攒批
using InsertBatch = std::vector<InsertEntry>;

using greptime::v1::Column;
using greptime::v1::Column_SemanticType;
using greptime::v1::ColumnDataType;
using greptime::v1::InsertRequest;
using greptime::v1::InsertRequests;

struct LineWriter {
    // 每个 Table 是一个 InsertRequest
    std::unordered_map<TableName, 
                       std::tuple<std::unordered_map<FieldName, std::vector<FieldVal>>,
                                  std::vector<Timestamp>, 
                                  uint32_t>
                                  > mp;
    LineWriter() = default;

    void add_row(TableName table_name, Timestamp ts, const std::unordered_map<FieldName, FieldVal> &fields);

    auto to_insert_request_vec() -> std::vector<InsertRequest>;
    
};

class StreamInserter {
public:
    StreamInserter(String dbname_, std::shared_ptr<Channel> channel_, std::shared_ptr<GreptimeDatabase::Stub> stub_, uint32_t batch_num_) :
    dbname(dbname_), channel(channel_), stub(stub_), batch_num(batch_num_) {
        context = std::make_shared<ClientContext>();
        context->set_wait_for_ready(true);
        writer = std::move(stub_->HandleRequests(context.get(), &response));
        cache.reserve(batch_num);
    }

    bool Write(InsertEntry &insert_entry);

    bool WriteInner(InsertRequests &insert_requests);

    bool WriteDone();

    grpc::Status Finish() { return writer->Finish(); }

    GreptimeResponse GetResponse() { return response; }

private:
    String dbname;
    GreptimeResponse response;
    std::shared_ptr<ClientContext> context;
    std::shared_ptr<Channel> channel;
    std::shared_ptr<GreptimeDatabase::Stub> stub;
    std::unique_ptr<grpc::ClientWriter<GreptimeRequest>> writer;
    InsertBatch cache = {};
    uint32_t batch_num;
};

};  // namespace greptime
