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

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <valarray>
#include <vector>

#include <google/protobuf/extension_set.h>
#include <google/protobuf/stubs/common.h>
#include <grpcpp/grpcpp.h>
#include <greptime/v1/column.pb.h>
#include <greptime/v1/common.pb.h>
#include <greptime/v1/database.pb.h>
#include <greptime/v1/database.grpc.pb.h>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>

#include <grpcpp/client_context.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/support/async_stream.h>

#include <grpcpp/channel.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using greptime::v1::Column;
using greptime::v1::GreptimeDatabase;
using greptime::v1::GreptimeRequest;
using greptime::v1::GreptimeResponse;
using greptime::v1::InsertRequest;
using greptime::v1::InsertRequests;
using greptime::v1::Column_SemanticType;
using greptime::v1::ColumnDataType;
using greptime::v1::RequestHeader;


struct WeatherRecord {
    uint64_t timestamp_millis;
    std::string collector;
    float temperature;
    int32_t humidity;
};

auto weather_records_1() -> std::vector<WeatherRecord> {
    return std::vector<WeatherRecord>{
        {1686109527000, "c1", 26.4, 15},
        {1686023127001, "c1", 29.3, 20},
        {1685936727002, "c1", 31.8, 13},
        {1686109527003, "c2", 20.4, 67},
        {1686109527004, "c2", 18.4, 74},
        {1685936727005, "c2", 19.2, 81}
    };
}

auto to_insert_request(std::vector<WeatherRecord> records) -> InsertRequest {

    InsertRequest insert_request;
    uint32_t rows = records.size();
    insert_request.set_table_name("weather_demo");
    insert_request.set_row_count(rows);
    {
        Column column;
        column.set_column_name("ts");
        column.set_semantic_type(Column_SemanticType::Column_SemanticType_TIMESTAMP);
        column.set_datatype(ColumnDataType::TIMESTAMP_MILLISECOND);

        auto values = column.mutable_values();
        for (const auto &record:records) {
            values->add_ts_millisecond_values(record.timestamp_millis);
        }
        std::cout << "row nums: " << column.values().time_millisecond_values_size() << std::endl;
        insert_request.add_columns()->CopyFrom(column);
        std::cout << "insert_request column size: " << insert_request.columns_size() << std::endl;
    }

   {
        Column column;
        column.set_column_name("collector");
        column.set_semantic_type(Column_SemanticType::Column_SemanticType_TAG);
        column.set_datatype(ColumnDataType::STRING);

        auto values = column.mutable_values();
        for (const auto &record:records) {
            values->add_string_values(record.collector);
        }
        std::cout << "row nums: " << column.values().string_values_size() << std::endl;
        insert_request.add_columns()->CopyFrom(column);
        std::cout << "insert_request column size: " << insert_request.columns_size() << std::endl;
    } 

    {
        Column column;
        column.set_column_name("temperature");
        column.set_semantic_type(Column_SemanticType::Column_SemanticType_FIELD);
        column.set_datatype(ColumnDataType::FLOAT32);

        auto values = column.mutable_values();
        for (const auto &record:records) {
            values->add_f32_values(record.temperature);
        }
        std::cout << "row nums: " << column.values().f32_values_size() << std::endl;
        insert_request.add_columns()->CopyFrom(column);
        std::cout << "insert_request column size: " << insert_request.columns_size() << std::endl;
    }

    {
        Column column;
        column.set_column_name("humidity");
        column.set_semantic_type(Column_SemanticType::Column_SemanticType_FIELD);
        column.set_datatype(ColumnDataType::INT32);

        auto values = column.mutable_values();
        for (const auto &record:records) {
            values->add_i32_values(record.humidity);
        }
        std::cout << "row nums: " << column.values().i32_values_size() << std::endl;
        insert_request.add_columns()->CopyFrom(column);
        std::cout << "insert_request column size: " << insert_request.columns_size() << std::endl;
    }
    std::cout << "total row nums: " << insert_request.row_count() << std::endl; 

    for (const auto column:insert_request.columns()) {
        std::cout << column.column_name() << std::endl;
    }

    return insert_request;
}


class GreptimeClient {
public:
    GreptimeClient(std::shared_ptr<Channel> channel)
        : stub_(GreptimeDatabase::NewStub(channel)) {}
    

    void Insert(std::vector<WeatherRecord> records) {

        auto insert_request_1 = to_insert_request(records);
        // auto insert_request_2 = to_insert_request(records);
        // auto insert_request_3 = to_insert_request(records);
        
        InsertRequests insert_requests;
        insert_requests.add_inserts()->CopyFrom(insert_request_1);
        // insert_requests.add_inserts()->CopyFrom(insert_request_2);
        // insert_requests.add_inserts()->CopyFrom(insert_request_3);
        
        RequestHeader request_header;
        request_header.set_dbname("public");

        GreptimeRequest greptime_request;
        // coredump
        // greptime_request.set_allocated_header(&request_header);
        // greptime_request.set_allocated_inserts(&insert_requests);
        greptime_request.mutable_header()->CopyFrom(request_header);
        greptime_request.mutable_inserts()->CopyFrom(insert_requests);

        // Send the request to the server.
        GreptimeResponse response;
        ClientContext context;
        Status status = stub_->Handle(&context, greptime_request, &response);

        if (status.ok()) {
            std::cout << "success!: ";
            std::cout << response.affected_rows().value() << std::endl;
        }
        else {
            std::cout << "fail!" << std::endl;
            std::cout << status.error_message() << std::endl;
            std::cout << status.error_details() << std::endl;
        }

    }


private:
    std::unique_ptr<GreptimeDatabase::Stub> stub_;
};

int main(int argc, char** argv) {
  // Create a GreptimeClient object and connect to the gRPC service.
  GreptimeClient client(grpc::CreateChannel("localhost:4001", grpc::InsecureChannelCredentials()));

  // Send an insert request.
  auto records = weather_records_1();
  client.Insert(records);

  return 0;
}

