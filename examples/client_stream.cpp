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

#include <google/protobuf/extension_set.h>
#include <google/protobuf/stubs/common.h>
#include <greptime/v1/column.pb.h>
#include <greptime/v1/common.pb.h>
#include <greptime/v1/database.grpc.pb.h>
#include <greptime/v1/database.pb.h>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/status.h>
#include <src/database.h>

#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <valarray>
#include <vector>

using greptime::v1::Column;
using greptime::v1::Column_SemanticType;
using greptime::v1::ColumnDataType;
using greptime::v1::InsertRequest;
using greptime::v1::InsertRequests;
using grpc::Channel;
using grpc::Status;

struct WeatherRecord {
  uint64_t timestamp_millis;
  std::string collector;
  float temperature;
  int32_t humidity;
};

auto weather_records_1() -> std::vector<WeatherRecord> {
  return std::vector<WeatherRecord>{
      {1686109527000, "c1", 26.4, 15}, {1686023127001, "c1", 29.3, 20},
      {1685936727002, "c1", 31.8, 13}, {1686109527000, "c2", 20.4, 67},
      {1686109527001, "c2", 18.4, 74}, {1685936727002, "c2", 19.2, 81}};
}

auto weather_records_2() -> std::vector<WeatherRecord> {
  return std::vector<WeatherRecord>{
      {1686109527003, "c1", 26.4, 15}, {1686023127004, "c1", 29.3, 20},
      {1685936727005, "c1", 31.8, 13}, {1686109527003, "c2", 20.4, 67},
      {1686109527004, "c2", 18.4, 74}, {1685936727005, "c2", 19.2, 81}};
}

auto to_insert_request(std::vector<WeatherRecord> records) -> InsertRequest {
  InsertRequest insert_request;
  uint32_t rows = records.size();
  insert_request.set_table_name("weather_demo");
  insert_request.set_row_count(rows);
  {
    Column column;
    column.set_column_name("ts");
    column.set_semantic_type(
        Column_SemanticType::Column_SemanticType_TIMESTAMP);
    column.set_datatype(ColumnDataType::TIMESTAMP_MILLISECOND);

    auto values = column.mutable_values();
    for (const auto& record : records) {
      values->add_ts_millisecond_values(record.timestamp_millis);
    }
    insert_request.add_columns()->Swap(&column);
  }

  {
    Column column;
    column.set_column_name("collector");
    column.set_semantic_type(Column_SemanticType::Column_SemanticType_TAG);
    column.set_datatype(ColumnDataType::STRING);

    auto values = column.mutable_values();
    for (const auto& record : records) {
      values->add_string_values(record.collector);
    }
    insert_request.add_columns()->Swap(&column);
  }

  {
    Column column;
    column.set_column_name("temperature");
    column.set_semantic_type(Column_SemanticType::Column_SemanticType_FIELD);
    column.set_datatype(ColumnDataType::FLOAT32);

    auto values = column.mutable_values();
    for (const auto& record : records) {
      values->add_f32_values(record.temperature);
    }
    insert_request.add_columns()->Swap(&column);
  }

  {
    Column column;
    column.set_column_name("humidity");
    column.set_semantic_type(Column_SemanticType::Column_SemanticType_FIELD);
    column.set_datatype(ColumnDataType::INT32);

    auto values = column.mutable_values();
    for (const auto& record : records) {
      values->add_i32_values(record.humidity);
    }
    insert_request.add_columns()->Swap(&column);
  }

  return insert_request;
}

auto to_insert_requests(const std::vector<InsertRequest>& vec_insert_requests)
    -> InsertRequests {
  InsertRequests insert_requests;
  for (auto insert_request : vec_insert_requests) {
    insert_requests.add_inserts()->CopyFrom(insert_request);
  }
  return insert_requests;
}

int main(int argc, char** argv) {
  /** =========================== 1.Create a Database object and connect to the
   * gRPC service =========================== **/

  greptime::Database database("public", "localhost:4001");

  /** =========================== 2.generate insert requests
   * =========================== **/
  auto insert_request_1 = to_insert_request(weather_records_1());
  auto insert_request_2 = to_insert_request(weather_records_2());
  auto insert_request_vec_1 = std::vector<InsertRequest>{insert_request_1};
  auto insert_request_vec_2 = std::vector<InsertRequest>{insert_request_2};

  auto table_name = insert_request_1.table_name();

  /** =========================== 3.continue insert requests
   * =========================== **/
  database.stream_inserter.WriteBatch(insert_request_vec_1);
  // stream_inserter.Write(insert_request_vec_2);
  database.stream_inserter.WriteDone();
  Status status = database.stream_inserter.Finish();

  /** =========================== 4.handle return response
   * =========================== **/
  if (status.ok()) {
    std::cout << "success!" << std::endl;
    auto response = database.stream_inserter.GetResponse();

    std::cout << "notice: [";
    std::cout << response.affected_rows().value() << "] ";
    std::cout << "rows of data are successfully inserted into the public "
                 "database table "
              << table_name << std::endl;
  } else {
    std::cout << "fail!" << std::endl;
    std::cout << status.error_message() << std::endl;
    std::cout << status.error_details() << std::endl;
  }

  return 0;
}
