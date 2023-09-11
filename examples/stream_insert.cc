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

#include <iostream>  // std::cout, std::endl
#include <string>    // std::string
#include <vector>    // std::vector

#include "db_client.h"
#include "greptime/v1/database.pb.h"  // row insert request, greptime database
#include "grpc/status.h"              // status
#include "stream_inserter.h"
#include "weather_record.h"

using greptime::v1::GreptimeDatabase;
using greptime::v1::GreptimeResponse;
using greptime::v1::RowInsertRequest;

int main() {
  static const std::string DEFAULT_DATABASE_NAME = "public";
  static const std::string DEFAULT_GRPC_ENDPOINT = "localhost:4001";
  static const size_t NUM_EXAMPLE_RECORDS = 5;

  greptime::DbClient db_client(DEFAULT_DATABASE_NAME, DEFAULT_GRPC_ENDPOINT);

  const std::vector<WeatherRecord> weather_records = WeatherRecordFactory::make_many(NUM_EXAMPLE_RECORDS);
  const RowInsertRequest row_insert_request = build_row_insert_request_from_records(weather_records);

  GreptimeResponse response;
  greptime::StreamInserter stream_inserter = db_client.new_stream_inserter(&response);

  stream_inserter.feed_one(row_insert_request);
  stream_inserter.writes_done();

  const grpc::Status status = stream_inserter.finish();

  if (status.ok()) {
    std::cout << "Success: " << response.affected_rows().value() << " rows are inserted into the table "
              << DEFAULT_DATABASE_NAME + '.' + row_insert_request.table_name() << std::endl;
  } else {
    std::cout << "Failure: " << status.error_message() << ' ' << status.error_details() << std::endl;
  }
}
