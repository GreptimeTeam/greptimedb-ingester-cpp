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

#include "weather_record.h"

#include <cassert>  // assert
#include <random>   // std::default_random_engine, std::uniform_int_distribution, std::uniform_real_distribution
#include <string>   // std::string
#include <vector>   // std::vector

#include "greptime/v1/common.pb.h"    // column data type, semantic type
#include "greptime/v1/database.pb.h"  // row insert request,
#include "greptime/v1/row.pb.h"       // rows, row, value, column schema,

using greptime::v1::ColumnDataType;
using greptime::v1::ColumnSchema;
using greptime::v1::Row;
using greptime::v1::RowInsertRequest;
using greptime::v1::Rows;
using greptime::v1::SemanticType;
using greptime::v1::Value;

WeatherRecord WeatherRecordFactory::make_one() {
  static std::default_random_engine rng;
  // FIXME(niebayes): replace magic numbers with meaningful macros or consts.
  static std::uniform_int_distribution<uint64_t> timestamp_delta_distribution(100, 500);
  static std::uniform_int_distribution<int8_t> collector_id_suffix_distribution(1, 9);
  static std::uniform_real_distribution<float> temperature_distribution(0.0, 50.0);
  static std::uniform_int_distribution<int32_t> humidity_distribution(0, 100);

  const WeatherRecord record = WeatherRecord{
      .timestamp = last_timestamp + timestamp_delta_distribution(rng),
      .collector_id = COLLECTOR_ID_PREFIX + std::to_string(collector_id_suffix_distribution(rng)),
      .temperature = temperature_distribution(rng),
      .humidity = humidity_distribution(rng),
  };

  last_timestamp = record.timestamp;

  return record;
}

std::vector<WeatherRecord> WeatherRecordFactory::make_many(const size_t num_records) {
  assert(num_records > 0 && "invalid arg: num_records = 0");

  std::vector<WeatherRecord> records(num_records);
  for (WeatherRecord& record : records) {
    record = WeatherRecordFactory::make_one();
  }
  return records;
}

static void add_column_schema(Rows* rows, const std::string& column_name, const ColumnDataType& data_type,
                              const SemanticType& semantic_type) {
  ColumnSchema column_schema;
  column_schema.set_column_name(column_name);
  column_schema.set_datatype(data_type);
  column_schema.set_semantic_type(semantic_type);

  rows->add_schema()->Swap(&column_schema);
}

RowInsertRequest build_row_insert_request_from_records(const std::vector<WeatherRecord>& weather_records) {
  Rows rows;

  add_column_schema(&rows, "timestamp", ColumnDataType::TIMESTAMP_MILLISECOND, SemanticType::TIMESTAMP);
  add_column_schema(&rows, "collector_id", ColumnDataType::STRING, SemanticType::TAG);
  add_column_schema(&rows, "temperature", ColumnDataType::FLOAT32, SemanticType::FIELD);
  add_column_schema(&rows, "humidity", ColumnDataType::INT32, SemanticType::FIELD);

  for (const WeatherRecord& weather_record : weather_records) {
    Row row;

#define ADD_VALUE(setter, field_name)        \
  {                                          \
    Value value;                             \
    value.setter(weather_record.field_name); \
    row.add_values()->Swap(&value);          \
  }

    ADD_VALUE(set_ts_millisecond_value, timestamp);
    ADD_VALUE(set_string_value, collector_id);
    ADD_VALUE(set_f32_value, temperature);
    ADD_VALUE(set_i32_value, humidity);

    rows.add_rows()->Swap(&row);
  }

  RowInsertRequest row_insert_request;
  row_insert_request.set_table_name(WeatherRecord::TABLE_NAME);
  row_insert_request.mutable_rows()->Swap(&rows);
  // FIXME(niebayes): how to set region id?

  return row_insert_request;
}
