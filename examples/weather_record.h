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

#pragma once

#include <string>  // std::string
#include <vector>  // std::vector

#include "greptime/v1/database.pb.h"  // row insert request

using greptime::v1::RowInsertRequest;

// FIXME(niebayes): ensure all the field types are compatible with the ARM
// platforms.
struct WeatherRecord {
  static const std::string TABLE_NAME;
  uint64_t timestamp;
  std::string collector_id;
  float temperature;
  // FIXME(niebayes): could humidity be negative? Or this is relative humidity?
  int32_t humidity;
};

class WeatherRecordFactory {
  static const std::string COLLECTOR_ID_PREFIX;
  static uint64_t last_timestamp;

 public:
  static WeatherRecord make_one();
  static std::vector<WeatherRecord> make_many(const size_t num_records);
};

RowInsertRequest build_row_insert_request_from_records(const std::vector<WeatherRecord>& weather_records);
