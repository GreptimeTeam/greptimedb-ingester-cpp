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
#include <limits>
#include <memory>
#include <string>
#include <valarray>
#include <vector>
#include <cmath>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/stubs/common.h>
#include <greptime/v1/column.pb.h>
#include <greptime/v1/common.pb.h>
#include <greptime/v1/database.grpc.pb.h>
#include <greptime/v1/database.pb.h>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>

#include <grpcpp/client_context.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/support/async_stream.h>

#include <grpcpp/channel.h>
#include <grpcpp/support/status.h>
#include <src/database.h>

using grpc::Status;

struct WeatherRecord {
    uint64_t timestamp_millis;
    float_t temperature;
    float_t humidity;
};

auto weather_records_1() -> std::vector<WeatherRecord> {
    return std::vector<WeatherRecord>{{1686109527000, 26.4, 15.0}, {1686023127001, 29.3, 20.0},
                                      {1685936727002, 31.8, 13.0}, {1686109527003, 20.4, 67.0},
                                      {1686109527004, 18.4, 74.0}, {1685936727005, 19.2, 81.0}};
}

int main(int argc, char** argv) {
    
    greptime::Database database("public", "localhost:4001");

    auto stream_inserter = database.CreateStreamInserter<float_t>();

    std::string table_name("weather_demo1"); 


    auto weather_demo = weather_records_1();

    for (auto &[ts, v1, v2] : weather_demo) {
        greptime::InsertEntry<float_t> insert_entry(ts);

        insert_entry.add_point(table_name, "temperature", v1);
        insert_entry.add_point(table_name, "humidity", v2);
        stream_inserter.Write(insert_entry);
    }

    stream_inserter.WriteDone();
    Status status = stream_inserter.Finish();

    if (status.ok()) {
        std::cout << "success!" << std::endl;
        auto response = stream_inserter.GetResponse();

        std::cout << "notice: [";
        std::cout << response.affected_rows().value() << "] ";
        std::cout << "rows of data are successfully inserted into the public database table " << table_name << std::endl;
    } else {
        std::cout << "fail!" << std::endl;
        std::cout << status.error_message() << std::endl;
        std::cout << status.error_details() << std::endl;
    }

    return 0;
}
