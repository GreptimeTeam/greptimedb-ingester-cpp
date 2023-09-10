# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/proto
  ${CMAKE_SOURCE_DIR}/third_party/grpc/grpcpp
)

# find the grpc package with the config mode since the grpc library only provides a gRPCConfig.cmake.in file.
# see: https://cmake.org/cmake/help/latest/command/find_package.html#search-modes
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR})
find_package(gRPC CONFIG REQUIRED)

add_library(${GREPTIME_CLIENT_CPP_LIB}
  db_client.cc
  stream_inserter.cc
)

target_link_libraries(${GREPTIME_CLIENT_CPP_LIB} PRIVATE 
  ${GREPTIME_PROTO_CPP_LIB}
  gRPC
)
