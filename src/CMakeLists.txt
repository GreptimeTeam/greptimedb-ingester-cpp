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
  ${CMAKE_SOURCE_DIR}/include 
  ${CMAKE_SOURCE_DIR}/third_party/grpc/grpcpp
  ${CMAKE_SOURCE_DIR}/contrib/greptime-proto/c++
)
add_library(${GREPTIME_CLIENT_CPP_LIB}
  db_client.cc
  stream_inserter.cc
)
target_link_libraries(${GREPTIME_CLIENT_CPP_LIB} PRIVATE 
  greptime_proto_cpp_lib
  grpc
)
