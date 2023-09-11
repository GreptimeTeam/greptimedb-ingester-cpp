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

# set the required min cmake version to 3.13 which was released in 2018. This
# may hopefully mitigate the compatability issues.
cmake_minimum_required(VERSION 3.13)
project(greptimedb_client_cpp VERSION 0.1)

# set the required cpp standard to c++11.
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# set well-known variables to be used by sub-projects.
set(GREPTIME_PROTO_CPP_LIB greptime_proto_cpp_lib)
set(GREPTIME_CLIENT_CPP_LIB greptime_client_cpp_lib)

include_directories(
  third_party/grpc/third_party/abseil-cpp
  third_party/grpc/third_party/protobuf/src
  third_party/grpc/include
  proto
  src
  examples
)

# the sub-directories' order cannot be channged since cmake processes a
# CMakeLists.txt file line by line, and processes each sub-directories
# recursively.
# add_subdirectory(third_party/grpc)
add_subdirectory(proto)
add_subdirectory(src)
add_subdirectory(examples)
