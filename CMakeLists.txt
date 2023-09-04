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

cmake_minimum_required(VERSION 3.22)
project(greptimedb-client-cpp VERSION 0.1)

# set cpp standard.
set(CMAKE_CXX_STANDARD 17)

# set the variable name for the greptime-client-cpp library.
set(GREPTIME_CLIENT_CPP_LIB greptime_client_cpp_lib)

# a trick to work around cmake issues.
# see: https://github.com/protocolbuffers/protobuf/issues/12185#issuecomment-1594685860
set(ABSL_ENABLE_INSTALL ON)

# include header files required by the greptime-proto library.
include_directories(
  third_party/grpc/third_party/protobuf/src
  third_party/grpc/third_party/abseil-cpp
)

# build the project tree by specifying all sub-projects as sub-directories.
#
# warning: add all third-party and contrib projects directly by specifying their
# relative paths rather than creating CMakeLists.txt files in their
# corresponding directories.
add_subdirectory(third_party/grpc)
add_subdirectory(contrib/greptime-proto/c++)
add_subdirectory(src)
add_subdirectory(examples)
