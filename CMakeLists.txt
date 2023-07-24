# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required (VERSION 3.22)
project (greptimedb-client-cpp VERSION 0.1 LANGUAGES CXX)

# generate complie_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# set project root directory
set(GREPTIMEDB_CLIENT_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

set(GREPTIMEDB_CLIENT_THIRD_PARTY_INCLUDE_DIR 
        ${GREPTIMEDB_CLIENT_ROOT_DIR}/contrib
        ${GREPTIMEDB_CLIENT_ROOT_DIR}/contrib/greptime-proto/c++
        ${GREPTIMEDB_CLIENT_ROOT_DIR}/contrib/grpc/include
        ${GREPTIMEDB_CLIENT_ROOT_DIR}/contrib/grpc/third_party/abseil-cpp
        )

include_directories(
        ${GREPTIMEDB_CLIENT_THIRD_PARTY_INCLUDE_DIR}
        )

add_subdirectory(contrib)
add_subdirectory(src)
add_subdirectory(examples)
