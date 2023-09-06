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
project(greptimedb-client-cpp VERSION 0.1)

# set the required cpp standard to c++11.
set(CMAKE_CXX_STANDARD 11)

# bind a variable to be reused by sub-projects.
set(GREPTIME_CLIENT_CPP_LIB greptime_client_cpp_lib)

# specify required proto files.
set(PROTO_FILE_PATH_PREFIX ${CMAKE_SOURCE_DIR}/contrib/greptime-proto/proto/greptime/v1)
set(REQUIRED_GREPTIME_PROTO_FILES
  ${PROTO_FILE_PATH_PREFIX}/common.proto
  ${PROTO_FILE_PATH_PREFIX}/database.proto
  ${PROTO_FILE_PATH_PREFIX}/row.proto
)

# find protobuf in the given path.
set(PROTOBUF_PATH ${CMAKE_SOURCE_DIR}/third_party/grpc/third_party/protobuf)
set(CMAKE_PREFIX_PATH ${PROTOBUF_PATH})
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

# feed the required greptime protobuf files to the found protobuf to generate c++ proto files.
# the generated c++ source files are referenced by the CPP_PROTO_SRCS variable 
# while the generated c++ header files are referenced by the CPP_PROTO_HDRS variable.
# the generated files are placed into the Protobuf_GENERATE_CPP_OUT_DIR path.
set(Protobuf_GENERATE_CPP_OUT_DIR ${CMAKE_SOURCE_DIR}/proto)
protobuf_generate_cpp(CPP_PROTO_SRCS CPP_PROTO_HDRS ${REQUIRED_GREPTIME_PROTO_FILES})

# process sub-projects recursively.
add_subdirectory(third_party/grpc)
add_subdirectory(src)
add_subdirectory(examples)
