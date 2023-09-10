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

# specify required input proto files.
set(INPUT_PROTO_FILE_PATH ${CMAKE_SOURCE_DIR}/contrib/greptime-proto/proto/greptime/v1)
set(INPUT_PROTO_FILES
  ${INPUT_PROTO_FILE_PATH}/common.proto
  ${INPUT_PROTO_FILE_PATH}/database.proto
  ${INPUT_PROTO_FILE_PATH}/row.proto
)
message("input proto files: ${INPUT_PROTO_FILES}")

# find the required protobuf package within the project.
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR})
find_package(Protobuf QUIET)

# compile the input proto files using the Protobuf_PROTOC_EXECUTABLE executable which is found 
# by the previous `find_package` command.
# the generated .h and .cc files can be referenced by the PROTO_SRCS and PROTO_HDRS variables.
add_custom_target(
  GENERATE_CPP_PROTO_FILES
  COMMAND protobuf_generate_cpp PROTO_SRCS PROTO_HDRS ${INPUT_PROTO_FILES}
  COMMAND add_library ${GREPTIME_PROTO_CPP_LIB} ${PROTO_SRCS}
)

# compile the proto source files into a library.
# add_library(${GREPTIME_PROTO_CPP_LIB} ${PROTO_SRCS})
