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

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(example_client_stream)

set(CLIENT_STREAM example_client_stream)
add_executable(${CLIENT_STREAM} client_stream.cpp)

# protobuf
find_package(Protobuf REQUIRED)
message(STATUS "==================Found Protobuf: ${PROTOBUF_VERSION}==================")
message(STATUS "==================Found Protobuf: Include Dir: ${PROTOBUF_INCLUDE_DIRS} Libraries: ${PROTOBUF_LIBRARIES}==================")

target_include_directories(${PROJECT_NAME} PUBLIC ${PROTOBUF_INCLUDE_DIRS})
target_link_libraries(${PROJECT_NAME} PUBLIC ${PROTOBUF_LIBRARIES})

# grpc
find_package(gRPC REQUIRED)

if(gRPC_FOUND)
    message(STATUS "Found gRPC++: ${gRPC_VERSION}")
    set(GRPCPP_INCLUDE_DIRS $<TARGET_PROPERTY:gRPC::grpc++,INTERFACE_INCLUDE_DIRECTORIES>)
    set(GRPCPP_LIBRARIES $<TARGET_PROPERTY:gRPC::grpc++_unsecure,INTERFACE_LINK_LIBRARIES>)
    set(GRPCPP_PLUGIN_EXEC $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
    find_package(PkgConfig REQUIRED)
    pkg_search_module(GRPCPP REQUIRED QUIET grpc++_unsecure)
    set(GRPCPP_PLUGIN_EXEC ${GRPCPP_PREFIX}/bin/grpc_cpp_plugin)
    message(STATUS "Found gRPC++: ${GRPCPP_LINK_LIBRARIES} (found version \"${GRPCPP_VERSION}\")")
endif()


set(CONTRIB_DIR ${GREPTIMEDB_CLIENT_ROOT_DIR}/contrib/greptime-proto/c++)

target_include_directories(${CLIENT_STREAM} PUBLIC ${CONTRIB_DIR})
target_link_libraries(${CLIENT_STREAM} PUBLIC greptime_proto_cpp_lib)
# target_link_libraries(${CLIENT_STREAM} PUBLIC ${PROTOBUF_LIBRARIES})
target_link_libraries(${CLIENT_STREAM} PUBLIC ${GRPCPP_LIBRARIES})
target_link_libraries(${CLIENT_STREAM} PUBLIC grpc grpc++)

set(SRC_DIR ${GREPTIMEDB_CLIENT_ROOT_DIR})
target_include_directories(${CLIENT_STREAM} PUBLIC ${SRC_DIR})
target_link_libraries(${CLIENT_STREAM} PUBLIC greptimedb_client_lib)
