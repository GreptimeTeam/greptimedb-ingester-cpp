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
project(greptimedb_client_lib)

# file(GLOB_RECURSE OUTPUT_SOURCES "*.h" "*.cpp")
set(OUTPUT_SOURCES
    client.h
    client.cpp
    database.h
    database.cpp
)

include(FindProtobuf)
find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})

add_library(${PROJECT_NAME} ${OUTPUT_SOURCES})


# include contrib c++
set(CONTRIB_DIR ${GREPTIMEDB_CLIENT_ROOT_DIR}/contrib/greptime-proto/c++)
target_include_directories(${PROJECT_NAME} PUBLIC ${CONTRIB_DIR})

target_link_libraries(${PROJECT_NAME} PUBLIC greptime_proto_cpp_lib)

# protobuf
find_package(Protobuf REQUIRED)

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
