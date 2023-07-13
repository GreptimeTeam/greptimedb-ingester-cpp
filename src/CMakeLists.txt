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

project(greptimedb_client_lib)

file(GLOB_RECURSE OUTPUT_SOURCES "*.h" "*.cpp")

add_library(${PROJECT_NAME} ${OUTPUT_SOURCES})

target_include_directories(${PROJECT_NAME} PUBLIC ${GREPTIMEDB_ROOT_DIR})

# include contrib c++
set(CONTRIB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/contrib/greptime-proto/c++)
target_include_directories(${PROJECT_NAME} PUBLIC ${CONTRIB_DIR})

target_link_libraries(${PROJECT_NAME} PUBLIC greptime-proto-cpp)