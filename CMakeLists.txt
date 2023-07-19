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

#=================== global packet ===================#
# file(GLOB_RECURSE sources "*.cpp")
# file(GLOB_RECURSE headers "*.h")

# include_directories(SYSTEM contrib/greptime-proto/c++)

# add_library(greptimedb ${headers} ${sources})
# target_include_directories(greptimedb PUBLIC contrib/greptime-proto/c++)
#=================== global packet ===================#

# set project root
set(GREPTIMEDB_CLIENT_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

# sub CMakeLists.txt
add_subdirectory(contrib)
add_subdirectory(src)
add_subdirectory(examples)
