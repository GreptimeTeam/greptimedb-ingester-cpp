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
set(GREPTIME_PROTO_CPP_LIB greptim_proto_cpp_lib)
set(GREPTIME_CLIENT_CPP_LIB greptime_client_cpp_lib)

# the sub-directories' order cannot be channged since cmake processes a
# CMakeLists.txt file line by line, and processes each sub-directories
# recursively.
#
# the following order is determined by the dependency relations: 
# (1) compile the grpc library so we can find the grpc package and the protobuf package. 
# (2) use the found protobuf package to compile all required greptime proto files to
#     generate a sequence of required .h and .cc proto files. 
# (3) use the grpc library and the generated .h and .cc proto files to compile the
# greptime-cpp-client library. 
# (4) use the client library to compile the example executables.
add_subdirectory(third_party/grpc)
add_subdirectory(proto)
add_subdirectory(src)
add_subdirectory(examples)
