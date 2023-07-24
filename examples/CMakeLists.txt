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

set(CLIENT_STREAM example_client_stream)
add_executable(${CLIENT_STREAM} client_stream.cpp)

# greptime lib
target_link_libraries(${CLIENT_STREAM} 
        PUBLIC 
        greptime_proto_cpp_lib
        greptimedb_client_lib
        )

# third party lib
target_link_libraries(${CLIENT_STREAM} 
        PUBLIC 
        grpc grpc++
        )

# include<src/xxxx.h>
target_include_directories(${CLIENT_STREAM} 
        PUBLIC 
        ${GREPTIMEDB_CLIENT_ROOT_DIR}
        )
