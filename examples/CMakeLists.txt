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

set(EXAMPLE_TARGET example_stream_inserter)
add_executable(${EXAMPLE_TARGET}
  stream_insert.cc 
  weather_record.cc
)

# for link options: PRIVATE, INTERFACE, PUBLIC, see:
# https://cmake.org/pipermail/cmake/2016-May/063400.html
target_link_libraries(${EXAMPLE_TARGET} PRIVATE
  ${GREPTIME_CLIENT_CPP_LIB}
)
