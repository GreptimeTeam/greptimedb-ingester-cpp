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

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/greptime-proto/c++)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/grpc)


#"${folder}/CMakeLists.txt" Third-party libraries may have substandard code.

# set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")
# set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# set_property(DIRECTORY PROPERTY EXCLUDE_FROM_ALL 1)

# # add_contrib cmake_folder[ base_folder1[, ...base_folderN]]
# function(add_contrib cmake_folder)
#     if (ARGN)
#         set(base_folders ${ARGN})
#     else()
#         set(base_folders ${cmake_folder})
#     endif()

#     foreach (base_folder ${base_folders})
#         # some typos in the code
#         if (NOT IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${base_folder}")
#             message(FATAL_ERROR "No such base folder '${base_folder}' (for '${cmake_folder}' cmake folder). Typo in the base folder name?")
#         endif()

#         file(GLOB contrib_files "${base_folder}/*")
#         if (NOT contrib_files)
#             # Checking out *all* submodules takes > 5 min. Therefore, the smoke build ("FastTest") in CI initializes only the set of
#             # submodules minimally needed for a build and we cannot assume here that all submodules are populated.
#             message(STATUS "submodule ${base_folder} is missing or empty. to fix try run:")
#             message(STATUS "    git submodule update --init")
#             return()
#         endif()
#     endforeach()

#     message(STATUS "Adding contrib module ${base_folders} (configuring with ${cmake_folder})")
#     add_subdirectory (${cmake_folder})
# endfunction()

# add_contrib (grpc)
# add_contrib(greptime-proto/c++)
