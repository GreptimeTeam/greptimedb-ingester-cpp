set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
project(greptimedb_client_lib)

file(GLOB_RECURSE OUTPUT_SOURCES "*.h" "*.cpp")

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
message(STATUS "==================Found Protobuf: ${PROTOBUF_VERSION}==================")
message(STATUS "==================Found Protobuf: Include Dir: ${PROTOBUF_INCLUDE_DIRS} Libraries: ${PROTOBUF_LIBRARIES}==================")

target_include_directories(${PROJECT_NAME} PUBLIC ${PROTOBUF_INCLUDE_DIRS})
target_link_libraries(${PROJECT_NAME} PUBLIC ${PROTOBUF_LIBRARIES})

# grpc
# find_package(gRPC REQUIRED)

# if(gRPC_FOUND)
#     message(STATUS "Found gRPC++: ${gRPC_VERSION}")
#     set(GRPCPP_INCLUDE_DIRS $<TARGET_PROPERTY:gRPC::grpc++,INTERFACE_INCLUDE_DIRECTORIES>)
#     set(GRPCPP_LIBRARIES $<TARGET_PROPERTY:gRPC::grpc++_unsecure,INTERFACE_LINK_LIBRARIES>)
#     set(GRPCPP_PLUGIN_EXEC $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
# else()
#     find_package(PkgConfig REQUIRED)
#     pkg_search_module(GRPCPP REQUIRED QUIET grpc++_unsecure)
#     set(GRPCPP_PLUGIN_EXEC ${GRPCPP_PREFIX}/bin/grpc_cpp_plugin)
#     message(STATUS "Found gRPC++: ${GRPCPP_LINK_LIBRARIES} (found version \"${GRPCPP_VERSION}\")")
# endif()

# message(STATUS "==================Found gRPC++: ${gRPC_VERSION}==================")

# set(GRPCPP_LIBRARIES /usr/local/lib/libgrpc++.a /usr/local/lib/libgrpc.a)
# target_link_libraries(${PROJECT_NAME} PUBLIC ${GRPCPP_LIBRARIES})


# target_include_directories(${PROEJCT_NAME} PUBLIC ${GRPC})