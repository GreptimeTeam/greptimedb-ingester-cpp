#!/usr/bin/bash

# let shell exit immediately if encounters an error
set -e 

# this script shall be executed in the project root path
wd=$(pwd)
cpp_out_dir="${wd}/proto"

# build grpc
echo "Building grpc ..."
cd "third_party/grpc/cmake"
cd build

# rm -rf build && mkdir build 
# cd build
# cmake ../.. -DCMAKE_CXX_STANDARD=11
# make -j

protoc_path=$(pwd)/third_party/protobuf
grpc_cpp_plugin_path=$(pwd)

# generate cpp proto files.
echo "Generating cpp proto files ..."
cd "${wd}/contrib/greptime-proto"
input_proto_path="proto"
input_proto_files=$(find ${input_proto_path} -type f -iname "*.proto")
${protoc_path}/protoc -I ${input_proto_path} --cpp_out=${cpp_out_dir} ${input_proto_files}
${protoc_path}/protoc -I ${input_proto_path} --grpc_out=${cpp_out_dir} --plugin=protoc-gen-grpc=${grpc_cpp_plugin_path}/grpc_cpp_plugin ${input_proto_files}

echo "Success!"
