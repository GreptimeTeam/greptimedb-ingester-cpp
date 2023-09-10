# greptimedb-client-cpp

## Preparation

There are a few procedures you need to accomplish first in order to build the project successfully.

- Initialize all necessary submodules by following the instructions in `docs/submodule_instructions.md`.
- Build the `protoc` compiler by executing the script `scripts/build_protoc.sh`.
  - This script builds the `protoc` compiler using the source codes in the `third_party/grpc/third_party/protobuf` submodule.
- Generate all required cpp protobuf files by executing the script `scripts/generate_cpp_proto.sh`.
  - This script generates protobuf files by feeding required `.proto` files to the built `proto` compiler.

## Build

```bash
git clone --recurse-submodules --depth 1 --shallow-submodules https://github.com/GreptimeTeam/greptimedb-client-cpp

cd greptimedb-client-cpp

# create a new build directory where the project is compiled
mkdir build && cd build

cmake ..

make -j$(nproc)
```

## Run examples

```bash
# the test program is in the greptimedb-client-cpp/build/examples directory
cd greptimedb-client-cpp/build/examples

# run the example binary
./example_stream_inserter
```
