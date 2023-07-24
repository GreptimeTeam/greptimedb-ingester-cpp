# greptimedb-client-cpp

## Dependencies

- `libprotobuf-lite32_3.21`
- `libprotobuf32_3.21`
- `libprotoc32_3.21`
- `protobuf-compiler_3.21`
- `libprotobuf-dev_3.21`

## Build

```bash
git clone git@github.com:GreptimeTeam/greptimedb-client-cpp.git

cd greptimedb-client-cpp

# update submodule 
git submodule update --init --recursive

# create a new build directory where the project is compiled
mkdir build && cd build

cmake ..

make -j$(nproc)
```

## Run

```bash
# the test program is in the greptimedb-client-cpp/build/examples directory
cd greptimedb-client-cpp/build/examples

# run the executable file you want to execute
./example_client_stream
```