# greptimedb-client-cpp

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
./example_client_stream
```