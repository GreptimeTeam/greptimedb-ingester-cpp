#! /usr/bin/env bash

for dir in "examples" "src"; do
    clang-format -style=file:./.clang-format -i ${dir}/*.cpp ${dir}/*.h
done
