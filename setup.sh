#!/bin/sh

seasort_dir=$(pwd)
[ ! -f $seasort_dir/seasort.cc ] && echo "seasort not found" && exit 1
mkdir -p $seasort_dir/build
cd $seasort_dir/build

seastar_dir=../seastar
[ ! -d $seastar_dir ] && echo "seastar not found" && exit 1

cmake -DCMAKE_PREFIX_PATH="$seastar_dir/build/release;$seastar_dir/build/release/_cooking/installed" -DCMAKE_MODULE_PATH=$seastar_dir/cmake $seasort_dir
