cmake_minimum_required(VERSION 3.5)

project(Seasort VERSION 0.1)

list(APPEND CMAKE_PREFIX_PATH "/home/p/g/seastar/build/release")
list(APPEND CMAKE_PREFIX_PATH "/home/p/g/seastar/build/release/_cooking/installed")
list(APPEND CMAKE_MODULE_PATH "/home/p/g/seastar/cmake")

find_package(Seastar REQUIRED)

add_executable(file_sort_example  file_sort_example.cc)

target_link_libraries(file_sort_example  Seastar::seastar)
