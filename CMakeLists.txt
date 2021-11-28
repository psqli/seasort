find_package(Seastar REQUIRED)

add_library(seasort  SHARED  seasort.cc)
target_link_libraries(seasort  Seastar::seastar)

add_executable (file_sort_example  file_sort_example.cc)
add_dependencies(file_sort_example  seasort)
