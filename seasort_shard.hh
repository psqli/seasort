
#pragma once

#include <cstdint>

#include <seastar/core/seastar.hh>
#include <seastar/core/layered_file.hh>
#include <seastar/core/file.hh>

#include "seasort.hh"
#include "seastar/core/iostream.hh"

namespace seastar {

template<typename T>
class seasort_shard {
private:

    input_stream<char> _input;
    output_stream<char> _output;

    seasort_state &_state;

    uint64_t _first_block;
    uint64_t _shard_block_count;

public:
    seasort_shard(seasort_state &state);

    future<>
    prepare();

    future<>
    sort_file();
};

} // namespace seastar
