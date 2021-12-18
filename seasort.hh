
// 2020-11-26
//
// Sort blocks in a file.
// se a block-merge sort distributed across shards.
//

#pragma once

#include "seastar/core/file.hh"
#include <cstdint>

namespace seastar {

    struct seasort_state {
        file input_file;
        file output_file;


        // Global parameters
        // ==============================================================

        uint64_t bytes_per_block = 0;
        uint64_t total_blocks = 0;

        /// Defines how many sections can be simultaneously read from the input file.
        /// This allows disk to read while the CPU is merging.


        // Shard parameters
        // ==============================================================

        uint64_t blocks_per_shard = 0;
        /// total_blocks / smp::count may have a remainder, which will be added to the last shard.
        uint64_t blocks_per_shard_remainder = 0;


        // Buffer parameters
        // ==============================================================

        uint64_t blocks_per_buffer = 512;

        uint64_t get_bytes_per_buffer() const {
            return blocks_per_buffer * bytes_per_block;
        }
    };

    template<typename T>
    class seasort {
    private:
        struct seasort_state _state;
    public:
        static future<int>
        sort_file(file &input_file, file &output_file);
    };
};
