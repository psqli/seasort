
// 2020-11-26
//
// Sort blocks in a file.
// se a block-merge sort distributed across shards.
//

#include <seastar/core/distributed.hh>
#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>

#include "seasort_shard.hh"

namespace seastar {

    struct seasort_state {
        file input_file;
        file output_file;
        uint64_t block_size = 0;
        uint64_t block_count = 0;
        uint64_t blocks_per_shard = 0;
        uint64_t remaining_blocks = 0;
    };

    template<typename T>
    class seasort {
    public:
        static future<int>
        sort_file(file input_file, file output_file);
    };
};
