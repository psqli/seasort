
#include <cstdint>

#include <seastar/core/file.hh>

#include "seasort.hh"

namespace seastar {

class seasort_shard {
private:
    seasort_state &_state;

    uint64_t _shard_block_first;
    uint64_t _shard_block_count;

public:
    seasort_shard(file input_file, uint64_t input_file_size);

    future<>
    sort_file(file input_file);
};

} // namespace seastar
