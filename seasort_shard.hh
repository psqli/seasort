
#include <cstdint>

#include <seastar/core/seastar.hh>
#include <seastar/core/layered_file.hh>
#include <seastar/core/file.hh>

#include "seasort.hh"

namespace seastar {

class seasort_shard {
private:
    seasort_state &_state;

    uint64_t _first_block;
    uint64_t _shard_block_count;

    vector<vector<T>> _sections;

public:
    seasort_shard(seasort_state &state);

    future<>
    sort_file(file input_file);
};

} // namespace seastar
