
// 2020-11-26
//
// Sort blocks in a file.
// A merge sort distributed across shards.

#include <seastar/core/file.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/distributed.hh>

#include "seasort.hh"
#include "seasort_shard.hh"

using namespace seastar;

namespace seastar {

template <typename T>
future<int>
seasort<T>::sort_file(file &input_file, file &output_file)
{
    struct seasort_state st;

    st.file_size = co_await input_file.size();
    st.block_size = T::size;

    st.input_file = std::move(input_file);
    st.output_file = std::move(output_file);
    st.file_size = file_size;
    st.block_size = block_size;
    st.block_count = file_size / block_size;
    st.blocks_per_shard = block_count / smp::count;
    // Each shard will read a portion of the input file.
    // FIXME: handle when block_count is less than shard_count
    st.blocks_per_shard_remainder = block_count % smp::count;

    assert(("Must have at least two blocks", state.block_count > 1));

    distributed<seasort_shard> _seasort_sharded;
    co_await _seasort_sharded.start(st);
    co_await _seasort_sharded.wait_for_all();
}

} // namespace seastar
