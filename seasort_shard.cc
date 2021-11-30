
#include <array>
#include <cassert>
#include <cstdint>

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>

#include "seasort.hh"
#include "seasort_shard.hh"

using seastar::input_stream;

namespace seastar {

// Here it's assumed that output_file has already the size attribute equal to that of input_file.
seasort_shard::seasort_shard(seasort_state &state)
    : _state(state)
{
    uint shard_id = this_shard_id();

    // Index of the first block of the shard.
    _shard_block_first = shard_id * _state.blocks_per_shard;
    _shard_block_count = _state.blocks_per_shard;
    // The last shard will be responsible for the remaining blocks (if any).
    if (shard_id == _state._shard_count-1 && _state.blocks_per_shard_remainder > 0) {
        _shard_block_count += _state.blocks_per_shard_remainder;
    }
}

future<>
seasort_shard::merge()
{
    std::array<T> buffer;
    // Make sure the input buffer is always full
    // and make sure the output buffer is always flushed
    // before merging the next block.
}

future<>
seasort_shard::mergesort(file *A, int len) {
	if (len == 1)
		return A;
	return merge(mergesort(A, len/2), mergesort(A+len/2, len-len/2), len);
}

future<>
seasort_shard::sort_file(file input_file, uint64_t file_size)
{


    // Bottom levels of the merge should be handled entirely in-memory.
    // When there is no more memory for the current size of the array,
    if (current_array_size > max_array_size) {
    }


    mergesort(input_file, file_size);


    // TODO: when ready, send a message to the core which will merge the result from this core with theirs.
    // The message should pass a reference to the local buffer, so the other cpu can use it.
    // This core may or may not be the merger.

    // If this core is the merger, then it will receive a notification from the core responsible for merging
    // blocks on the shard to be merged next.
}

}
