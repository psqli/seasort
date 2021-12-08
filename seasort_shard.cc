

#include <algorithm>
#include <concepts>
#include <cstdint>

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>

#include "seasort.hh"
#include "seasort_shard.hh"

using std::vector;
using std::inplace_merge;

namespace seastar {

// Here it's assumed that output_file has already the size attribute equal to that of input_file.
seasort_shard::seasort_shard(seasort_state &state) :
    _state(state),
    _sections(state.section_count, vector<T>(state.section_block_count)),
{
    uint shard_id = this_shard_id();

    // Index of the first block of the shard.
    _first_block = shard_id * _state.blocks_per_shard;
    _shard_block_count = _state.blocks_per_shard;
    // The last shard will be responsible for the remaining blocks (if any).
    if (shard_id == _state._shard_count-1 && _state.blocks_per_shard_remainder > 0) {
        _shard_block_count += _state.blocks_per_shard_remainder;
    }

    uint64_t _shard_sections = _shard_block_count / _state.section_block_count;
}

// This concept guarantees that the type is valid for ordering
template<typename Titerable<Tcomparable>>
concept iterable_and_comparable =
    std::input_iterator<Titerable> &&
    std::totally_ordered<Tcomparable>;


class async_prefetch_iterator
{

public:
    async_prefetch_iterator(uint64_t size) :
        _size(size)
    {
    }
    vector<char> _buffer;

    future<> operator++()
    {
        _pending_fetches.pop_front();
        // TODO:  launch a new fetch
        _pending_fetches.push_back(read_ahead);
    }

private:
    queue<future<>> _pending_fetches;
}

// For merges that fit entirely in memory it returns immediately.
// For merges that require disk access it returns a future for the merge result.
template<typename Titerable typename Tcomparable>
future<>
merge_async(Titerable left, Titerable right, Titerable &output) requires iterable_and_comparable<Titerable>
{

    if (/* current_merge_buffer_size > max_mem_per_shard */)
    inplace_merge(first, last, result);

    co_return result;
}

template<typename Titerable>
future<>
merge_sort(Titerable first)
{
    if (last - first == 1) {
        co_return ;
    }
    if (last - first > 1) {
        Titerable middle = first + (last - first) / 2;
        co_return merge_async(co_await merge_sort(first, middle), 
                              co_await merge_sort(middle, last));
    }
}

future<>
seasort_shard::read_section(uint64_t offset)
{
    _state.input_file.read_dma(_state.block_size * offset, _buffer.data(), _state.block_size * _shard_block_count);
}

// For merges that fit entirely in memory (i.e. size < _state.section_block_count) it returns immediately.
// For merges that require disk access it returns a future for the merge result.
future<uint64_t>
seasort_shard::merge(uint64_t first_offset, uint64_t second_offset, uint64_t size)
{
    using queue = circular_buffer;

    // queue of in-progress async reads
    // NOTE: I decided to not use parallel_for_each here because it would add extra complexity ... and also because coroutines :-)
    queue<future<>> pending_sections;

    for () {
        pending_sections.push_back(read_section(first_offset + i));
    }

    // Force co_await in-order here. This simplifies implementation and does the sane thing as
    // the read requests are sequential and are sent in-order by us. 
    if (!pending_sections.empty()) {
        future<> next_section = co_await pending_sections.pop_front();
    }

    // If merge was not finished, at least one request will be issued because
    // it's guaranteed that a section was consumed above.
    while (pending_sections.size() < _state.shard_allocated_sections) {
        pending_sections.push_back(read_section(first_offset, _buffer));
    }

    // TODO: merge blocks_per_section and call read next section (if any).

    // TODO: make sure that the output buffer is always flushed before merging the next block.

}


// offset and size are used here because implementing an async iterator would not be feasible at the moment
future<uint64_t>
seasort_shard::mergesort(uint64_t offset, uint64_t size) {
	if (size == 1)
		co_return;
    uint64_t middle = size / 2;
	co_return merge(mergesort(offset, middle), mergesort(offset+middle, size-middle), size);
}

future<>
seasort_shard::sort_file(file input_file, uint64_t file_size)
{
    mergesort(input_file, file_size);

    // TODO: when ready, send a message to the core which will merge the result from this core with theirs.
    // The message should pass a reference to the local buffer, so the other cpu can use it.
    // This core may or may not be the merger.

    // If this core is the merger, then it will receive a notification from the core responsible for merging
    // blocks on the shard to be merged next.
}

} // namespace seastar
