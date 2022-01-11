
// Seastar headers
#include <seastar/core/smp.hh>

// Local headers
#include "block_range_iterator.hh"
#include "seasort.hh"
#include "seasort_shard.hh"

using std::vector;
using std::inplace_merge;

namespace seastar {

// Here it's assumed that output_file has already the size attribute equal to that of input_file.
template<typename T>
seasort_shard<T>::seasort_shard(seasort_state &state) :
    _state(state)
    //_sections(state.section_count, vector<T>(state.section_block_count)),
{
    uint shard_id = this_shard_id();

    // Index of the first block of the shard.
    _first_block = shard_id * _state.blocks_per_shard;
    _shard_block_count = _state.blocks_per_shard;
    // The last shard will be responsible for the remaining blocks (if any).
    if (shard_id == smp::count-1 && _state.blocks_per_shard_remainder > 0) {
        _shard_block_count += _state.blocks_per_shard_remainder;
    }

    uint64_t _shard_sections = _shard_block_count / _state.blocks_per_buffer;
    if (_shard_block_count % _state.blocks_per_buffer > 0) {
        _shard_sections++;
    }
}

//using range = boost::integer_range<uint64_t>;

template<typename T>
future<block_range_iterator>
merge_async(block_range_iterator it_a, block_range_iterator it_b, output_stream<char> out)
{
    while (it_a.has_block() && it_b.has_block()) {
        auto a(co_await it_a.get_block()), b(co_await it_b.get_block());
        if (T(a) <= T(b)) { // less_equal operator guarantees stable sort
            co_await out.write(a.get(), a.size());
        } else {
            co_await out.write(b.get(), b.size());
        }
    }

    // flip files: This is needed for merge sort that is not in-place
    // now output file becomes input file, an vice versa

    co_return block_range_iterator();
}

// depth-first (recursive) merge sort
//
// One of the main issues here is to decide the size of the output_buffer and when it
// will be flush. This may impact on the IO performance. For example, if the buffer
// is written and read a few sections later, the read ahead from disk hardware will
// be discarded, and read position will return to a point which may not be cached.
//
// invariant: merges do not overlap and do not happen simultaneously or concurrently
template<typename T>
future<block_range_iterator>
depth_first_merge_sort(block_range_iterator a, block_range_iterator aux)
{
    if (a.size() == 1)
        co_return a; // RVO must happen here as member _buffer is not copyable
    auto splitted = a.split();
    co_return co_await merge_async<T>(co_await depth_first_merge_sort<T>(std::move(splitted.first), aux),
                                      co_await depth_first_merge_sort<T>(std::move(splitted.second), aux),
                                      aux);
}

template<typename T>
future<>
breadth_first_merge_sort(block_range_iterator it, block_range_iterator it_out) {
    uint64_t range_size = 1;
    while (true) {
        auto out = co_await it_out.get_output_stream();
        range_size *= 2;
        auto current_range = it.get_subrange(0, range_size);
        while (true) {
            auto splitted = current_range.split();
            auto it_a(std::move(splitted.first)), it_b(std::move(splitted.second));
            merge_async<T>(it_a, it_b, out);
            current_range.get_next_range();
        }
        // change file
    }
}

// read_ahead defines the maximum memory reserved for each iterator
// For seasort, there are at most two iterators active at a time.
template<typename T>
future<>
seasort_shard<T>::sort_file()
{
    auto it = make_block_range_iterator(_state.input_file, _state.bytes_per_block, _first_block, _shard_block_count, 16 /* TODO: what value */);
    auto out = make_block_range_iterator(_state.output_file, _state.bytes_per_block, _first_block, _shard_block_count, 16 /* TODO: what value */);

    breadth_first_merge_sort(it, out);

    // TODO: when ready, send a message to the core which will merge the result from this core with theirs.
    // The message should pass a reference to the local buffer, so the other cpu can use it.
    // This core may or may not be the merger.

    // If this core is the merger, then it will receive a notification from the core responsible for merging
    // blocks on the shard to be merged next.

    uint parent_shard = 0; // TODO: set parent shard
    smp::submit_to(parent_shard, [] () {
        // TODO: send the range here
    });
}

} // namespace seastar
