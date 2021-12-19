
#include <boost/range/irange.hpp>
#include <concepts>
#include <cstdint>

#include <cstdlib>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <utility>

#include "seasort.hh"
#include "seasort_shard.hh"
#include "seastar/core/fstream.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/core/when_all.hh"

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

struct file_range {
    file &_file;
    const uint64_t _block_size;
    const uint64_t _block_offset;
    const uint64_t _block_count;
    const uint _read_ahead;
    file_range(file &file, uint64_t block_size, uint64_t block_offset, uint64_t block_count, uint read_ahead)
        : _file(file)
        , _block_size(block_size)
        , _block_offset(block_offset)
        , _block_count(block_count)
        , _read_ahead(read_ahead)
        { }

    /// Changes the data source without resetting the range parameters.
    /// Returns the current file reference.
    file& change_file(file &new_file_ref) {
        file& old_file_ref = _file;
        _file = new_file_ref;
        return old_file_ref;
    }
};

// this iterator is an effort for reading a file sequentially while allowing to split and skip
// ranges efficiently
//
//   max blocks in memory (per iterator)       max blocks for pure
//   -----------------------------------   =   in-memory range
//           min read ahead
class block_range_iterator {
    file_range _fr;

    const uint64_t _block_offset;
    const uint64_t _block_count;

    input_stream<char> _range;
    temporary_buffer<char> _buffer;

    uint64_t _current_block = 0;
    int64_t _prev_block = -1;

    uint64_t _blocks_to_skip = 0;

public:
    block_range_iterator(file_range fr, uint64_t block_offset, uint64_t block_count)
        : _fr(fr)
        , _block_offset(block_offset)
        , _block_count(block_count)
        , _range(
            make_file_input_stream(fr._file, block_offset * fr._block_size,
                std::min(block_count * fr._block_size, fr._read_ahead * fr._block_size),
                file_input_stream_options({
                    .buffer_size = fr._block_size,
                    .read_ahead = fr._read_ahead /* TODO: maximize memory usage here */,
            }))) { }

    /// Return a new block_range_iterator
    future<block_range_iterator>
    next_range() {
        auto next_range_block_offset = _block_offset + _block_count;
        auto next_range_block_count = std::min(_block_count, _fr._block_count - next_range_block_offset);
        block_range_iterator next_range(_fr, next_range_block_offset, next_range_block_count);

        // If next_range is an in-memory range, move the input stream to it.
        // The rationale is that if a range can be entirely in-memory, it's
        // not necessary to create a separate input stream for the next
        // range.
        if (_block_count <= /* TODO: blocks_per_buffer */) {
            next_range._range = std::move(_range);
        }

        co_return next_range;
    }

    std::pair<block_range_iterator, block_range_iterator>
    split() {
        return {
            block_range_iterator(_fr, _block_offset, _block_count/2),
            block_range_iterator(_fr, _block_offset + _block_count/2, _block_count - _block_count/2),
        };
    }

    uint64_t size() {
        return _block_count;
    }

    bool
    has_block() {
        return _current_block < _block_count;
    }

    block_range_iterator& operator++ () {
        if (_buffer.size())
            _buffer.trim_front(_fr._block_size);
        else
            _blocks_to_skip++;
        return *this;
    }

    block_range_iterator operator++ (int) {
        block_range_iterator copy(_fr, _block_offset + _current_block, _block_count - _current_block);
        copy._buffer = _buffer.clone();
        ++*this;
        return copy;
    }

    /// Get the current block. The future resolves immediately if the current block is already in memory.
    /// This does not increment the iterator, so calling it twice in a row will return the same block.
    future<temporary_buffer<char>>
    get_block() {
        // if previously incremented, here the blocks are skipped
        if (_blocks_to_skip) {
            auto blocks_in_buffer = _buffer.size() / _fr._block_size;
            int64_t bytes_diff = (blocks_in_buffer - _blocks_to_skip) * _fr._block_size;
            _buffer.trim_front(std::max(bytes_diff, 0L));
            // skip input_stream if needed
            if (bytes_diff < 0)
                co_await _range.skip(abs(bytes_diff));
            _blocks_to_skip = 0;
        }
        // input_stream already does buffering. However, the buffering done here is to allow in-memory
        // iterators to be used when the range size is too small.
        if (_buffer.size()) {
            co_return _buffer.share(0, _fr._block_size);
        } else {
            co_return co_await _range.read_exactly(_fr._block_size);
        }
    }
};

block_range_iterator
make_block_range_iterator(file &file, uint64_t bytes_per_block, uint64_t block_offset, uint64_t block_count, uint read_ahead) {
    auto fr = file_range(file, bytes_per_block, block_offset, block_count, read_ahead);
    return block_range_iterator(std::move(fr), block_offset, block_count);
}

template<typename T>
future<block_range_iterator>
merge_async(block_range_iterator it_a, block_range_iterator it_b, file &aux)
{
    while (it_a.has_block() && it_b.has_block()) {
        auto a(co_await it_a.get_block()), b(co_await it_b.get_block());
        if (T(a) <= T(b)) { // less_equal operator guarantees stable sort
            co_await out.write(a.get(), a.size());
            ++it_a;
        } else {
            co_await out.write(b.get(), b.size());
            ++it_b;
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
depth_first_merge_sort(block_range_iterator a, file &aux)
{
    if (a.size() == 1)
        co_return a; // RVO must happen here as member _buffer is not copyable
    auto splitted = a.split();
    co_return co_await merge_async<T>(co_await depth_first_merge_sort<T>(std::move(splitted.first), aux),
                                      co_await depth_first_merge_sort<T>(std::move(splitted.second), aux),
                                      aux);
}

future<>
breadth_first_merge_sort(block_range_iterator it, file &aux) {
    _output = co_await make_file_output_stream(_state.output_file, file_output_stream_options({
        .buffer_size = static_cast<unsigned int>(_state.get_bytes_per_buffer()),
        .write_behind = _state.max_buffers_in_memory,
    }));
    while (true) {
        auto it_a(it), it_b(it);
        auto it_out(it);
        merge_async<T>(it_a, it_b, it_out);
        // change file
    }
}

template<typename T>
future<>
seasort_shard<T>::sort_file()
{
    auto it = co_await make_block_range_iterator(_state.input_file, _state.bytes_per_block, _first_block, _shard_block_count, 16 /* TODO: what value */);

    breadth_first_merge_sort(it);

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
