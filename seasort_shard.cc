

#include <concepts>
#include <cstdint>

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>

#include "seasort.hh"
#include "seasort_shard.hh"
#include "seastar/core/fstream.hh"
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

template<typename T>
future<>
seasort_shard<T>::get_input_stream()
{

    uint64_t offset = _first_block * _state.bytes_per_block;
    uint64_t size = _shard_block_count * _state.bytes_per_block;

    _input = make_file_input_stream(_state.input_file, offset, size, file_input_stream_options({
        .buffer_size = _state.get_bytes_per_buffer(),
        .read_ahead = 16, // 
    }));

    _output = co_await make_file_output_stream(_state.output_file, file_output_stream_options({
        .buffer_size = static_cast<unsigned int>(_state.get_bytes_per_buffer()),
        .write_behind = _state.max_buffers_in_memory,
    }));
}

struct block_iterator {
private:
    const uint64_t _bytes_per_block;
    const uint64_t _blocks_per_buffer;
    const uint64_t _blocks_per_stream;
    const input_stream<char> _stream;
    temporary_buffer<char> _current_buffer;
    uint64_t _current_block;
    bool read_next_buffer = true;
public:
    block_iterator(file input, uint64_t bytes_per_block, uint64_t blocks_per_buffer, uint64_t blocks_per_stream, uint64_t skip_blocks, uint read_ahead_sections)
        : _stream(make_file_input_stream(input, skip_blocks * bytes_per_block, blocks_per_stream * bytes_per_block, file_input_stream_options({
            .buffer_size = blocks_per_buffer * bytes_per_block,
            .read_ahead = read_ahead_sections,
        })))
        , _bytes_per_block(bytes_per_block)
        , _blocks_per_buffer(blocks_per_buffer)
        , _blocks_per_stream(blocks_per_stream)
        , _current_block(0)
        , _current_buffer()
    {
        // nothing
    }

    block_iterator& operator ++()
    {
        uint64_t next_within_section = (_current_block % _blocks_per_buffer) + 1;
        if (next_within_section < _blocks_per_buffer) {
            _current_block++;
        } else {
            read_next_buffer = true;
        }
        return *this;
    }

    bool
    has_block()
    {
        return _current_block < _blocks_per_buffer;
    }

    operator bool()
    {
        return _current_buffer.size();
    }

    future<temporary_buffer<char>>
    get_block()
    {
        uint64_t current_block_within_section = _current_block % _blocks_per_buffer;
        if (_current_block >= _blocks_per_stream) {
            // reached the end of stream: return empty buffer
            co_return temporary_buffer<char>();
        } else if (current_block_within_section >= _blocks_per_buffer) {
            // reached the end of section: await another section and copy it to _section_buffer
            // The input stream is responsible for launching one or more read ahead requests.
            _current_buffer = co_await _stream.read_exactly(_bytes_per_block * _blocks_per_buffer);
            read_next_buffer = false;
        }
        // copy elision with Return Value Optimization?
        co_return temporary_buffer(_current_buffer.get_write() + current_block_within_section * _bytes_per_block, _bytes_per_block);
    }
};

template<typename T>
future<output_stream<char>&>
merge_async(block_iterator it_a, block_iterator it_b, output_stream<char> &out)
{
    while (it_a.has_block() && it_b.has_block()) {
        auto a(co_await it_b.get_block());
        auto b(co_await it_b.get_block());
        if (T(a) <= T(b)) { // less_equal operator guarantees stable sort
            co_await out.write(a.get(), a.size());
            ++it_a;
        } else {
            co_await out.write(b.get(), b.size());
            ++it_b;
        }
    }

    co_return out;
}

future<output_stream<char>>
merge_sort(file &input, uint64_t len)
{

	
}

template<typename T>
future<>
seasort_shard<T>::sort_file()
{

    // first pass: merge within each section (memory-only)
    // ==========================================================================================

    for (;;) {
        // read a section
        temporary_buffer<char> buf = co_await _input.read_exactly(_state.get_bytes_per_buffer());
        if (buf.size() == 0) {
            break;
        }

        // order section
        block_iterator it_a(_input, _state.bytes_per_block, _state.blocks_per_buffer, _state.blocks_per_buffer);
        block_iterator it_b(_input, _state.bytes_per_block, _state.blocks_per_buffer, _state.blocks_per_buffer);
        merge_async<T>(it_a, it_b, _output);
    }

    // now output file becomes input file, an vice versa

    // after all sections are internally ordered, merge between sections (hybrid memory-disk)
    // ==========================================================================================

    uint64_t skip = 1;
    while (true) {
        while (true) {
            input_stream<char> in_a = make_file_input_stream(file, offset, size, file_input_stream_options({
                .buffer_size = _state.get_bytes_per_section(),
                .read_ahead = _state.max_sections_in_memory,
            }));
            input_stream<char> in_b = make_file_input_stream(file, offset, size, file_input_stream_options({
                .buffer_size = _state.get_bytes_per_section(),
                .read_ahead = _state.max_sections_in_memory,
            }));
        }
        skip *= 2;
    }

    // TODO: when ready, send a message to the core which will merge the result from this core with theirs.
    // The message should pass a reference to the local buffer, so the other cpu can use it.
    // This core may or may not be the merger.

    // If this core is the merger, then it will receive a notification from the core responsible for merging
    // blocks on the shard to be merged next.
}

} // namespace seastar
