

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

    uint64_t _shard_sections = _shard_block_count / _state.blocks_per_section;
    if (_shard_block_count % _state.blocks_per_section > 0) {
        _shard_sections++;
    }
}

template<typename T>
future<>
seasort_shard<T>::prepare()
{

    uint64_t offset = _first_block * _state.bytes_per_block;
    uint64_t size = _shard_block_count * _state.bytes_per_block;
    _input = make_file_input_stream(_state.input_file, offset, size, file_input_stream_options({
        .buffer_size = _state.get_bytes_per_section(),
        .read_ahead = _state.max_sections_in_memory,
    }));

    _output = co_await make_file_output_stream(_state.output_file, file_output_stream_options({
        .buffer_size = static_cast<unsigned int>(_state.get_bytes_per_section()),
        .write_behind = _state.max_sections_in_memory,
    }));
}

struct block_iterator {
private:
    input_stream<char> &_stream;
    const uint64_t _bytes_per_block;
    const uint64_t _blocks_per_section;
    const uint64_t _blocks_per_stream;
    temporary_buffer<char> _section_buf;
    uint64_t _current_block;
    bool _read_next_section = true;
public:
    block_iterator(input_stream<char> &stream, uint64_t bytes_per_block, uint64_t blocks_per_section, uint64_t blocks_per_stream)
        : _stream(stream)
        , _bytes_per_block(bytes_per_block)
        , _blocks_per_section(blocks_per_section)
        , _blocks_per_stream(blocks_per_stream)
        , _current_block(0)
        , _section_buf()
    {}

    block_iterator& operator ++()
    {
        uint64_t next_within_section = (_current_block % _blocks_per_section) + 1;
        if (next_within_section < _blocks_per_section) {
            _current_block++;
        } else {
            _read_next_section = true;
        }
        return *this;
    }

    bool
    has_next()
    {
        uint64_t next_block = _current_block + 1;
        return next_block < _blocks_per_stream;
    }

    operator bool()
    {
        return _section_buf.size();
    }

    future<temporary_buffer<char>>
    get()
    {
        uint64_t current_block_within_section = _current_block % _blocks_per_section;
        if (_current_block >= _blocks_per_stream) {
            // reached the end of stream: return empty buffer
            co_return temporary_buffer<char>();
        } else if (current_block_within_section >= _blocks_per_section) {
            // reached the end of section: await another section and copy it to _section_buffer
            // The input stream is responsible for launching one or more read ahead requests.
            _section_buf = co_await _stream.read_exactly(_bytes_per_block * _blocks_per_section);
            _read_next_section = false;
        }
        // copy elision with Return Value Optimization?
        co_return temporary_buffer(_section_buf.get_write() + current_block_within_section * _bytes_per_block, _bytes_per_block);
    }
};

template<typename T>
future<output_stream<char>&>
merge_async(block_iterator it_a, block_iterator it_b, output_stream<char> &out)
{
    while (it_a.has_next() && it_b.has_next()) {
        auto a{co_await it_a.get()}, b(co_await it_b.get());
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

template<typename T>
future<>
seasort_shard<T>::sort_file()
{

    // First, order each section.
    // input_stream reads ahead as configured.
    for (;;) {
        temporary_buffer<char> buf = co_await _input.read_exactly(_state.get_bytes_per_section());
        if (buf.size() == 0) {
            break;
        }

        block_iterator it(buf, _state.get_bytes_per_section());

        inplace_merge(it, it + _state.blocks_per_section / 2, it + (_state.blocks_per_section - 1),
            [](const temporary_buffer<char>& a, const temporary_buffer<char>& b) {
                return T(a) < T(b);
            });

        co_await _output.write(buf.get());
    }

    // Now, merge sections

    merge_async();

    // TODO: when ready, send a message to the core which will merge the result from this core with theirs.
    // The message should pass a reference to the local buffer, so the other cpu can use it.
    // This core may or may not be the merger.

    // If this core is the merger, then it will receive a notification from the core responsible for merging
    // blocks on the shard to be merged next.
}

} // namespace seastar
