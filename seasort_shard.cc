

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
    uint64_t const _block_size;
    temporary_buffer<char> _buf;
    uint64_t _current_block;
    bool _read_next_section = true;
public:
    block_iterator(input_stream<char> &stream, uint64_t block_size)
        : _stream(stream)
        , _buf()
        , _block_size(block_size)
        , _current_block(0)
    {}

    block_iterator& operator ++()
    {
        uint64_t next_pos = _current_block + 1;
        if (next_pos < _buf.size()) {
            _current_block = next_pos;
        } else {
            _read_next_section = true;
        }
        return *this;
    }

    operator future<temporary_buffer<char>>()
    {
        if (_read_next_section) {
            _read_next_section = false;
            _buf = co_await _stream.read_exactly(_block_size * 1000 /* TODO: blocks per section */);
            // The stream is responsible for launching one or more read ahead requests.
        }
        // copy elision with Return Value Optimization?
        co_return temporary_buffer(_buf.get_write() + _current_block, _block_size);
    }
};

template<typename T>
future<>
merge_async(block_iterator it_a, block_iterator it_b, output_stream<char> out)
{
    while (1) {
        T a(co_await it_a);
        T b(co_await it_b);

        if (a < b) {
            co_await out.write(it_a++);
        } else {
            co_await out.write(it_b++);
        }
    }
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
