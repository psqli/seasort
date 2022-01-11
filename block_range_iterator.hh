
#pragma once

// C++ std headers
#include <cstdint>

// Seastar headers
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/temporary_buffer.hh>

namespace seastar {

// this iterator allows to read a file sequentially while also allowing to split and
// skip ranges efficiently
//
//   max blocks in memory (per iterator)       max blocks for pure
//   -----------------------------------   =   in-memory range
//           min read ahead
class block_range_iterator {
    using tmp_buffer = temporary_buffer<char>;

    file &_file;

    const uint64_t _block_offset;
    const uint64_t _block_count;
    const uint64_t _block_size;
    const uint _read_ahead;

    input_stream<char> _range_stream;

    queue<future<tmp_buffer>> _future_blocks;
    bool _in_memory = false;

    uint64_t _current_block = 0;

public:
    block_range_iterator(file &file, uint64_t block_offset, uint64_t block_count,
                         uint64_t block_size = 4096, uint read_ahead = 4096,
                         input_stream<char> *range_stream = nullptr)
        : _file(file)
        , _block_offset(block_offset)
        , _block_count(block_count)
        , _block_size(block_size)
        , _read_ahead(read_ahead)
        , _range_stream(make_file_input_stream(
            file,
            block_offset * block_size,
            std::min<uint64_t>(block_count, read_ahead) * block_size,
            file_input_stream_options({
                .buffer_size = block_size,
                .read_ahead = read_ahead /* TODO: maximize memory usage here */,
            })))
        , _future_blocks(std::min<uint64_t>(block_count, read_ahead))
    {
        // If the range can be stored entirely in memory, it's not necessary
        // to use the input stream.
        if (_block_count <= _read_ahead && range_stream != nullptr) {
            for (uint64_t i = 0; i < _block_count; ++i) {
                _future_blocks.push(range_stream->read_exactly(_block_size));
            }
            _in_memory = true;
        }
    }

    /// Go to the next block_range available.
    block_range_iterator
    get_next_range() {
        return block_range_iterator(_file, _block_offset + _block_count, _block_count, _block_size, _read_ahead, &_range_stream);
    }

    block_range_iterator
    get_subrange(uint64_t block_offset, uint64_t block_count) {
        assert(block_offset < _block_count && block_count <= _block_count - block_offset);
        return block_range_iterator(_file, _block_offset + block_offset, block_count, _block_size, _read_ahead, &_range_stream);
    }

    /// Split the block range iterator into n_parts.
    std::vector<block_range_iterator>
    split(uint n_parts) {
        std::vector<block_range_iterator> parts;

        // Split the current range into n_parts.
        uint64_t block_count_per_part = _block_count / n_parts;
        uint64_t block_count_remainder = _block_count % n_parts;

        for (uint i = 0; i < n_parts; i++) {
            uint64_t block_offset = _block_offset + i * block_count_per_part;
            uint64_t block_count = block_count_per_part;
            if (i < block_count_remainder) {
                block_count++;
            }
            parts.push_back(get_subrange(block_offset, block_count));
        }

        return parts;
    }

    /// Split the block range iterator into two parts. The split is done in the middle of the range.
    std::pair<block_range_iterator, block_range_iterator>
    split() {
        return {
            get_subrange(_block_offset, _block_count/2),
            get_subrange(_block_offset + _block_count/2, _block_count - _block_count/2),
        };
    }

    uint64_t size() {
        return _block_count;
    }

    bool
    has_block() {
        return _current_block < _block_count;
    }

    /// Get the current block. The future resolves immediately if the current block is already in memory.
    /// This increments the iterator.
    future<temporary_buffer<char>>
    get_block() {
        // input_stream already does buffering. However, the buffering done here is to allow in-memory
        // iterators to be used when the range size is too small.
        return !_future_blocks.empty() ? _future_blocks.pop()
                                       : _range_stream.read_exactly(_block_size);
    }

    future<output_stream<char>>
    get_output_stream() {
        co_return co_await api_v3::make_file_output_stream(_file);
    }
};

block_range_iterator
make_block_range_iterator(file &file, uint64_t block_offset, uint64_t block_count, uint64_t bytes_per_block, uint read_ahead) {
    return block_range_iterator(file, block_offset, block_count, bytes_per_block, read_ahead);
}

future<block_range_iterator>
make_block_range_iterator(file &file, uint64_t bytes_per_block = 4096, uint read_ahead = 4096) {
    co_return make_block_range_iterator(file, 0, co_await file.size() / bytes_per_block, bytes_per_block, read_ahead);
}

} // namespace seastar
