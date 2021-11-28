
// 2020-11-26
//
// Sort blocks in a file.
// se a block-merge sort distributed across shards.
//

#include <seastar/core/iostream.hh>

using seastar::input_stream, seastar::output_stream;

namespace seastar {

    template<typename T>
    class seasort {
    public:
        future<int>
        sort_file(input_stream<char> unsorted_file, output_stream<char> sorted_file);
    };
};
