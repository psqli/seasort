
// 2020-11-26
//
// Sort blocks in a file.
// a block-merge sort distributed across shards.
// 

#include "seasort.hh"

namespace seastar {

template <typename T>
future<int>
seasort<T>::sort_file(input_stream<char> unsorted_file, output_stream<char> sorted_file)
{
    
}

}