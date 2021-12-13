
#include <algorithm>
#include <cstdint>
#include <execution>
#include <string>

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

#include "seasort.hh"
#include "seastar/core/shared_ptr.hh"

using namespace seastar;

using std::lexicographical_compare;

struct block
{
    block(temporary_buffer<char> &buf)
        : _buf(buf)
    {
        // nothing
    }

    bool operator ==(const block &other) const
    {
        return _buf == other._buf;
    }

    bool operator !=(const block &other) const
    {
        return _buf != other._buf;
    }

    bool operator <(const block &other) const
    {
        return lexicographical_compare(std::execution::seq, _buf.begin(),       _buf.end(),
                                                            other._buf.begin(), other._buf.end());
    }

    bool operator >(const block &other) const
    {
        return lexicographical_compare(std::execution::seq, other._buf.begin(), other._buf.end(),
                                                            _buf.begin(),       _buf.end());
    }

    bool operator <=(const block &other) const
    {
        return !(*this > other);
    }

    bool operator >=(const block &other) const
    {
        return !(*this < other);
    }

private:
    temporary_buffer<char> &_buf;
};


using std::string;

future<int>
__main(string input_filename, string output_filename)
{
    file input_file = co_await open_file_dma(input_filename, open_flags::ro);
    file output_file = co_await open_file_dma(output_filename, open_flags::create | open_flags::rw);

    co_return 0;
    //(co_await input_file._file_impl.get()->stat()).st_ino;
    //size_t input_file_size = co_await input_file.size();
}

int
main(int argc, char **argv)
{
    app_template app;

    // Register command line options
    app.add_positional_options({
        { "input_file",  boost::program_options::value<string>(), "Input file (unsorted)", 1 },
        { "output_file", boost::program_options::value<string>(), "Output file (sorted)",  1 },
    });

    return app.run(argc, argv, [&app] () -> future<int> {
        auto &args = app.configuration();
        int res = co_await __main(args["input_file"].as<string>(), args["output_file"].as<string>());
        co_return res;
    });

}
