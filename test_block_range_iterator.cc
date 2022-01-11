
#include <cstdint>
#include <string>

#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>

#include "block_range_iterator.hh"

using namespace seastar;
using std::string;

future<int>
__main(string input_filename, uint64_t block_size, uint read_ahead)
{
    file input_file = co_await open_file_dma(input_filename, open_flags::ro);
    auto bri = co_await make_block_range_iterator(input_file, block_size, read_ahead);

    while (bri.has_block()) {
        auto block = co_await bri.get_block();
    }

    co_return 0;
}

int main(int argc, char **argv) {
    app_template app;

    // Register command line options
    app.add_positional_options({
        { "input_file",  boost::program_options::value<string>(), "Input file", 1 },
        { "block_size",  boost::program_options::value<uint64_t>(), "Block size (in bytes)", 1 },
        { "read_ahead",  boost::program_options::value<uint>(), "Read ahead (in blocks)", 1 },
    });

    return app.run(argc, argv, [&app] () -> future<int> {
        auto &args = app.configuration();
        co_return co_await __main(args["input_file"].as<string>(),
                                  args["block_size"].as<uint64_t>(),
                                  args["read_ahead"].as<uint>());
    });

}
