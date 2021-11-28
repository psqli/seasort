
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/temporary_buffer.hh>

#include <algorithm>
#include <execution>

#include "seasort.hh"

using namespace seastar;

using std::lexicographical_compare;

class block
{
private:
    temporary_buffer<char> _buf;

public:
    block(temporary_buffer<char>&& buf) : _buf(std::move(buf)) {}
    block(size_t size) : _buf(size) {}

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
};

class file_sort
{
    
};

int
main(int argc, char **argv)
{
    app_template app;

    distributed<file_sort> sharded_file_sort;

    app.run(argc, argv, [] () -> future<> {

    });
}
