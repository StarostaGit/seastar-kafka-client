#include <seastar/core/future.hh>
#include <iostream>

#include <kafka4seastar/protocol/metadata_request.hh>

int main() {
    namespace k4s = kafka4seastar;

    auto f = seastar::make_ready_future<>().then([] () {
       std::cout << "hej\n";
       return seastar::make_ready_future<>();
    });

    auto r = k4s::metadata_request();

    return 0;
}