#include <seastar/core/future.hh>
#include <iostream>

int main() {
    auto f = seastar::make_ready_future<>().then([] () {
       std::cout << "hej\n";
       return seastar::make_ready_future<>();
    });
    return 0;
}