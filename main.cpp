#include <seastar/core/future.hh>
#include <iostream>

#include <kafka4seastar/protocol/metadata_request.hh>
#include <kafka4seastar/connection/connection_manager.hh>
#include <kafka4seastar/utils/metadata_manager.hh>

int main() {
    namespace k4s = kafka4seastar;

    auto f = seastar::make_ready_future<>().then([] () {
       std::cout << "hej\n";
       return seastar::make_ready_future<>();
    });

    auto r = k4s::metadata_request();
    
    k4s::connection_manager m {"manager"};

    return 0;
}