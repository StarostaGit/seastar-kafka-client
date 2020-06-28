# seastar-kafka-client
A Kafka client library for Seastar applications

## Building
To build seastar-kafka-client as a standalone library simply run the following commands:
```bash
$ mkdir build/release
$ cd build/release
$ cmake -DCMAKE_BUILD_TYPE=Release ../..
$ make
```

Additionally, if you want to install the library, run `sudo make install`.

Seastar-kafka-client uses Seastar and will try to find it installed on the machine. 
If you wnat to provide the path manually you can set the following cmake cached variables:
* `CMAKE_PATH_PREFIX` to `/path/to/seastar/build/release`
* `CMAKE_MODULE_PREFIX` to `/path/to/seastar/cmake`

## Usage
Example usage of the producer client is shown in `demo/kafka_demo.cc`. Generally, it should
look like this:
```cpp
#include <kafka4seastar/producer/kafka_producer>

namespace k4s = kafka4seastar;

// setup the producer
k4s::producer_properties properties;
properties.client_id = "client-id";

// list of servers to establish a starting connection with
properties.servers = {
        {host, port} // for example {"172.13.0.1", 9092}
};

k4s::kafka_producer producer(std::move(properties));
producer.init().wait();

// scheduling the message for production
producer.produce("topic", "key", "value");

// ending work with producer
producer.disconnect().wait();
```
