# Aerospike Ruby Adapter for Redis
[![Gem Version](https://badge.fury.io/rb/aerospike-redis.svg)](http://badge.fury.io/rb/aerospike-redis)

Try Aerospike as a back-end replacement for Redis in Ruby applications.

The aim of this gem is to facilitate trying Aerospike in your Redis-backed Ruby application without the need to learn a new API or modifications to existing codes, as long as the Redis commands used are within the subset supported by this gem.

If you are using Redis as a cache or session store in Ruby on Rails applications, an Aerospike-backed cache and session store is already available: [Aerospike::Store](https://github.com/amirrf/aerospike-store-rails).

Aerospike offers seamless clustering and SSD-optimized data persistence. Find out more about [Aerospike](http://www.aerospike.com).

## Dependencies

- [Aerospike Ruby Client](https://github.com/aerospike/aerospike-client-ruby)
- [Redis Ruby Client](https://github.com/redis/redis-rb)

These gems will be installed automatically using `bundle`.

## Installation

### Installation from RubyGems:

    $ gem install aerospike-redis

## Usage

Specify Aerospike as the driver when creating the Redis client instance:

```ruby
require 'redis'
require 'aerospike'
require 'aerospike/redis'

redis = Redis.new(:driver => :aerospike)
```

It is possible to pass Aerospike config options here. Defaults are:

```ruby
redis = Redis.new(:driver => :aerospike,
                  :host => '127.0.0.1',
                  :port => 3000,
                  :namespace => 'test',
                  :set => 'test',
                  :bin => 'redis')
```

## How does it work

It works as a driver for Redis client, redirecting Redis commands to Aerospike instead of writing to a Redis connection. Thanks to the extensible architecture of the [Redis Ruby Client](https://github.com/redis/redis-rb) that makes this possible.

Parts of the Redis API are directly supported by the Aerospike API, while most of the functions are implemented through calling UDFs.

## Supported commands

Currently most of the Keys, Strings, and Lists commands are supported:

### Keys
`DEL`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `PERSIST`, `PEXPIRE`, `PTTL`, `TTL`

*Notes:* PEXPIRE and PTTL just convert milliseconds to seconds as Aerospike does not provide milliseconds precision to the client.

### Strings
`APPEND`, `DECR`, `DECRBY`, `GET`, `GETRANGE`, `GETSET`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `MGET`, `MSET`, `MSETNX`, `PSETEX`, `SET`, `SETEX`, `SETNX`, `SETRANGE` ,`STRLEN`

*Notes:* Implementation of `GETSET` is not atomic, it executes two separate commands from the client.

### Lists
`LINDEX`, `LINSERT`, `LLEN`, `LPOP`, `LPUSH`, `LPUSHX`, `LRANGE`, `LREM`, `LSET`, `LTRIM`, `RPOP`, `RPOPLPUSH`, `RPUSH`, `RPUSHX`

*Notes:* Implementation of `RPOPLPUSHGETSET` is not atomic, it executes two separate commands from the client.

## Testing
A test suite is provided comparing the result of calling the same function through Aerospike vs. Redis.

These tests are completely based on a subset of [Redis Ruby Client](https://github.com/redis/redis-rb) tests.

    $ bundle exec rspec

## Contributing

1. Fork it ( https://github.com/amirrf/aerospike-redis-ruby/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## License

The Aerospike-Redis-Ruby is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.
