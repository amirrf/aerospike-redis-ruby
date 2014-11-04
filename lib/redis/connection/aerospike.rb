require "redis/connection/registry"
require "redis/errors"
require "timeout"
require 'aerospike'

class Redis
  module Connection
    class Aerospike
      REDIS_UDF = 'redis_udf'

      def self.connect(config)
        config[:host] ||= '127.0.0.1'
        config[:port] ||= 3000
        connection = ::Aerospike::Client.new(config[:host], config[:port])

        task =  connection.register_udf_from_file(File.join(File.dirname(File.expand_path(__FILE__)), REDIS_UDF) + '.lua',
                                                  REDIS_UDF + '.lua', ::Aerospike::Language::LUA)
        task.wait_till_completed()

        #todo: support config[:timeout]

        instance = new(connection)
        instance.set_options(config)
        instance
      rescue Errno::ETIMEDOUT
        raise TimeoutError
      end

      def set_options(config)
        @namespace = config[:namespace] || 'test'
        @set = config[:set] || 'test'
        @bin = config[:bin] || 'redis'
      end

      def initialize(connection)
        @connection = connection
      end

      def connected?
        @connection && @connection.connected?
      end

      def timeout=(timeout)
        #todo: support setting timeout
      end

      def disconnect
        @connection.close
        @connection = nil
      end

      def as_key(key_name)
        return ::Aerospike::Key.new(@namespace, @set, key_name.to_s)
      end


      # EXISTS key
      # Returns if key exists.
      # Integer reply, specifically:
      #    1 if the key exists.
      #    0 if the key does not exist.
      def process_exists(command)
        key = command.first
        @connection.exists(as_key(key))? 1 : 0
      end


      # DEL key [key ...]
      # Removes the specified keys. A key is ignored if it does not exist.
      # Integer reply: The number of keys that were removed.
      def process_del(command)
        result = 0
        command.each do |key|
          result += 1 if @connection.delete(as_key(key))
        end
        result
      end

      # EXPIRE key seconds
      # Set a timeout on key. After the timeout has expired, the key will automatically be deleted. A key with an associated timeout is often said to be volatile in Redis terminology.
      # Return value
      # Integer reply, specifically:
      #     1 if the timeout was set.
      #     0 if key does not exist or the timeout could not be set.
      def process_expire(command)
        key = command[0]
        seconds = command[1]
        options = {:expiration => seconds }
        @connection.touch(as_key(key), options)? 0 : 1  # return 1 when result is OK = 0
      rescue ::Aerospike::Exceptions::Aerospike => e
        if (e.result_code == ::Aerospike::ResultCode::KEY_NOT_FOUND_ERROR)
          return 0
        else
          raise
        end
      end

      # PEXPIRE key milliseconds
      def process_pexpire(command)
        command[1] /= 1000.0
        process_expire(command)
      end

      # EXPIREAT key timestamp
      # EXPIREAT has the same effect and semantic as EXPIRE, but instead of specifying the number of seconds representing the TTL (time to live), it takes an absolute Unix timestamp (seconds since January 1, 1970).
      # Please for the specific semantics of the command refer to the documentation of EXPIRE.# Return value
      # Integer reply, specifically:
      #    1 if the timeout was set.
      #    0 if key does not exist or the timeout could not be set (see: EXPIRE).
      def process_expireat(command)
        command[1] -=  Time.now.to_i
        if command[1] > 0
          process_expire(command)
        else
          process_del([command.first])
        end
      end

      # PEXPIREAT key milliseconds-timestamp
      def process_pexpireat(command)
        command[1] /= 1000.0
        process_expireat(command)
      end

      # TTL key
      # Returns the remaining time to live of a key that has a timeout. This introspection capability allows a Redis client to check how many seconds a given key will continue to be part of the dataset.
      # In Redis 2.6 or older the command returns -1 if the key does not exist or if the key exist but has no associated expire.
      # Starting with Redis 2.8 the return value in case of error changed:
      #     The command returns -2 if the key does not exist.
      #     The command returns -1 if the key exists but has no associated expire.
      # Return value
      # Integer reply: TTL in seconds, or a negative value in order to signal an error (see the description above).
      def process_ttl(command)
        key = command.first
        (header = @connection.get_header(as_key(key)))? (header.expiration == 0xFFFFFFFF? -1 : header.expiration) : -2
      rescue ::Aerospike::Exceptions::Aerospike => e
        if (e.result_code == ::Aerospike::ResultCode::KEY_NOT_FOUND_ERROR)
          return -2
        else
          raise
        end
      end

      # PTTL key
      # Like TTL this command returns the remaining time to live of a key that has an expire set, with the sole difference that TTL returns the amount of remaining time in seconds while PTTL returns it in milliseconds
      def process_pttl(command)
        ttl = process_ttl(command)
        (ttl > 0)? ttl * 1000 : ttl
      end

      # PERSIST key
      # Remove the existing timeout on key, turning the key from volatile (a key with an expire set) to persistent (a key that will never expire as no timeout is associated).
      # Return value
      # Integer reply, specifically:
      #     1 if the timeout was removed.
      #     0 if key does not exist or does not have an associated timeout.
      def process_persist(command)
        key = command.first
        options = {:expiration => -1 }
        @connection.touch(as_key(key), options)? 0 : 1  # return 1 when result is OK = 0
      rescue ::Aerospike::Exceptions::Aerospike => e
        if (e.result_code == ::Aerospike::ResultCode::KEY_NOT_FOUND_ERROR)
          return 0
        else
          raise
        end
      end

      # TYPE key
      # Returns the string representation of the type of the value stored at key. The different types that can be returned are: string, list, set, zset and hash.
      # Return value
      # Simple string reply: type of key, or none when key does not exist.
      # def process_type(command)
      #   key = command.first
      #   if record = @connection.get(as_key(key))
      #     value = record.bins[@bin]
      #     if value.is_a? Integer
      #       @result = 'integer'
      #     else
      #       @result = nil
      #     end
      #   else
      #     @result = nil
      #   end
      # rescue ::Aerospike::Exceptions::Aerospike => e
      #   if (e.result_code == ::Aerospike::ResultCode::KEY_NOT_FOUND_ERROR)
      #     return 0
      #   else
      #     raise
      #   end
      # end



      # SET key value [EX seconds] [PX milliseconds] [NX|XX]
      # Set key to hold the string value. If key already holds a value, it is overwritten, regardless of its type. Any previous time to live associated with the key is discarded on successful SET operation.
      # Options
      #     EX seconds -- Set the specified expire time, in seconds.
      #     PX milliseconds -- Set the specified expire time, in milliseconds.
      #     NX -- Only set the key if it does not already exist.
      #     XX -- Only set the key if it already exist.
      # Simple string reply: OK if SET was executed correctly. Null reply: a Null Bulk Reply is returned if the SET operation was not performed becase the user specified the NX or XX option but the condition was not met.
      def process_set(command)
        key = command.slice!(0)
        val = command.slice!(0)

        options = {}
        while !command.empty? do
            case command.slice!(0)
            when 'EX'
              options[:expiration] = command.slice!(0)
            when 'PX'
              options[:expiration] = command.slice!(0) / 1000.0
            when 'NX'
              options[:record_exists_action] = ::Aerospike::RecordExistsAction::CREATE_ONLY
            when 'XX'
              options[:record_exists_action] = ::Aerospike::RecordExistsAction::UPDATE_ONLY
            end
          end

          @connection.put(as_key(key), {@bin => val}, options)
          return 'OK'
        rescue ::Aerospike::Exceptions::Aerospike => e
          if (e.result_code == ::Aerospike::ResultCode::KEY_NOT_FOUND_ERROR) || (e.result_code == ::Aerospike::ResultCode::KEY_EXISTS_ERROR)
            return false
          else
            raise
          end
        end

        # SETEX key seconds value
        # Set key to hold the string value and set key to timeout after a given number of seconds. This command is equivalent to executing the following commands:
        # SET mykey value
        # EXPIRE mykey seconds
        # Return value
        # Simple string reply
        def process_setex(command)
          key = command[0]
          seconds = command[1]
          val = command[2]
          options = {:expiration => seconds }
          @connection.put(as_key(key), {@bin => val}, options)
          return 'OK'
        end

        # PSETEX key milliseconds value
        # PSETEX works exactly like SETEX with the sole difference that the expire time is specified in milliseconds instead of seconds.
        def process_psetex(command)
          command[1] /= 1000.0
          process_setex(command)
        end

        # SETNX key value
        # Set key to hold string value if key does not exist. In that case, it is equal to SET. When key already holds a value, no operation is performed. SETNX is short for "SET if N ot e X ists".
        # Return value
        # Integer reply, specifically:
        #     1 if the key was set
        #     0 if the key was not set
        def process_setnx(command)
          key = command[0]
          val = command[1]
          options = {:record_exists_action => ::Aerospike::RecordExistsAction::CREATE_ONLY}
          @connection.put(as_key(key), {@bin => val}, options)
          return 1
        rescue ::Aerospike::Exceptions::Aerospike => e
          if (e.result_code == ::Aerospike::ResultCode::KEY_EXISTS_ERROR)
            return 0
          else
            raise
          end
        end

        # GET key
        # Get the value of key. If the key does not exist the special value nil is returned. An error is returned if the value stored at key is not a string, because GET only handles string values.
        # Return value
        # Bulk string reply: the value of key, or nil when key does not exist.
        def process_get(command)
          key = command.first
          if record = @connection.get(as_key(key))
            value = record.bins[@bin]
          else
            value = nil
          end
          value
        rescue ::Aerospike::Exceptions::Aerospike => e
          #          puts e.inspect
          if (e.result_code == ::Aerospike::ResultCode::KEY_NOT_FOUND_ERROR)
            return false
          else
            raise
          end
        end

        # APPEND key value
        # If key already exists and is a string, this command appends the value at the end of the string. If key does not exist it is created and set as an empty string, so APPEND will be similar to SET in this special case.
        # Integer reply: the length of the string after the append operation.
        def process_append(command)
          key = command.first
          val = command[1]
          @connection.append(as_key(key), {@bin => val})

          @connection.execute_udf(as_key(key), REDIS_UDF, 'strlen', [::Aerospike::StringValue.new(@bin)])
        rescue ::Aerospike::Exceptions::Aerospike => e
          if (e.result_code == ::Aerospike::ResultCode::KEY_EXISTS_ERROR)
            return 0
          else
            raise
          end
        end

        # STRLEN key
        # Returns the length of the string value stored at key. An error is returned when key holds a non-string value.
        # Return value
        # Integer reply: the length of the string at key, or 0 when key does not exist.
        def process_strlen(command)
          key = command.first
          @connection.execute_udf(as_key(key), REDIS_UDF, 'strlen', [::Aerospike::StringValue.new(@bin)])
        rescue ::Aerospike::Exceptions::Aerospike => e
          if (e.result_code == ::Aerospike::ResultCode::KEY_NOT_FOUND_ERROR)
            return 0
          else
            raise
          end
        end

        # GETRANGE key start end
        # Warning: this command was renamed to GETRANGE, it is called SUBSTR in Redis versions <= 2.0.
        # Returns the substring of the string value stored at key, determined by the offsets start and end (both are inclusive). Negative offsets can be used in order to provide an offset starting from the end of the string. So -1 means the last character, -2 the penultimate and so forth.
        # The function handles out of range requests by limiting the resulting range to the actual length of the string.
        # Return value
        # Bulk string reply
        def process_getrange(command)
          key = command.first
          @connection.execute_udf(as_key(key), REDIS_UDF, 'getrange',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::IntegerValue.new(command[1]),
                                   ::Aerospike::IntegerValue.new(command[2])
                                   ])
        end

        # GETSET key value
        # Atomically sets key to value and returns the old value stored at key. Returns an error when key exists but does not hold a string value.
        def process_getset(command)
          key = command.first
          val = command[1]

          if record = @connection.get(as_key(key))
            old_value = record.bins[@bin]
          else
            old_value = nil
          end
          @connection.put(as_key(key), {@bin => val})
          old_value
        end



        # INCR key
        # Increments the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer. This operation is limited to 64 bit signed integers.
        # Return value
        # Integer reply: the value of key after the increment
        def perform_add(key_name, add_value)
          # bin_int = ::Aerospike::Bin.new(@bin, add_value)
          # rec = @connection.operate(as_key(key_name), [
          #                        ::Aerospike::Operation.add(bin_int),
          #                        ::Aerospike::Operation.get(bin_int.name)
          #   ])
          # rec.bins[@bin]
          #
          # the above method does not work since add does not work on string bins! Error:  Bin type error
          @connection.execute_udf(as_key(key_name), REDIS_UDF, 'add',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::IntegerValue.new(add_value)
                                   ])
        end

        def process_incr(command)
          perform_add(command.first, +1)
        end

        # INCRBY key increment
        # Increments the number stored at key by increment. If the key does not exist, it is set to 0 before performing the operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer. This operation is limited to 64 bit signed integers.
        # Return value
        # Integer reply: the value of key after the increment
        def process_incrby(command)
          perform_add(command.first, command[1])
        end

        def process_decr(command)
          perform_add(command.first, -1)
        end

        def process_decrby(command)
          perform_add(command.first, -1 * command[1])
        end

        # INCRBYFLOAT key increment
        # Increment the string representing a floating point number stored at key by the specified increment. If the key does not exist, it is set to 0 before performing the operation. An error is returned if one of the following conditions occur:
        #     The key contains a value of the wrong type (not a string).
        #     The current key content or the specified increment are not parsable as a double precision floating point number.
        # If the command is successful the new incremented value is stored as the new value of the key (replacing the old one), and returned to the caller as a string.
        # Both the value already contained in the string key and the increment argument can be optionally provided in exponential notation, however the value computed after the increment is stored consistently in the same format, that is, an integer number followed (if needed) by a dot, and a variable number of digits representing the decimal part of the number. Trailing zeroes are always removed.
        # The precision of the output is fixed at 17 digits after the decimal point regardless of the actual internal precision of the computation.
        # Return value
        # Bulk string reply: the value of key after the increment.
        def process_incrbyfloat(command)
          key = command.first
          add_value = command[1]

          @connection.execute_udf(as_key(key), REDIS_UDF, 'incrbyfloat',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::StringValue.new(add_value.to_s)
                                   ]).to_s
        end


        # SETRANGE key offset value
        # Overwrites part of the string stored at key, starting at the specified offset, for the entire length of value. If the offset is larger than the current length of the string at key, the string is padded with zero-bytes to make offset fit. Non-existing keys are considered as empty strings, so this command will make sure it holds a string large enough to be able to set value at offset.
        # Return value
        # Integer reply: the length of the string after it was modified by the command.
        def process_setrange(command)
          key = command.first
          offset = command[1]
          value = command[2]

          @connection.execute_udf(as_key(key), REDIS_UDF, 'setrange',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::IntegerValue.new(offset),
                                   ::Aerospike::StringValue.new(value)
                                   ])
          # CAUTION: zero-bytes mentioned in function description is replaced with spaces here
        end



        # MGET key [key ...]
        # Returns the values of all specified keys. For every key that does not hold a string value or does not exist, the special value nil is returned. Because of this, the operation never fails.
        # Return value
        # Array reply: list of values at the specified keys.
        def process_mget(command)
          keys = command.map { |key| as_key(key) }
          records = @connection.batch_get(keys, [@bin])
          records.map { |rec| rec.nil?? nil : rec.bins[@bin] }
        end

        # MSET key value [key value ...]
        # Sets the given keys to their respective values. MSET replaces existing values with new values, just as regular SET. See MSETNX if you don't want to overwrite existing values.
        # MSET is atomic, so all given keys are set at once. It is not possible for clients to see that some of the keys were updated while others are unchanged.
        # Return value
        # Simple string reply: always OK since MSET can't fail.
        def process_mset(command)
          command.each_slice(2) { |kv|
            key = kv[0]
            val = kv[1]
            begin
              @connection.put(as_key(key), {@bin => val})
            rescue ::Aerospike::Exceptions::Aerospike
              # ignore and continue to set other keys
            end
          }
          return 'OK'
        end

        # MSETNX key value [key value ...]
        # Sets the given keys to their respective values. MSETNX will not perform any operation at all even if just a single key already exists.
        # Because of this semantic MSETNX can be used in order to set different keys representing different fields of an unique logic object in a way that ensures that either all the fields or none at all are set.
        # MSETNX is atomic, so all given keys are set at once. It is not possible for clients to see that some of the keys were updated while others are unchanged.
        # Return value
        # Integer reply, specifically:
        #     1 if the all the keys were set.
        #     0 if no key was set (at least one key already existed).
        def process_msetnx(command)
          keys = command.select.each_with_index { |str, i| i.even? }
          if @connection.batch_exists(keys.map{ |key| as_key(key) }).reduce{|r,e| r || e}
            return 0
          else
            command.each_slice(2) { |kv|
              key = kv[0]
              val = kv[1]
              @connection.put(as_key(key), {@bin => val})
            }
            return 1
          end
        end


        # LIST function ##############################################

        # LPUSH key value [value ...]
        # Insert all the specified values at the head of the list stored at key. If key does not exist, it is created as empty list before performing the push operations. When key holds a value that is not a list, an error is returned.
        # It is possible to push multiple elements using a single command call just specifying multiple arguments at the end of the command. Elements are inserted one after the other to the head of the list, from the leftmost element to the rightmost element. So for instance the command LPUSH mylist a b c will result into a list containing c as first element, b as second element and a as third element.
        # Return value
        # Integer reply: the length of the list after the push operations.
        def process_lpush(command)
          key = command.slice!(0)
          @connection.execute_udf(as_key(key), REDIS_UDF, 'lpush',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::ListValue.new(command)
                                   ])
        end

        # LPUSHX key value
        # Inserts value at the head of the list stored at key, only if key already exists and holds a list. In contrary to LPUSH, no operation will be performed when key does not yet exist.
        # Return value
        # Integer reply: the length of the list after the push operation.
        def process_lpushx(command)
          key = command.first
          val = command[1]
          @connection.execute_udf(as_key(key), REDIS_UDF, 'lpushx',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::StringValue.new(val)
                                   ])
        end

        # LPOP key
        # Removes and returns the first element of the list stored at key.
        # Return value
        # Bulk string reply: the value of the first element, or nil when key does not exist.
        def process_lpop(command)
          key = command.first
          @connection.execute_udf(as_key(key), REDIS_UDF, 'lpop', [::Aerospike::StringValue.new(@bin)])
        end


        # RPUSH key value [value ...]
        def process_rpush(command)
          key = command.slice!(0)
          @connection.execute_udf(as_key(key), REDIS_UDF, 'rpush',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::ListValue.new(command)
                                   ])
        end

        # RPUSHX key value
        def process_rpushx(command)
          key = command.first
          val = command[1]
          @connection.execute_udf(as_key(key), REDIS_UDF, 'rpushx',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::StringValue.new(val)
                                   ])
        end

        # RPOP key
        def process_rpop(command)
          key = command.first
          @connection.execute_udf(as_key(key), REDIS_UDF, 'rpop',[::Aerospike::StringValue.new(@bin)])
        end

        # RPOPLPUSH source destination
        def process_rpoplpush(command)
          key1 = command.first
          key2 = command[1]
          value = @connection.execute_udf(as_key(key1), REDIS_UDF, 'rpop', [::Aerospike::StringValue.new(@bin)])
          if (value != nil) then
            @connection.execute_udf(as_key(key2), REDIS_UDF, 'lpush',
                                    [::Aerospike::StringValue.new(@bin),
                                     ::Aerospike::ListValue.new([value])
                                     ])
          end
          value
        end

        # LLEN key
        # Returns the length of the list stored at key. If key does not exist, it is interpreted as an empty list and 0 is returned. An error is returned when the value stored at key is not a list.
        # Return value
        # Integer reply: the length of the list at key.
        def process_llen(command)
          key = command.first
          @connection.execute_udf(as_key(key), REDIS_UDF, 'llen', [::Aerospike::StringValue.new(@bin)])
        end

        # LSET key index value
        # Sets the list element at index to value. For more information on the index argument, see LINDEX.
        # An error is returned for out of range indexes.
        # Return value
        # Simple string reply
        def process_lset(command)
          key = command.first
          index = command[1]
          val = command[2]
          @connection.execute_udf(as_key(key), REDIS_UDF, 'lset',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::IntegerValue.new(index),
                                   ::Aerospike::StringValue.new(val)
                                   ])
        end

        # LINDEX key index
        def process_lindex(command)
          key = command.first
          index = command[1]
          @connection.execute_udf(as_key(key), REDIS_UDF, 'lindex',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::IntegerValue.new(index)
                                   ])
        end

        # LINSERT key BEFORE|AFTER pivot value
        def process_linsert(command)
          key = command.first
          placement = command[1].to_s
          pivot = command[2]
          value = command[3]

          @connection.execute_udf(as_key(key), REDIS_UDF, 'linsert',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::StringValue.new(placement),
                                   ::Aerospike::StringValue.new(pivot),
                                   ::Aerospike::StringValue.new(value)
                                   ])
        end

        def process_lrange(command)
          key = command.first
          start = command[1]
          stop = command[2]

          @connection.execute_udf(as_key(key), REDIS_UDF, 'lrange',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::IntegerValue.new(start),
                                   ::Aerospike::IntegerValue.new(stop)
                                   ])
        end

        # LREM key count value
        def process_lrem(command)
          key = command.first
          count = command[1]
          value = command[2]

          @connection.execute_udf(as_key(key), REDIS_UDF, 'lrem',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::IntegerValue.new(count),
                                   ::Aerospike::StringValue.new(value)
                                   ])
        end

        # LTRIM key start stop
        def process_ltrim(command)
          key = command.first
          start = command[1]
          stop = command[2]

          @connection.execute_udf(as_key(key), REDIS_UDF, 'ltrim',
                                  [::Aerospike::StringValue.new(@bin),
                                   ::Aerospike::IntegerValue.new(start),
                                   ::Aerospike::IntegerValue.new(stop)
                                   ])
        end

       def write(command)
          #        @connection.write(command.flatten(1))
          command = command.flatten(1)

          # puts "write command: " + command.inspect

          cmd = command.slice!(0)
          # puts "command: " + cmd.to_s
          # puts 'params: ' + command.to_s

          method_name = 'process_' + cmd.to_s
          if respond_to?(method_name)
            @result = send(method_name, command)
          else
            raise "command not supported\n" + command.inspect
          end
          # puts "result was: " + @result.to_s
          #        return @result

        rescue Errno::EAGAIN
          raise TimeoutError
        end  # wrtie

        def read
          #        reply = @connection.read
          #        reply = CommandError.new(reply.message) if reply.is_a?(RuntimeError)
          #        reply

          reply =  @result
          @result = nil
          return reply

          #puts  "it is reading!"
        rescue Errno::EAGAIN
          raise TimeoutError
        rescue RuntimeError => err
          raise ProtocolError.new(err.message)
        end # read

    end # class AerospikeRedis
  end # module Connection
end # class Redis

Redis::Connection.drivers << Redis::Connection::Aerospike