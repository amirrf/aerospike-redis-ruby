# Copyright (c) 2014 Amir Rahimi Farahani
# 
# All tests are based on tests provided in Redis Ruby Client (https://github.com/redis/redis-rb)
# with the following license:
#   ######################################################################
#   Copyright (c) 2009 Ezra Zygmuntowicz
#   Permission is hereby granted, free of charge, to any person obtaining
#   a copy of this software and associated documentation files (the
#   "Software"), to deal in the Software without restriction, including
#   without limitation the rights to use, copy, modify, merge, publish,
#   distribute, sublicense, and/or sell copies of the Software, and to
#   permit persons to whom the Software is furnished to do so, subject to
#   the following conditions:
#   The above copyright notice and this permission notice shall be
#   included in all copies or substantial portions of the Software.
#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
#   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
#   LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
#   OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
#   WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#   ######################################################################

require "spec_helper"

describe "Aerospike driver for Redis - Lists" do

  let(:r) do
    Redis.new(:host => "127.0.0.1", :port => 6379, :driver => :ruby)
  end

  let(:a) do
    Redis.new(:host => "127.0.0.1", :port => 3000, :driver => :aerospike)
  end

  before :each do
    a.del('foo', 'bar', 'baz')
    r.del('foo', 'bar', 'baz')
  end

  after do
    # r.quit
    # a.quit
  end

  it '#lpush' do
    expect(a.lpush("foo", "s1")).to eq r.lpush("foo", "s1")
    expect(a.lpush("foo", "s2")).to eq r.lpush("foo", "s2")
    expect(a.llen("foo")).to eq r.llen("foo")

    expect(a.lpop("foo")).to eq r.lpop("foo")
  end

  it '#variadic lpush' do
    expect(a.lpush("foo", ["s1", "s2", "s3"])).to eq r.lpush("foo", ["s1", "s2", "s3"])

    expect(a.llen("foo")).to eq r.llen("foo")
    expect(a.lpop("foo")).to eq r.lpop("foo")
  end

  it '#lpushx' do
    expect(a.lpushx("foo", "s1")).to eq r.lpushx("foo", "s1")
    expect(a.lpush("foo", "s2")).to eq r.lpush("foo", "s2")
    expect(a.lpushx("foo", "s3")).to eq r.lpushx("foo", "s3")

    expect(a.llen("foo")).to eq r.llen("foo")
    expect(a.lrange("foo", 0, -1)).to eq r.lrange("foo", 0, -1)
  end

  it '#rpush' do
    expect(a.rpush("foo", "s1")).to eq r.rpush("foo", "s1")
    expect(a.rpush("foo", "s2")).to eq r.rpush("foo", "s2")

    expect(a.llen("foo")).to eq r.llen("foo")
    expect(a.rpop("foo")).to eq r.rpop("foo")
  end

  it '#variadic rpush' do
    expect(a.rpush("foo", ["s1", "s2", "s3"])).to eq r.rpush("foo", ["s1", "s2", "s3"])

    expect(a.llen("foo")).to eq r.llen("foo")
    expect(a.rpop("foo")).to eq r.rpop("foo")
  end

  it '#rpushx' do
    expect(a.rpushx("foo", "s1")).to eq r.rpushx("foo", "s1")
    expect(a.rpush("foo", "s2")).to eq r.rpush("foo", "s2")
    expect(a.rpushx("foo", "s3")).to eq r.rpushx("foo", "s3")

    expect(a.llen("foo")).to eq r.llen("foo")
    expect(a.lrange("foo", 0, -1)).to eq r.lrange("foo", 0, -1)
  end

  it '#llen' do
    expect(a.rpush("foo", "s1")).to eq r.rpush("foo", "s1")
    expect(a.rpush("foo", "s2")).to eq r.rpush("foo", "s2")

    expect(a.llen("foo")).to eq r.llen("foo")
  end

  it '#lrange' do
    expect(a.rpush("foo", "s1")).to eq r.rpush("foo", "s1")
    expect(a.rpush("foo", "s2")).to eq r.rpush("foo", "s2")
    expect(a.rpush("foo", "s3")).to eq r.rpush("foo", "s3")

    expect(a.lrange("foo", 1, -1)).to eq r.lrange("foo", 1, -1)
    expect(a.lrange("foo", 0, 1)).to eq r.lrange("foo", 0, 1)

    expect(a.lrange("bar", 0, -1)).to eq r.lrange("bar", 0, -1)
  end

  it '#ltrim' do
    expect(a.rpush("foo", "s1")).to eq r.rpush("foo", "s1")
    expect(a.rpush("foo", "s2")).to eq r.rpush("foo", "s2")
    expect(a.rpush("foo", "s3")).to eq r.rpush("foo", "s3")

    expect(a.ltrim("foo", 0, 1)).to eq r.ltrim("foo", 0, 1)

    expect(a.llen("foo")).to eq r.llen("foo")
    expect(a.lrange("bar", 0, -1)).to eq r.lrange("bar", 0, -1)
  end

  it '#lindex' do
    expect(a.rpush("foo", "s1")).to eq r.rpush("foo", "s1")
    expect(a.rpush("foo", "s2")).to eq r.rpush("foo", "s2")

    expect(a.lindex("foo", 0)).to eq r.lindex("foo", 0)
    expect(a.lindex("foo", 1)).to eq r.lindex("foo", 1)
  end

  it '#lset' do
    expect(a.rpush("foo", "s1")).to eq r.rpush("foo", "s1")
    expect(a.rpush("foo", "s2")).to eq r.rpush("foo", "s2")

    expect(a.lindex("foo", 1)).to eq r.lindex("foo", 1)
    expect(a.lset("foo", 1, "s3")).to eq r.lset("foo", 1, "s3")
    expect(a.lindex("foo", 1)).to eq r.lindex("foo", 1)

    # assert_raise Redis::CommandError do
    #   r.lset("foo", 4, "s3")
    # end
  end

  it '#lrem' do
    expect(a.rpush("foo", "s1")).to eq r.rpush("foo", "s1")
    expect(a.rpush("foo", "s2")).to eq r.rpush("foo", "s2")

    expect(a.lrem("foo", 1, "s1")).to eq r.lrem("foo", 1, "s1")
    expect(a.lrange("foo", 0, -1)).to eq r.lrange("foo", 0, -1)
  end

  it '#lpop' do
    expect(a.rpush("foo", "s1")).to eq r.rpush("foo", "s1")
    expect(a.rpush("foo", "s2")).to eq r.rpush("foo", "s2")

    expect(a.llen("foo")).to eq r.llen("foo")
    expect(a.lpop("foo")).to eq r.lpop("foo")
    expect(a.llen("foo")).to eq r.llen("foo")
  end

  it '#rpop' do
    expect(a.rpush("foo", "s1")).to eq r.rpush("foo", "s1")
    expect(a.rpush("foo", "s2")).to eq r.rpush("foo", "s2")

    expect(a.llen("foo")).to eq r.llen("foo")
    expect(a.rpop("foo")).to eq r.rpop("foo")
    expect(a.llen("foo")).to eq r.llen("foo")
  end

  it '#linsert' do
    expect(a.rpush("foo", "s1")).to eq r.rpush("foo", "s1")
    expect(a.rpush("foo", "s2")).to eq r.rpush("foo", "s2")
    expect(a.linsert("foo", :before, "s3", "s2")).to eq r.linsert "foo", :before, "s3", "s2"

    # added: do a working linsert. the original test above fails to find "s3" which does not exist
    expect(a.linsert("foo", :before, "s2", "s3")).to eq r.linsert "foo", :before, "s2", "s3"

    expect(a.lrange("foo", 0, -1)).to eq r.lrange("foo", 0, -1)

    # assert_raise(Redis::CommandError) do
    #   r.linsert "foo", :anywhere, "s3", "s2"
    # end
  end

  it '#rpoplpush' do
    expect(a.rpush("foo", "s1")).to eq r.rpush("foo", "s1")
    expect(a.rpush("foo", "s2")).to eq r.rpush("foo", "s2")

    expect(a.rpoplpush("foo", "bar")).to eq r.rpoplpush("foo", "bar")
    expect(a.lrange("bar", 0, -1)).to eq r.lrange("bar", 0, -1)
    expect(a.rpoplpush("foo", "bar")).to eq r.rpoplpush("foo", "bar")
    expect(a.lrange("bar", 0, -1)).to eq r.lrange("bar", 0, -1)
  end

end # describe
