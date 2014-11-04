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

describe "Aerospike driver for Redis - ValueTypes" do

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

  it '#exists' do
    expect(a.exists("foo")).to eq r.exists("foo")

    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")

    expect(a.exists("foo")).to eq r.exists("foo")
  end

  # def test_type
  #   assert_equal "none", r.type("foo")

  #   r.set("foo", "s1")

  #   assert_equal "string", r.type("foo")
  # end

  # def test_keys
  #   r.set("f", "s1")
  #   r.set("fo", "s2")
  #   r.set("foo", "s3")

  #   assert_equal ["f","fo", "foo"], r.keys("f*").sort
  # end

  it '#expire' do
    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")
    expect(a.expire("foo", 2)).to eq r.expire("foo", 2)
    expect(a.ttl("foo")).to be_between(0, 2).inclusive
  end

  it '#pexpire' do
    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")
    expect(a.pexpire("foo", 2000)).to eq r.pexpire("foo", 2000)
    expect(a.ttl("foo")).to be_between(0, 2).inclusive
  end

  it '#expireat' do
    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")
    expect(a.expireat("foo", (Time.now + 2).to_i)).to eq r.expireat("foo", (Time.now + 2).to_i)
    expect(a.ttl("foo")).to be_between(0, 2).inclusive
  end

  it '#pexpireat' do
    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")
    expect(a.pexpireat("foo", (Time.now + 2).to_i * 1_000)).to eq r.pexpireat("foo", (Time.now + 2).to_i * 1_000)
    expect(a.ttl("foo")).to be_between(0, 2).inclusive
  end

  it '#persist' do
    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")
    expect(a.expire("foo", 1)).to eq r.expire("foo", 1)
    expect(a.persist("foo")).to eq r.persist("foo")

    expect(a.ttl("foo")).to eq r.ttl("foo")
  end

  it '#ttl' do
    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")
    expect(a.expire("foo", 2)).to eq r.expire("foo", 2)
    expect(a.ttl("foo")).to be_between(0, 2).inclusive
  end

  it '#pttl' do
    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")
    expect(a.expire("foo", 2)).to eq r.expire("foo", 2)
    expect(a.pttl("foo")).to be_between(1, 2000).inclusive
  end

  # def test_dump_and_restore
  #   target_version "2.5.7" do
  #     r.set("foo", "a")
  #     v = r.dump("foo")
  #     r.del("foo")

  #     assert r.restore("foo", 1000, v)
  #     assert_equal "a", r.get("foo")
  #     assert [0, 1].include? r.ttl("foo")

  #     r.rpush("bar", ["b", "c", "d"])
  #     w = r.dump("bar")
  #     r.del("bar")

  #     assert r.restore("bar", 1000, w)
  #     assert_equal ["b", "c", "d"], r.lrange("bar", 0, -1)
  #     assert [0, 1].include? r.ttl("bar")
  #   end
  # end

  # def test_move
  #   r.select 14
  #   r.flushdb

  #   r.set "bar", "s3"

  #   r.select 15

  #   r.set "foo", "s1"
  #   r.set "bar", "s2"

  #   assert r.move("foo", 14)
  #   assert_equal nil, r.get("foo")

  #   assert !r.move("bar", 14)
  #   assert_equal "s2", r.get("bar")

  #   r.select 14

  #   assert_equal "s1", r.get("foo")
  #   assert_equal "s3", r.get("bar")
  # end

end # describe
