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

describe "Aerospike driver for Redis - Strings" do

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

  it "#set and #get" do
    expect(a.set("foo", "s1")).to eq(r.set("foo", "s1"))
    expect(a.get("foo")).to eq (r.get("foo"))
  end

  it "#set and #get with brackets" do
    expect(a["foo"] = "s1").to eq(r["foo"] = "s1")
    expect(a["foo"]).to eq (r["foo"])
  end

  it "#set and #get with brackets and symbol" do
    expect(a[:foo] = "s1").to eq(r[:foo] = "s1")
    expect(a[:foo]).to eq (r[:foo])
  end

  it '#set and #get with newline characters' do
    expect(a.set("foo", "1\n")).to eq(r.set("foo", "1\n"))
    expect(a.get("foo")).to eq (r.get("foo"))
  end

  it '#set and #get with non string value' do
    value = ["a", "b"]

    expect(a.set("foo", value)).to eq(r.set("foo", value))
    expect(a.get("foo")).to eq (r.get("foo"))
  end

  # def test_set_and_get_with_ascii_characters
  #   if defined?(Encoding)
  #     with_external_encoding("ASCII-8BIT") do
  #       (0..255).each do |i|
  #         str = "#{i.chr}---#{i.chr}"
  #         r.set("foo", str)

  #         assert_equal str, r.get("foo")
  #       end
  #     end
  #   end
  # end

  it '#set with ex' do
    expect(a.set("foo", "bar", :ex => 2)).to eq r.set("foo", "bar", :ex => 2)
    expect(a.ttl("foo")).to be_between(0, 2).inclusive
  end

  it '#set with px' do
    expect(a.set("foo", "bar", :px => 2000)).to eq r.set("foo", "bar", :px => 2000)
    expect(a.ttl("foo")).to be_between(0, 2).inclusive
  end

  it '#set with nx' do
    expect(a.set("foo", "qux", :nx => true)).to eq r.set("foo", "qux", :nx => true)
    expect(a.set("foo", "bar", :nx => true)).to eq r.set("foo", "bar", :nx => true)
    expect(a.get("foo")).to eq (r.get("foo"))

    expect(a.del("foo")).to eq (r.del("foo"))
    expect(a.set("foo", "bar", :nx => true)).to eq r.set("foo", "bar", :nx => true)
    expect(a.get("foo")).to eq (r.get("foo"))
  end

  it '#set with xx' do
    expect(a.set("foo", "qux")).to eq r.set("foo", "qux")
    expect(a.set("foo", "qux", :xx => true)).to eq r.set("foo", "qux", :xx => true)
    expect(a.get("foo")).to eq (r.get("foo"))

    expect(a.del("foo")).to eq (r.del("foo"))
    expect(a.set("foo", "qux", :xx => true)).to eq r.set("foo", "qux", :xx => true)
  end

  it '#setex' do
    expect(a.setex("foo", 1, "bar")).to eq r.setex("foo", 1, "bar")
    expect(a.get("foo")).to eq (r.get("foo"))
    expect(a.ttl("foo")).to be_between(0, 1).inclusive
  end

  it '#setex with non string value' do
    value = ["b", "a", "r"]

    expect(a.setex("foo", 1, value)).to eq r.setex("foo", 1, value)
    expect(a.get("foo")).to eq (r.get("foo"))
    expect(a.ttl("foo")).to be_between(0, 1).inclusive
  end

  it '#psetex' do
    expect(a.psetex("foo", 1000, "bar")).to eq r.psetex("foo", 1000, "bar")
    expect(a.get("foo")).to eq (r.get("foo"))
    expect(a.ttl("foo")).to be_between(0, 1).inclusive
  end

  it '#psetex with non string value' do
    value = ["b", "a", "r"]

    expect(a.psetex("foo", 1000, value)).to eq r.psetex("foo", 1000, value)
    expect(a.get("foo")).to eq (r.get("foo"))
    expect(a.ttl("foo")).to be_between(0, 1).inclusive
  end

  it '#getset' do
    expect(a.set("foo", "bar")).to eq r.set("foo", "bar")
    expect(a.getset("foo", "baz")).to eq r.getset("foo", "baz")
    expect(a.get("foo")).to eq (r.get("foo"))
  end

  it '#getset with non string value' do
    expect(a.set("foo", "zap")).to eq r.set("foo", "zap")

    value = ["b", "a", "r"]

    expect(a.getset("foo", value)).to eq r.getset("foo", value)
    expect(a.get("foo")).to eq (r.get("foo"))
  end

  it '#setnx' do
    expect(a.set("foo", "qux")).to eq r.set("foo", "qux")
    expect(a.setnx("foo", "bar")).to eq r.setnx("foo", "bar")
    expect(a.get("foo")).to eq (r.get("foo"))

    expect(a.del("foo")).to eq (r.del("foo"))
    expect(a.setnx("foo", "bar")).to eq r.setnx("foo", "bar")
    expect(a.get("foo")).to eq (r.get("foo"))
  end

  it '#setnx with non string value' do
    value = ["b", "a", "r"]

    expect(a.set("foo", "qux")).to eq r.set("foo", "qux")
    expect(a.setnx("foo", value)).to eq r.setnx("foo", value)
    expect(a.get("foo")).to eq (r.get("foo"))

    expect(a.del("foo")).to eq (r.del("foo"))
    expect(a.setnx("foo", value)).to eq r.setnx("foo", value)
    expect(a.get("foo")).to eq (r.get("foo"))
  end

  it '#incr' do
    expect(a.incr("foo")).to eq r.incr("foo")
    expect(a.incr("foo")).to eq r.incr("foo")
    expect(a.incr("foo")).to eq r.incr("foo")
  end

  it '#incrby' do
    expect(a.incrby("foo", 1)).to eq r.incrby("foo", 1)
    expect(a.incrby("foo", 2)).to eq r.incrby("foo", 2)
    expect(a.incrby("foo", 3)).to eq r.incrby("foo", 3)
  end

  it '#incrbyfloat' do
    expect(a.incrbyfloat("foo", 1.23)).to eq r.incrbyfloat("foo", 1.23)
    expect(a.incrbyfloat("foo", 0.77)).to eq r.incrbyfloat("foo", 0.77)
    expect(a.incrbyfloat("foo", -0.1)).to eq r.incrbyfloat("foo", -0.1)
  end

  it '#decr' do
    expect(a.set("foo", 3)).to eq r.set("foo", 3)

    expect(a.decr("foo")).to eq r.decr("foo")
    expect(a.decr("foo")).to eq r.decr("foo")
    expect(a.decr("foo")).to eq r.decr("foo")
  end

  it '#decrby' do
    expect(a.set("foo", 6)).to eq r.set("foo", 6)

    expect(a.decrby("foo", 3)).to eq r.decrby("foo", 3)
    expect(a.decrby("foo", 2)).to eq r.decrby("foo", 2)
    expect(a.decrby("foo", 1)).to eq r.decrby("foo", 1)
  end

  it '#append' do
    expect(a.set("foo", "s")).to eq r.set("foo", "s")
    expect(a.append "foo", "1").to eq r.append "foo", "1"
    expect(a.get("foo")).to eq r.get("foo")
  end

  # def test_getbit
  #   r.set("foo", "a")

  #   assert_equal 1, r.getbit("foo", 1)
  #   assert_equal 1, r.getbit("foo", 2)
  #   assert_equal 0, r.getbit("foo", 3)
  #   assert_equal 0, r.getbit("foo", 4)
  #   assert_equal 0, r.getbit("foo", 5)
  #   assert_equal 0, r.getbit("foo", 6)
  #   assert_equal 1, r.getbit("foo", 7)
  # end

  # def test_setbit
  #   r.set("foo", "a")

  #   r.setbit("foo", 6, 1)

  #   assert_equal "c", r.get("foo")
  # end

  # def test_bitcount
  #   target_version "2.5.10" do
  #     r.set("foo", "abcde")

  #     assert_equal 10, r.bitcount("foo", 1, 3)
  #     assert_equal 17, r.bitcount("foo", 0, -1)
  #   end
  # end

  it '#getrange' do
    expect(a.set("foo", "abcde")).to eq r.set("foo", "abcde")
    expect(r.getrange("foo", 1, 3)).to eq r.getrange("foo", 1, 3)
    expect(r.getrange("foo", 0, -1)).to eq r.getrange("foo", 0, -1)
  end

  it '#setrange' do
    expect(a.set("foo", "abcde")).to eq r.set("foo", "abcde")
    expect(a.setrange("foo", 1, "bar")).to eq r.setrange("foo", 1, "bar")
    expect(a.get("foo")).to eq r.get("foo")
  end

  it '#setrange with non string value' do
    expect(a.set("foo", "abcde")).to eq r.set("foo", "abcde")
    value = ["b", "a", "r"]
    expect(a.setrange("foo", 2, value)).to eq r.setrange("foo", 2, value)
    expect(a.get("foo")).to eq r.get("foo")
  end

  it '#strlen' do
    expect(a.set "foo", "lorem").to eq r.set "foo", "lorem"
    expect(a.strlen("foo")).to eq r.strlen("foo")
  end

  it '#mget' do
    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")
    expect(a.set("bar", "s2")).to eq r.set("bar", "s2")

    expect(a.mget("foo", "bar")).to eq r.mget("foo", "bar")
    expect(a.mget("foo", "bar", "baz")).to eq r.mget("foo", "bar", "baz")
  end

  it 'mget mapped' do
    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")
    expect(a.set("bar", "s2")).to eq r.set("bar", "s2")

    expect(r.mapped_mget("foo", "bar")).to eq r.mapped_mget("foo", "bar")
    # response = r.mapped_mget("foo", "bar")

    # assert_equal "s1", response["foo"]
    # assert_equal "s2", response["bar"]

    expect(r.mapped_mget("foo", "bar", "baz")).to eq r.mapped_mget("foo", "bar", "baz")
    # response = r.mapped_mget("foo", "bar", "baz")

    # assert_equal "s1", response["foo"]
    # assert_equal "s2", response["bar"]
    # assert_equal nil , response["baz"]
  end

  it '#mapped mget in a pipeline returns hash' do
    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")
    expect(a.set("bar", "s2")).to eq r.set("bar", "s2")

    result_r = r.pipelined do
      r.mapped_mget("foo", "bar")
    end

    result_a = a.pipelined do
      a.mapped_mget("foo", "bar")
    end

    expect(result_a).to eq result_r
    # assert_equal result[0], { "foo" => "s1", "bar" => "s2" }
  end

  it '#mset' do
    expect(a.mset(:foo, "s1", :bar, "s2")).to eq r.mset(:foo, "s1", :bar, "s2")

    expect(a.get("foo")).to eq r.get("foo")
    expect(a.get("bar")).to eq r.get("bar")
  end

  it '#mset mapped' do
    expect(a.mapped_mset(:foo => "s1", :bar => "s2")).to eq r.mapped_mset(:foo => "s1", :bar => "s2")

    expect(a.get("foo")).to eq r.get("foo")
    expect(a.get("bar")).to eq r.get("bar")
  end

  it '#msetnx' do
    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")
    expect(a.msetnx(:foo, "s2", :bar, "s3")).to eq r.msetnx(:foo, "s2", :bar, "s3")

    expect(a.get("foo")).to eq r.get("foo")
    expect(a.get("bar")).to eq r.get("bar")

    expect(a.del("foo")).to eq r.del("foo")
    expect(a.msetnx(:foo, "s2", :bar, "s3")).to eq r.msetnx(:foo, "s2", :bar, "s3")
    expect(a.get("foo")).to eq r.get("foo")
    expect(a.get("bar")).to eq r.get("bar")
  end

  it '#msetnx mapped' do
    expect(a.set("foo", "s1")).to eq r.set("foo", "s1")
    expect(a.mapped_msetnx(:foo => "s2", :bar => "s3")).to eq r.mapped_msetnx(:foo => "s2", :bar => "s3")

    expect(a.get("foo")).to eq r.get("foo")
    expect(a.get("bar")).to eq r.get("bar")

    expect(a.del("foo")).to eq r.del("foo")
    expect(a.mapped_msetnx(:foo => "s2", :bar => "s3")).to eq r.mapped_msetnx(:foo => "s2", :bar => "s3")
    expect(a.get("foo")).to eq r.get("foo")
    expect(a.get("bar")).to eq r.get("bar")
  end

  # def test_bitop
  #   try_encoding("UTF-8") do
  #     target_version "2.5.10" do
  #       r.set("foo", "a")
  #       r.set("bar", "b")

  #       r.bitop(:and, "foo&bar", "foo", "bar")
  #       assert_equal "\x60", r.get("foo&bar")
  #       r.bitop(:or, "foo|bar", "foo", "bar")
  #       assert_equal "\x63", r.get("foo|bar")
  #       r.bitop(:xor, "foo^bar", "foo", "bar")
  #       assert_equal "\x03", r.get("foo^bar")
  #       r.bitop(:not, "~foo", "foo")
  #       assert_equal "\x9E", r.get("~foo")
  #     end
  #   end
  # end

end # describe
