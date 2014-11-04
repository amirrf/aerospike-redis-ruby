# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'aerospike/redis/version'

Gem::Specification.new do |spec|
  spec.name          = "aerospike-redis"
  spec.version       = Aerospike::Redis::VERSION
  spec.authors       = ["Amir Rahimi Farahani"]
  spec.email         = ["amirrf@gmail.com"]
  spec.summary       = "Aerospike Ruby Adapter for Redis."
  spec.description   = "Try Aerospike as a back-end replacement for Redis in Ruby applications."
  spec.homepage      = "https://github.com/amirrf/aerospike-redis-ruby"
  spec.license       = "Apache2.0"
  spec.files         = Dir.glob("lib/**/*") + %w(LICENSE.txt README.md)
  spec.require_paths = ["lib"]
  spec.required_ruby_version = '>= 1.9.3'
  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_runtime_dependency "aerospike", '~> 0.1', '>= 0.1.3'
  spec.add_runtime_dependency "redis", "~> 3"
end
