$: << File.expand_path('../../lib', __FILE__)

require 'bundler/setup'
require 'eurydice/pelops'

ENV['CASSANDRA_HOST'] ||= 'localhost'

require_relative 'eurydice/support/cluster'
require_relative 'eurydice/support/column_family'
require_relative 'eurydice/support/keyspace'