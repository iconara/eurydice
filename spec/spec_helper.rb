$: << File.expand_path('../../lib', __FILE__)

ENV['CASSANDRA_HOST'] ||= 'localhost'

require 'bundler/setup'
require 'eurydice/pelops'

require_relative 'eurydice/support/cluster'
require_relative 'eurydice/support/column_family'
require_relative 'eurydice/support/keyspace'