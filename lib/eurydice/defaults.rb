# encoding: utf-8

require 'cassandra'


module Eurydice
  DEFAULT_STRATEGY_CLASS = Cassandra::LOCATOR_STRATEGY_CLASSES[:simple]
  DEFAULT_STRATEGY_OPTIONS = {:replication_factor => 1}.freeze
end