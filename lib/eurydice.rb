# encoding: utf-8

require 'java'
require 'cassandra'

module Eurydice
  class EurydiceError < StandardError; end
  class InvalidRequestError < EurydiceError; end
  class KeyspaceExistsError < InvalidRequestError; end
  class NotFoundError < EurydiceError; end
  class TimeoutError < EurydiceError; end
  class BatchError < EurydiceError; end

  DEFAULT_STRATEGY_CLASS = Cassandra::LOCATOR_STRATEGY_CLASSES[:simple]
  DEFAULT_STRATEGY_OPTIONS = {:replication_factor => 1}.freeze

  def self.connect(*args)
    Pelops.connect(*args)
  end
  
  def self.disconnect!
    Pelops.disconnect!
  end

  module ConsistencyLevelHelpers
    def get_cl(options)
      cl = options.fetch(:consistency_level, options.fetch(:cl, :one))
      Cassandra::CONSISTENCY_LEVELS[cl]
    end
    
    def default_cl?(options)
      !(options.key?(:consistency_level) || options.key?(:cl))
    end
  end
end

require 'eurydice/pelops'
require 'eurydice/astyanax'
