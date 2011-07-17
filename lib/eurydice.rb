# encoding: utf-8

require 'java'

EURYDICE_EXT_HOME = File.expand_path('../ext', __FILE__)

$CLASSPATH << EURYDICE_EXT_HOME

Dir["#{EURYDICE_EXT_HOME}/*.jar"].each { |jar| require(jar) }


module Pelops
  import 'org.scale7.cassandra.pelops.Cluster'
  import 'org.scale7.cassandra.pelops.Pelops'
  import 'org.scale7.cassandra.pelops.Selector'
  import 'org.scale7.cassandra.pelops.exceptions.InvalidRequestException'
end

module Cassandra
  import 'org.apache.cassandra.thrift.ConsistencyLevel'
  import 'org.apache.cassandra.thrift.Column'
  import 'org.apache.cassandra.thrift.KsDef'
  import 'org.apache.cassandra.thrift.CfDef'
  import 'org.apache.cassandra.thrift.InvalidRequestException'
end

module Eurydice
  def self.connect(keyspace_name, host='localhost', port=9160, pool_name='eurydice')
    cluster = Pelops::Cluster.new(host, port)
    Pelops::Pelops.add_pool(pool_name, cluster, keyspace_name)
    Keyspace.new(keyspace_name, cluster, pool_name)
  end
  
  def self.disconnect
    Pelops::Pelops.shutdown
  end
  
  class EurydiceError < StandardError; end
  
  class InvalidRequestError < EurydiceError; end
  
  class KeyspaceExistsError < InvalidRequestError; end
  
  module ExceptionHelpers
    def transform_thrift_exception(e)
      if e.respond_to?(:cause)
        case e.cause
        when Cassandra::InvalidRequestException, Pelops::InvalidRequestException
          message = e.cause.why
          backtrace = e.backtrace
          error_class = case message
          when /Keyspace already exists/
          then KeyspaceExistsError
          else InvalidRequestError
          end
          raise error_class, message, backtrace
        end
      end
      raise e
    end
  end
  
  class Keyspace
    include ExceptionHelpers
    
    attr_reader :name
    
    def initialize(name, cluster, pool_name, driver=Pelops::Pelops)
      @name = name
      @cluster = cluster
      @pool_name = pool_name
      @driver = driver
    end
    
    def create!(options={})
      definition = Cassandra::KsDef.new
      definition.name = @name
      definition.strategy_class = options.fetch(:strategy_class, 'org.apache.cassandra.locator.LocalStrategy')
      definition.cf_defs = java.util.Collections.emptyList
      keyspace_manager.add_keyspace(definition)
      @driver.add_pool(@pool_name, @cluster, @name)
    rescue Exception => e
      transform_thrift_exception(e)
    end
    
    def drop!
      keyspace_manager.drop_keyspace(@name)
    rescue Exception => e
      transform_thrift_exception(e)
    end
    
    def column_family(name)
      ColumnFamily.new(self, name)
    end
    
    def create_mutator
      @driver.create_mutator(@pool_name)
    end
    
    def create_selector
      @driver.create_selector(@pool_name)
    end

    def keyspace_manager
      @keyspace_manager ||= @driver.create_keyspace_manager(@cluster)
    end
    
    def column_family_manger
      @column_family_manger ||= @driver.create_column_family_manager(@cluster, @name)
    end
  end
  
  class ColumnFamily
    include ExceptionHelpers
    
    attr_reader :name
    
    def initialize(keyspace, name)
      @keyspace, @name = keyspace, name
    end
    
    def create!(options={})
      definition = Cassandra::CfDef.new
      definition.keyspace = @keyspace.name
      definition.name = @name
      @keyspace.column_family_manger.add_column_family(definition)
    rescue Exception => e
      transform_thrift_exception(e)
    end
    
    def drop!
      @keyspace.column_family_manger.drop_column_family(@name)
    rescue Exception => e
      transform_thrift_exception(e)
    end
    
    def truncate!
      @keyspace.column_family_manger.truncate_column_family(@name)
    end
    
    def update(row_key, properties, options={})
      cl = options[:consistency_level] || CONSISTENCY_LEVELS[:one]
      mutator = @keyspace.create_mutator
      columns = properties.map { |k, v| mutator.new_column(k.to_s, v.to_s) }
      mutator.write_columns(@name, row_key, columns)
      mutator.execute(cl)
    rescue Exception => e
      transform_thrift_exception(e)
    end
    alias_method :insert, :update
    
    def get(row_key, options={})
      cl = options[:consistency_level] || CONSISTENCY_LEVELS[:one]
      selector = @keyspace.create_selector
      columns = selector.get_columns_from_row(@name, row_key, false, cl)
      if columns.empty?
        nil
      else
        columns.reduce({}) do |acc, column|
          key = selector.class.get_column_string_name(column)
          value = selector.class.get_column_string_value(column)
          acc[key] = value
          acc
        end
      end
    rescue Exception => e
      transform_thrift_exception(e)
    end
    
  private
  
    CONSISTENCY_LEVELS = {
      :one    => Cassandra::ConsistencyLevel::ONE,
      :quorum => Cassandra::ConsistencyLevel::QUORUM,
      :all    => Cassandra::ConsistencyLevel::ALL,
      :any    => Cassandra::ConsistencyLevel::ANY
    }  
  end
end
