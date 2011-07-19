# encoding: utf-8

require 'java'

EURYDICE_EXT_HOME = File.expand_path('../ext', __FILE__)

$CLASSPATH << EURYDICE_EXT_HOME

Dir["#{EURYDICE_EXT_HOME}/*.jar"].each { |jar| require(jar) }


module Pelops
  import 'org.scale7.cassandra.pelops.Cluster'
  import 'org.scale7.cassandra.pelops.Pelops'
  import 'org.scale7.cassandra.pelops.Selector'
  import 'org.scale7.cassandra.pelops.Bytes'
  import 'org.scale7.cassandra.pelops.exceptions.InvalidRequestException'
  import 'org.scale7.cassandra.pelops.exceptions.NotFoundException'
end

module Cassandra
  import 'org.apache.cassandra.thrift.ConsistencyLevel'
  import 'org.apache.cassandra.thrift.Column'
  import 'org.apache.cassandra.thrift.KsDef'
  import 'org.apache.cassandra.thrift.CfDef'
  import 'org.apache.cassandra.thrift.InvalidRequestException'
  import 'org.apache.cassandra.thrift.SlicePredicate'
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
  
  class NotFoundError < EurydiceError; end
  
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
        when Pelops::NotFoundException
          raise NotFoundError, e.cause.message, e.backtrace
        end
      end
      raise e
    end
    
    def thrift_exception_handler
      yield
    rescue Exception => e
      transform_thrift_exception(e)
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
    
    def connected?
      @driver.create_cluster_manager(@cluster).cassandra_version
      true
    rescue Exception => e
      false
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
      thrift_exception_handler do
        definition = Cassandra::CfDef.new
        definition.keyspace = @keyspace.name
        definition.name = @name
        @keyspace.column_family_manger.add_column_family(definition)
      end
    end
    
    def drop!
      thrift_exception_handler do
        @keyspace.column_family_manger.drop_column_family(@name)
      end
    end
    
    def truncate!
      thrift_exception_handler do
        @keyspace.column_family_manger.truncate_column_family(@name)
      end
    end
    
    def update(row_key, properties, options={})
      thrift_exception_handler do
        mutator = @keyspace.create_mutator
        columns = properties.map do |k, v| 
          mutator.new_column(Pelops::Bytes.new(k.to_s.to_java_bytes), Pelops::Bytes.new(v.to_s.to_java_bytes))
        end
        mutator.write_columns(@name, row_key, columns)
        mutator.execute(get_cl(options))
      end
    end
    alias_method :insert, :update
    
    def exists?(row_key, options={})
      thrift_exception_handler do
        selector = @keyspace.create_selector
        predicate = Cassandra::SlicePredicate.new
        count = selector.get_column_count(@name, row_key, get_cl(options))
        count > 0
      end
    end
    
    def get(row_key, options={})
      thrift_exception_handler do
        selector = @keyspace.create_selector
        columns = selector.get_columns_from_row(@name, row_key, false, get_cl(options))
        if columns.empty?
          nil
        else
          columns_to_h(columns)
        end
      end
    end
    
    def get_column(row_key, column_key, options={})
      thrift_exception_handler do
        selector = @keyspace.create_selector
        column = selector.get_column_from_row(@name, row_key, column_key, get_cl(options))
        String.from_java_bytes(column.get_value)
      end
    rescue NotFoundError => e
      nil
    end
    
    def get_column_multi(row_keys, column_key, options={})
      thrift_exception_handler do
        selector = @keyspace.create_selector
        column_predicate = Cassandra::SlicePredicate.new
        column_predicate.addToColumn_names(Pelops::Bytes.new(column_key.to_java_bytes).get_bytes)
        byte_row_keys = row_keys.map { |rk| Pelops::Bytes.new(rk.to_java_bytes) }
        result = selector.get_columns_from_rows(@name, byte_row_keys, column_predicate, get_cl(options))
        result.reduce({}) do |acc, (row_key, columns)|
          acc[String.from_java_bytes(row_key.to_byte_array)] = columns_to_h(columns)
          acc
        end
      end
    end
    
  private
  
    def columns_to_h(columns)
      columns.reduce({}) do |acc, column|
        key   = String.from_java_bytes(column.get_name)
        value = String.from_java_bytes(column.get_value)
        acc[key] = value
        acc
      end
    end
  
    def get_cl(options)
      cl = options.fetch(:consistency_level, options.fetch(:cl, :one))
      CONSISTENCY_LEVELS[cl]
    end
  
    CONSISTENCY_LEVELS = {
      :one    => Cassandra::ConsistencyLevel::ONE,
      :quorum => Cassandra::ConsistencyLevel::QUORUM,
      :all    => Cassandra::ConsistencyLevel::ALL,
      :any    => Cassandra::ConsistencyLevel::ANY
    }  
  end
end
