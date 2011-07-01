# encoding: utf-8

require 'java'

EURYDICE_EXT_HOME = File.expand_path('../ext', __FILE__)

$CLASSPATH << EURYDICE_EXT_HOME

Dir["#{EURYDICE_EXT_HOME}/*.jar"].each { |jar| require(jar) }


module Pelops
  import 'org.scale7.cassandra.pelops.Cluster'
  import 'org.scale7.cassandra.pelops.Pelops'
  import 'org.scale7.cassandra.pelops.Selector'
end

module Cassandra
  import 'org.apache.cassandra.thrift.ConsistencyLevel'
  import 'org.apache.cassandra.thrift.Column'
end

module Eurydice
  def self.connect(keyspace, host='localhost', port=9160, pool_name='eurydice')
    cluster = Pelops::Cluster.new(host, port)
    Pelops::Pelops.add_pool(pool_name, cluster, keyspace)
    Keyspace.new(pool_name)
  end
  
  def self.disconnect
    Pelops::Pelops.shutdown
  end
  
  class Keyspace
    def initialize(pool_name)
      @pool_name = pool_name
    end
    
    def column_family(name)
      ColumnFamily.new(self, name)
    end
    
    def create_mutator
      Pelops::Pelops.create_mutator(@pool_name)
    end
    
    def create_selector
      Pelops::Pelops.create_selector(@pool_name)
    end
  end
  
  class ColumnFamily
    def initialize(keyspace, name)
      @keyspace, @name = keyspace, name
    end
    
    def update(row_key, properties, options={})
      cl = options[:consistency_level] || CONSISTENCY_LEVELS[:one]
      mutator = @keyspace.create_mutator
      columns = properties.map { |k, v| mutator.new_column(k.to_s, v.to_s) }
      mutator.write_columns(@name, row_key, columns)
      mutator.execute(cl)
    end
    
    def get(row_key, options={})
      cl = options[:consistency_level] || CONSISTENCY_LEVELS[:one]
      selector = @keyspace.create_selector
      columns = selector.get_columns_from_row(@name, row_key, false, cl)
      columns.reduce({}) do |acc, column|
        key = Pelops::Selector.get_column_string_name(column)
        value = Pelops::Selector.get_column_string_value(column)
        acc[key] = value
        acc
      end
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
