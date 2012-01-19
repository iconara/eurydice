# encoding: utf-8

module Eurydice
  module Pelops
    class Keyspace
      include ExceptionHelpers
    
      attr_reader :name
    
      def initialize(name, cluster, pool_name, driver)
        @name = name
        @cluster = cluster
        @pool_name = pool_name
        @driver = driver
      end
    
      def definition(reload=false)
        thrift_exception_handler do
          @definition = nil if reload
          @definition ||= keyspace_manager.get_keyspace_schema(@name).to_h
          @definition
        end
      end
        
      def exists?
        keyspace_manager.keyspace_names.map { |ks_def| ks_def.name }.include?(@name)
      end
    
      def create!(options={})
        thrift_exception_handler do
          ks_properties = options.merge(:name => @name)
          ks_properties[:strategy_class] = DEFAULT_STRATEGY_CLASS unless ks_properties.key?(:strategy_class)
          ks_properties[:strategy_options] = DEFAULT_STRATEGY_OPTIONS if !ks_properties.key?(:strategy_options) && ks_properties[:strategy_class] == DEFAULT_STRATEGY_CLASS
          ks_def = Cassandra::KsDef.from_h(ks_properties)
          keyspace_manager.add_keyspace(ks_def)
          @driver.add_pool(@pool_name, @cluster, @name)
        end
      end
    
      def drop!
        keyspace_manager.drop_keyspace(@name)
      rescue Exception => e
        transform_thrift_exception(e)
      end
    
      def column_families(reload=false)
        definition(reload)[:column_families].keys
      end
    
      def column_family(name, options={})
        create = options.fetch(:create, true)
        cf = ColumnFamily.new(self, name)
        cf.create! if create && !cf.exists?
        cf
      end
    
      def create_mutator
        @driver.create_mutator(@pool_name)
      end
    
      def create_selector
        @driver.create_selector(@pool_name)
      end
    
      def create_row_deletor
        @driver.create_row_deletor(@pool_name)
      end

      def keyspace_manager
        @keyspace_manager ||= @driver.create_keyspace_manager(@cluster)
      end
    
      def column_family_manger
        @column_family_manger ||= @driver.create_column_family_manager(@cluster, @name)
      end
    end
  end
end
