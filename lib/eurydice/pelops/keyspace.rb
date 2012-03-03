# encoding: utf-8

module Eurydice
  module Pelops
    class Keyspace
      include ExceptionHelpers
      include ConsistencyLevelHelpers
    
      attr_reader :name
    
      def initialize(name, cluster, pool_name, driver)
        @name = name
        @cluster = cluster
        @pool_name = pool_name
        @driver = driver
        @batch_key = "#{@name}-batch"
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
      
      def batch(options={})
        if batch_in_progress?
          check_batch_options!(options)
          yield current_batch_mutator
        else
          start_batch(options)
          begin
            yield current_batch_mutator
          rescue
            clear_batch!
            raise
          end
          end_batch!
        end
      end
      
    private
    
      DEFAULT_STRATEGY_CLASS = Cassandra::LOCATOR_STRATEGY_CLASSES[:simple]
      DEFAULT_STRATEGY_OPTIONS = {:replication_factor => 1}.freeze

      def start_batch(options={})
        thread_local_storage[@batch_key] ||= {
          :mutator => Mutator.new(self),
          :options => options
        }
      end
      
      def batch_in_progress?
        !!thread_local_storage[@batch_key]
      end
      
      def check_batch_options!(options)
        unless default_cl?(options)
          required_cl = get_cl(options)
          current_cl = get_cl(current_batch_options)
          raise BatchError, %(Inconsistent consistency levels! Current batch: #{current_cl}, required: #{required_cl}) unless required_cl == current_cl
        end
      end
      
      def current_batch_mutator
        thread_local_storage[@batch_key][:mutator]
      end
      
      def current_batch_options
        thread_local_storage[@batch_key][:options]
      end

      def clear_batch!
        thread_local_storage.delete(@batch_key)
        nil
      end
      
      def end_batch!
        current_batch_mutator.execute!(current_batch_options)
        clear_batch!
      end
      
      def thread_local_storage
        Thread.current[:eurydice_pelops] ||= {}
      end
    end
  end
end
