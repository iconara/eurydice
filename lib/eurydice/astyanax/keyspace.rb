# encoding: utf-8

module Eurydice
  module Astyanax
    class Keyspace
      include ConsistencyLevelHelpers
      
      attr_reader :name

      def initialize(cluster, name)
        @cluster = cluster
        @name = name
        @batch_key = "#{@name}-batch"
      end

      def create!(options={})
        ks_properties = options.merge(:name => @name)
        ks_properties[:strategy_class] = DEFAULT_STRATEGY_CLASS unless ks_properties.key?(:strategy_class)
        ks_properties[:strategy_options] = DEFAULT_STRATEGY_OPTIONS if !ks_properties.key?(:strategy_options) && ks_properties[:strategy_class] == DEFAULT_STRATEGY_CLASS
        ks_def = Cassandra::KsDef.from_h(ks_properties)
        @cluster.add_keyspace(ks_def)
      end

      def drop!
        @cluster.drop_keyspace(@name)
      end

      def definition(reload=false)
        @definition = nil if reload
        @definition ||= @cluster.describe_keyspace(@name).thrift_keyspace_definition.to_h
        @definition
      end

      def exists?
        @cluster.keyspaces.include?(@name)
      end

      def column_families(reload=false)
        definition(reload)[:column_families].keys
      end

      def column_family(cf_name, options={})
        create = options.fetch(:create, true)
        cf = ColumnFamily.new(self, cf_name)
        cf.create! if create && !cf.exists?
        cf
      end

      def batch(options={})
        if batch_in_progress?
          check_batch_options!(options)
          yield current_batch_mutator
        else
          start_batch(options)
          begin
            yield current_batch_mutator
            end_batch!
          ensure
            clear_batch!
          end
        end
        nil
      end

      # internal methods

      def add_column_family(cf_def)
        @cluster.add_column_family(cf_def)
      end

      def drop_column_family(cf_name)
        @cluster.drop_column_family(@name, cf_name)
      end

      def keyspace_client
        @keyspace_client ||= @cluster.create_keyspace_client(@name)
      end

    private

      def start_batch(options={})
        thread_local_storage[@batch_key] ||= {
          :mutator => keyspace_client.prepare_mutation_batch,
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
        thread_local_storage[@batch_key] = nil
        thread_local_storage.delete(@batch_key)
        nil
      end
      
      def end_batch!
        current_batch_mutator.set_consistency_level(get_cl(current_batch_options))
        current_batch_mutator.execute
      end
      
      def thread_local_storage
        Thread.current[:eurydice_pelops] ||= {}
      end
    end
  end
end