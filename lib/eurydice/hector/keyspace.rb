# encoding: utf-8

module Eurydice
  module Hector
    class Keyspace
      attr_reader :name
    
      def initialize(name, cluster)
        @name = name
        @cluster = cluster
      end
    
      def definition(reload=false)
        @definition = nil if reload
        @definition ||= begin
          ks_def = @cluster.describe_keyspace(@name)
          ks_def.to_h
        end
      end
        
      def exists?
        @cluster.keyspaces.include?(@name)
      end
    
      def create!(options={})
        ks_properties = options.merge(:name => @name)
        ks_properties[:strategy_class] = DEFAULT_STRATEGY_CLASS unless ks_properties.key?(:strategy_class)
        ks_properties[:strategy_options] = DEFAULT_STRATEGY_OPTIONS if !ks_properties.key?(:strategy_options) && ks_properties[:strategy_class] == DEFAULT_STRATEGY_CLASS
        @keyspace = @cluster.create_keyspace!(ks_properties)
      end
    
      def drop!
        @cluster.drop_keyspace!(@name)
      end
    
      def column_families(reload=false)
        definition(reload)[:column_families].keys
      end
    
      def column_family(name, options={})
        cf = ColumnFamily.new(name, self)
        cf.create! if options.fetch(:create, true) && !cf.exists?
        cf
      end
      
      # The following methods are internal to the Hector implementation
      
      def add_column_family(properties)
        @cluster.add_column_family(properties.merge(:keyspace => @name))
      end
      
      def drop_column_family(cf_name)
        @cluster.drop_column_family(@name, cf_name)
      end
      
      def create_template(cf_name, row_key_serializer, column_key_serializer)
        ::Hector::ThriftColumnFamilyTemplate.new(@keyspace, cf_name, row_key_serializer, column_key_serializer)
      end
      
      def truncate_column_family(cf_name)
        @cluster.truncate_column_family(@name, cf_name)
      end
    end
  end
end
