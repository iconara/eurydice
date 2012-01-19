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
        @cluster.create_keyspace!(::Hector::Ddl::KeyspaceDefinition.from_h(ks_properties))
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
    end
  end
end
