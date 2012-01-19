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
      end
        
      def exists?
        @cluster.keyspaces.include?(@name)
      end
    
      def create!(options={})
        strategy_class = options[:strategy_class] || DEFAULT_STRATEGY_CLASS
        strategy_options = options[:strategy_options]
        strategy_options = DEFAULT_STRATEGY_OPTIONS if !strategy_options && strategy_class == DEFAULT_STRATEGY_CLASS
        ks_def = ::Hector::BasicKeyspaceDefinition.new
        ks_def.name = @name
        ks_def.strategy_class = strategy_class
        strategy_options.each do |property, value|
          ks_def.set_strategy_option(property.to_s, value.to_s)
        end
        @cluster.create_keyspace!(ks_def)
      end
    
      def drop!
      end
    
      def column_families(reload=false)
      end
    
      def column_family(name, options={})
      end
    
      def create_mutator
      end
    
      def create_selector
      end
    
      def create_row_deletor
      end

      def keyspace_manager
      end
    
      def column_family_manger
      end
    end
  end
end
