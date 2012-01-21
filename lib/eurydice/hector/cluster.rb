# encoding: utf-8

module Eurydice
  module Hector
    class Cluster
      def initialize(cluster)
        @cluster = cluster
      end
    
      def disconnect!
        @cluster.connection_manager.shutdown
        @cluster = nil
      end
    
      def connected?
        !!@cluster
      end
    
      def keyspace(keyspace_name, options={})
        keyspace = Keyspace.new(keyspace_name, self)
        keyspace.create! if options.fetch(:create, true) && !keyspace.exists?
        keyspace
      end
    
      def keyspaces
        @cluster.describe_keyspaces.map { |ks_def| ks_def.name }
      end
      
      def nodes
      end
      
      # The following methods are internal to the Hector implementation
      
      def create_keyspace!(properties, block_until_complete=true)
        @cluster.add_keyspace(::Hector::Ddl::KeyspaceDefinition.from_h(properties), block_until_complete)
        ::Hector::HFactory.createKeyspace(properties[:name], @cluster);
      end
      
      def drop_keyspace!(keyspace_name)
        @cluster.drop_keyspace(keyspace_name)
      end
      
      def describe_keyspace(keyspace_name)
        @cluster.describe_keyspace(keyspace_name)
      end
      
      def add_column_family(properties, block_until_complete=true)
        @cluster.add_column_family(::Hector::Ddl::ColumnFamilyDefinition.from_h(properties), block_until_complete)
      end
      
      def drop_column_family(keyspace_name, cf_name)
        @cluster.drop_column_family(keyspace_name, cf_name)
      end
      
      def truncate_column_family(keyspace_name, cf_name)
        @cluster.truncate(keyspace_name, cf_name)
      end
    end
  end
end
