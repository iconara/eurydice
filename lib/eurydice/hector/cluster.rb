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
      
      # internal to the Hector implementation
      def create_keyspace!(definition, block_until_complete=true)
        @cluster.add_keyspace(definition, block_until_complete)
      end
    end
  end
end
