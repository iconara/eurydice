# encoding: utf-8

module Eurydice
  module Pelops
    class Cluster
      def initialize(cluster, driver=::Pelops::Pelops)
        @cluster = cluster
        @driver = driver
      end
    
      def connected?
        @driver.create_cluster_manager(@cluster).cassandra_version
        true
      rescue Exception => e
        false
      end
    
      def keyspace(keyspace_name, options={})
        pool_name = options.fetch(:pool_name, "eurydice_#{keyspace_name}_pool")
        create = options.fetch(:create, true)
        unless @driver.get_db_conn_pool(pool_name)
          @driver.add_pool(pool_name, @cluster, keyspace_name)
        end
        keyspace = Keyspace.new(keyspace_name, @cluster, pool_name, @driver)
        keyspace.create! if create && !keyspace.exists?
        keyspace
      end
    
      def keyspaces
        keyspace_manager.keyspace_names.map { |ks_def| ks_def.name }
      end
      
      def nodes
        @cluster.nodes.map { |n| n.address }
      end
    
    private
  
      def keyspace_manager
        @keyspace_manager ||= @driver.create_keyspace_manager(@cluster)
      end
    end
  end
end
