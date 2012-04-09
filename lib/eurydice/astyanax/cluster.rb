# encoding: utf-8

module Eurydice
  module Astyanax
    class Cluster
      def initialize(context)
        @context = context
        @cluster = context.entity
      end

      def keyspaces
        @cluster.describe_keyspaces.map { |ks| ks.name }
      end

      def keyspace(keyspace_name, options={})
        ks = Keyspace.new(self, keyspace_name)
        ks.create! unless options[:create] == false
        ks
      end

      # internal methods

      def describe_keyspace(keyspace_name)
        @cluster.describe_keyspace(keyspace_name)
      end

      def add_keyspace(ks_def)
        @cluster.add_keyspace(::Astyanax::Thrift::ThriftKeyspaceDefinitionImpl.new(ks_def))
      end

      def add_column_family(cf_def)
        @cluster.add_column_family(::Astyanax::Thrift::ThriftColumnFamilyDefinitionImpl.new(cf_def))
      end

      def drop_keyspace(keyspace_name)
        @cluster.drop_keyspace(keyspace_name)
      end

      def drop_column_family(keyspace_name, column_family_name)
        @cluster.drop_column_family(keyspace_name, column_family_name)
      end

      def create_keyspace_client(keyspace_name)
        @cluster.get_keyspace(keyspace_name)
      end
    end
  end
end