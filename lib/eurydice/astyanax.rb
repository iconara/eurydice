# encoding: utf-8

require 'astyanax-jars'
require 'eurydice'


module Astyanax
  java_import 'com.netflix.astyanax.AstyanaxContext'
  java_import 'com.netflix.astyanax.Serializer'
  java_import 'com.netflix.astyanax.model.ColumnFamily'
  java_import 'com.netflix.astyanax.model.ConsistencyLevel'
  java_import 'com.netflix.astyanax.model.ColumnSlice'
  java_import 'com.netflix.astyanax.serializers.BytesArraySerializer'
  java_import 'com.netflix.astyanax.impl.AstyanaxConfigurationImpl'
  java_import 'com.netflix.astyanax.shallows.EmptyColumn'
  java_import 'com.netflix.astyanax.connectionpool.NodeDiscoveryType'
  java_import 'com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl'
  java_import 'com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor'

  module Thrift
    java_import 'com.netflix.astyanax.thrift.ThriftFamilyFactory'
    java_import 'com.netflix.astyanax.thrift.ddl.ThriftKeyspaceDefinitionImpl'
    java_import 'com.netflix.astyanax.thrift.ddl.ThriftColumnFamilyDefinitionImpl'
  end
end

module Eurydice
  module Astyanax
    def self.connect(options={})
      context_builder = ::Astyanax::AstyanaxContext::Builder.new.tap do |b|
        conf = ::Astyanax::AstyanaxConfigurationImpl.new.tap do |c|
          c.setDiscoveryType(::Astyanax::NodeDiscoveryType::NONE)
        end

        pool_conf = ::Astyanax::ConnectionPoolConfigurationImpl.new('eurydice_pool').tap do |c|
          c.set_port(options[:port] || 9160)
          c.set_seeds(options[:host] || '127.0.0.1')
        end

        b.for_cluster(options[:cluster_name]) if options[:cluster_name]
        b.for_keyspace(options[:keyspace_name]) if options[:keyspace_name]
        b.with_astyanax_configuration(conf)
        b.with_connection_pool_configuration(pool_conf)
        b.with_connection_pool_monitor(::Astyanax::CountingConnectionPoolMonitor.new)
      end

      thrift_factory = ::Astyanax::Thrift::ThriftFamilyFactory.instance
      context = context_builder.build_cluster(thrift_factory)
      context.start
      Cluster.new(context)
    end
  
    def self.disconnect!
    end

    module ConsistencyLevelHelpers
      include ::Eurydice::ConsistencyLevelHelpers

      CONSISTENCY_LEVELS = {
        :any          => ::Astyanax::ConsistencyLevel::CL_ANY,
        :one          => ::Astyanax::ConsistencyLevel::CL_ONE,
        :local_quorum => ::Astyanax::ConsistencyLevel::CL_LOCAL_QUORUM,
        :each_quorum  => ::Astyanax::ConsistencyLevel::CL_EACH_QUORUM,
        :quorum       => ::Astyanax::ConsistencyLevel::CL_QUORUM,
        :all          => ::Astyanax::ConsistencyLevel::CL_ALL
      }

      def get_cl(options)
        cl = options.fetch(:consistency_level, options.fetch(:cl, :one))
        CONSISTENCY_LEVELS[cl]
      end
    end
  end
end

require_relative 'astyanax/cluster'
require_relative 'astyanax/keyspace'
require_relative 'astyanax/column_family'
