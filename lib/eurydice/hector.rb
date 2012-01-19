# encoding: utf-8

require 'hector-core-jars'
require 'eurydice'
require 'cassandra'


module Hector
  import 'me.prettyprint.hector.api.Cluster'
  import 'me.prettyprint.hector.api.factory.HFactory'
  import 'me.prettyprint.cassandra.service.CassandraHostConfigurator'
  import 'me.prettyprint.cassandra.model.BasicKeyspaceDefinition'
end

module Eurydice
  module Hector
    def self.connect(options={})
      cluster_name = options.fetch(:cluster_name, 'eurydice')
      configurator = ::Hector::CassandraHostConfigurator.new(options.fetch(:host, 'localhost'))
      configurator.port = options.fetch(:port, 9160)
      configurator.cassandra_thrift_socket_timeout = options.fetch(:thrift_timeout, 3000)
      configurator.auto_discover_hosts = options.fetch(:auto_discover_hosts, false)
      Cluster.new(::Hector::HFactory.get_or_create_cluster(cluster_name, configurator))
    end
  
    def self.disconnect!
      raise NotImplementedError, %(The Hector driver has no global disconnect method)
    end
  end
end

require_relative 'hector/cluster'
require_relative 'hector/keyspace'
require_relative 'hector/column_family'
