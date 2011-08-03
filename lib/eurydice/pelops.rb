# encoding: utf-8

require 'eurydice'
require 'cassandra'


module Pelops
  import 'org.scale7.cassandra.pelops.Cluster'
  import 'org.scale7.cassandra.pelops.Pelops'
  import 'org.scale7.cassandra.pelops.Selector'
  import 'org.scale7.cassandra.pelops.Bytes'
  import 'org.scale7.cassandra.pelops.exceptions.InvalidRequestException'
  import 'org.scale7.cassandra.pelops.exceptions.NotFoundException'
end

module Eurydice
  def self.connect
    Pelops.connect
  end
  
  module Pelops
    def self.connect(options={})
      host = options.fetch(:host, 'localhost')
      port = options.fetch(:port, 9160)
      pool_name = options.fetch(:pool_name, 'eurydice')
      Cluster.new(::Pelops::Cluster.new(host, port))
    end
  
    def self.keyspace(keyspace_name, host='localhost', port=9160, pool_name='eurydice')
      cluster = ::Pelops::Cluster.new(host, port)
      ::Pelops::Pelops.add_pool(pool_name, cluster, keyspace_name)
      Keyspace.new(keyspace_name, cluster, pool_name)
    end
  
    def self.disconnect!
      ::Pelops::Pelops.shutdown
    end

    module ExceptionHelpers
      def transform_thrift_exception(e)
        if e.respond_to?(:cause)
          case e.cause
          when Cassandra::InvalidRequestException, ::Pelops::InvalidRequestException
            message = e.cause.why
            backtrace = e.backtrace
            error_class = begin
              case message
              when /Keyspace already exists/
              then KeyspaceExistsError
              else InvalidRequestError
              end
            end
            raise error_class, message, backtrace
          when ::Pelops::NotFoundException
            raise NotFoundError, e.cause.message, e.backtrace
          when Thrift::TTransportException
            raise TimeoutError, e.cause.message, e.backtrace
          end
        end
        raise e
      end
    
      def thrift_exception_handler
        yield
      rescue Exception => e
        transform_thrift_exception(e)
      end
    end
  end
end

require_relative 'pelops/cluster'
require_relative 'pelops/keyspace'
require_relative 'pelops/column_family'
