# encoding: utf-8

require 'pelops-jars'
require 'eurydice'
require 'cassandra'


module Pelops
  import 'org.scale7.cassandra.pelops.Cluster'
  import 'org.scale7.cassandra.pelops.Pelops'
  import 'org.scale7.cassandra.pelops.Selector'
  import 'org.scale7.cassandra.pelops.Bytes'
  import 'org.scale7.cassandra.pelops.exceptions.InvalidRequestException'
  import 'org.scale7.cassandra.pelops.exceptions.NotFoundException'
  import 'org.scale7.cassandra.pelops.exceptions.ApplicationException'
end

module Eurydice
  def self.connect(*args)
    Pelops.connect(*args)
  end
  
  def self.disconnect!
    Pelops.disconnect!
  end
  
  module Pelops
    def self.connect(options={})
      host = options.fetch(:host, 'localhost')
      port = options.fetch(:port, 9160)
      pool_name = options.fetch(:pool_name, 'eurydice')
      timeout = options.fetch(:timeout, ::Pelops::Cluster::DEFAULT_TIMEOUT)
      dynamic_node_discovery = options.fetch(:dynamic_node_discovery, false)
      Cluster.new(::Pelops::Cluster.new(host, port, timeout, dynamic_node_discovery))
    end
  
    def self.keyspace(keyspace_name, host='localhost', port=9160, pool_name='eurydice')
      cluster = ::Pelops::Cluster.new(host, port)
      ::Pelops::Pelops.add_pool(pool_name, cluster, keyspace_name)
      Keyspace.new(keyspace_name, cluster, pool_name)
    end
  
    def self.disconnect!
      ::Pelops::Pelops.shutdown
    end

    module ByteHelpers
      extend self
      
      def empty_pelops_bytes
        ::Pelops::Bytes::EMPTY
      end
      
      def to_pelops_bytes(obj, type=nil)
        case type
        when :long
          ::Pelops::Bytes.from_long(obj)
        else
          ::Pelops::Bytes.new(obj.to_s.to_java_bytes)
        end
      end
      
      def to_nio_bytes(str)
        to_pelops_bytes(str).bytes
      end
      
      def to_byte_array(str)
        str.to_java_bytes
      end
      
      def pelops_bytes_to_s(pb)
        String.from_java_bytes(pb.to_byte_array)
      end
      
      def nio_bytes_to_s(nb)
        pelops_bytes_to_s(::Pelops::Bytes.new(nb))
      end
      
      def byte_array_to_s(ba, type=nil)
        case type
        when :long
          ::Pelops::Bytes.new(ba).to_long
        else
          String.from_java_bytes(ba)
        end
      end
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
          when ::Pelops::ApplicationException
            raise EurydiceError, e.cause.message, e.backtrace
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
    
    module ConsistencyLevelHelpers
      def get_cl(options)
        cl = options.fetch(:consistency_level, options.fetch(:cl, :one))
        Cassandra::CONSISTENCY_LEVELS[cl]
      end
      
      def default_cl?(options)
        !(options.key?(:consistency_level) || options.key?(:cl))
      end
    end
  end
end

require_relative 'pelops/mutator'
require_relative 'pelops/cluster'
require_relative 'pelops/keyspace'
require_relative 'pelops/column_family'
