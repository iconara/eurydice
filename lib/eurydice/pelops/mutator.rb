# encoding: utf-8

module Eurydice
  module Pelops
    class Mutator
      include ExceptionHelpers
      include ConsistencyLevelHelpers
      include ByteHelpers
      
      def initialize(keyspace)
        @keyspace = keyspace
        @mutator = @keyspace.create_mutator
      end
        
      def delete_column(cf_name, row_key, column_key)
        @mutator.delete_column(cf_name, row_key, to_pelops_bytes(column_key))
      end
    
      def delete_columns(cf_name, row_key, column_keys)
        @mutator.delete_columns(cf_name, row_key, column_keys.map { |k| to_pelops_bytes(k) })
      end
    
      def update(cf_name, row_key, properties, options={})
        types = options[:validations] || {}
        key_type = options[:comparator]
        columns = properties.map do |k, v|
          key = to_pelops_bytes(k, key_type)
          value = to_pelops_bytes(v, types[k])
          ttl = options.fetch(:ttl, @mutator.class::NO_TTL)
          @mutator.new_column(key, value, ttl)
        end
        @mutator.write_columns(cf_name, row_key, columns)
      end
      alias_method :insert, :update
        
      def execute!(options={})
        thrift_exception_handler do
          @mutator.execute(get_cl(options))
        end
      end
    end
  end
end