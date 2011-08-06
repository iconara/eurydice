# encoding: utf-8

module Eurydice
  module Pelops
    class ColumnFamily
      include ExceptionHelpers
      include ByteHelpers
    
      attr_reader :name, :keyspace
    
      def initialize(keyspace, name)
        @keyspace, @name = keyspace, name
      end
    
      def definition(reload=true)
        @definition = nil if reload
        @definition ||= @keyspace.definition(true)[:column_families][@name]
      end
    
      def exists?
        !!definition(true)
      end
    
      def create!(options={})
        thrift_exception_handler do
          @keyspace.column_family_manger.add_column_family(Cassandra::CfDef.from_h(options.merge(:keyspace => @keyspace.name, :name => @name)))
        end
      end
    
      def drop!
        thrift_exception_handler do
          @keyspace.column_family_manger.drop_column_family(@name)
        end
      end
    
      def truncate!
        thrift_exception_handler do
          @keyspace.column_family_manger.truncate_column_family(@name)
        end
      end
    
      def delete(row_key, options={})
        thrift_exception_handler do
          deletor = @keyspace.create_row_deletor
          deletor.delete_row(@name, row_key, get_cl(options))
        end
      end
    
      def delete_column(row_key, column_key, options={})
        thrift_exception_handler do
          mutator = @keyspace.create_mutator
          mutator.delete_column(@name, row_key, to_pelops_bytes(column_key))
          mutator.execute(get_cl(options))
        end
      end
    
      def delete_columns(row_key, column_keys, options={})
        thrift_exception_handler do
          mutator = @keyspace.create_mutator
          mutator.delete_columns(@name, row_key, column_keys.map { |k| to_pelops_bytes(k) })
          mutator.execute(get_cl(options))
        end
      end
    
      def update(row_key, properties, options={})
        thrift_exception_handler do
          mutator = @keyspace.create_mutator
          columns = properties.map do |k, v|
            mutator.new_column(to_pelops_bytes(k), to_pelops_bytes(v))
          end
          mutator.write_columns(@name, row_key, columns)
          mutator.execute(get_cl(options))
        end
      end
      alias_method :insert, :update
    
      def key?(row_key, options={})
        thrift_exception_handler do
          selector = @keyspace.create_selector
          predicate = Cassandra::SlicePredicate.new
          count = selector.get_column_count(@name, row_key, get_cl(options))
          count > 0
        end
      end
      alias_method :row_exists?, :key?
    
      def get(row_or_rows, options={})
        case row_or_rows
        when Array then get_multi(row_or_rows, options)
        else get_single(row_or_rows, options)
        end
      end
      alias_method :get_row, :get
      alias_method :get_rows, :get
      
      def get_column(row_key, column_key, options={})
        thrift_exception_handler do
          selector = @keyspace.create_selector
          column = selector.get_column_from_row(@name, row_key, column_key, get_cl(options))
          byte_array_to_s(column.get_value)
        end
      rescue NotFoundError => e
        nil
      end
      
    private
    
      def get_single(row_key, options={})
        thrift_exception_handler do
          selector = @keyspace.create_selector
          column_predicate = create_column_predicate(options)
          columns = selector.get_columns_from_row(@name, to_pelops_bytes(row_key), column_predicate, get_cl(options))
          columns_to_h(columns)
        end
      end
    
      def get_multi(row_keys, options={})
        thrift_exception_handler do
          selector = @keyspace.create_selector
          column_predicate = create_column_predicate(options)
          byte_row_keys = row_keys.map { |rk| to_pelops_bytes(rk) }
          rows = selector.get_columns_from_rows(@name, byte_row_keys, column_predicate, get_cl(options))
          rows_to_h(rows)
        end
      end
      
      def create_column_predicate(options)
        max_column_count = options.fetch(:max_column_count, java.lang.Integer::MAX_VALUE)
        reversed = options.fetch(:reversed, false)
        case options[:columns]
        when Range
          ::Pelops::Selector.new_columns_predicate(to_pelops_bytes(options[:columns].begin), to_pelops_bytes(options[:columns].end), reversed, max_column_count)
        when Array
          ::Pelops::Selector.new_columns_predicate(*options[:columns].map { |col| to_pelops_bytes(col) })
        else
          ::Pelops::Selector.new_columns_predicate_all(reversed, max_column_count)
        end
      end
    
      def rows_to_h(rows)
        rows.reduce({}) do |acc, (row_key, columns)|
          columns_h = columns_to_h(columns)
          acc[pelops_bytes_to_s(row_key)] = columns_h if columns_h && !columns_h.empty?
          acc
        end
      end
  
      def columns_to_h(columns)
        if columns.empty?
          nil
        else
          columns.reduce({}) do |acc, column|
            key   = byte_array_to_s(column.get_name)
            value = byte_array_to_s(column.get_value)
            acc[key] = value
            acc
          end
        end
      end
  
      def get_cl(options)
        cl = options.fetch(:consistency_level, options.fetch(:cl, :one))
        Cassandra::CONSISTENCY_LEVELS[cl]
      end
    end
  end
end
