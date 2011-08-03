# encoding: utf-8

module Eurydice
  module Pelops
    class ColumnFamily
      include ExceptionHelpers
    
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
    
      # Valid (and implemented) options:
      # * :comparator_type - the name of the class used to sort column keys
      # * :subcomparator_type - same as :comparator_type but for the keys of subcolumns
      # * :default_validation_class - the name of the class used to validate the column values
      # * :column_type - :standard (default) or :super
      # * :column_metadata - a hash with column names as keys, see below for valid options
      #
      # Valid options for column metadata:
      # * :validation_class - the name of the class used to validate the column value, required if any options for the column are specified
      # * :index_name - if set this column will be indexed and this is specifies the index name
      # * :index_type - defaults to :keys, which is also the only valid value
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
          mutator.delete_column(@name, row_key, ::Pelops::Bytes.new(column_key.to_s.to_java_bytes))
          mutator.execute(get_cl(options))
        end
      end
    
      def delete_columns(row_key, column_keys, options={})
        thrift_exception_handler do
          mutator = @keyspace.create_mutator
          mutator.delete_columns(@name, row_key, column_keys.map { |k| ::Pelops::Bytes.new(k.to_s.to_java_bytes) })
          mutator.execute(get_cl(options))
        end
      end
    
      def update(row_key, properties, options={})
        thrift_exception_handler do
          mutator = @keyspace.create_mutator
          columns = properties.map do |k, v|
            mutator.new_column(::Pelops::Bytes.new(k.to_s.to_java_bytes), ::Pelops::Bytes.new(v.to_s.to_java_bytes))
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
    
      def get(row_key, options={})
        thrift_exception_handler do
          selector = @keyspace.create_selector
          columns = selector.get_columns_from_row(@name, row_key, false, get_cl(options))
          if columns.empty?
            nil
          else
            columns_to_h(columns)
          end
        end
      end
    
      def get_column(row_key, column_key, options={})
        thrift_exception_handler do
          selector = @keyspace.create_selector
          column = selector.get_column_from_row(@name, row_key, column_key, get_cl(options))
          String.from_java_bytes(column.get_value)
        end
      rescue NotFoundError => e
        nil
      end
    
      def get_column_multi(row_keys, column_key, options={})
        thrift_exception_handler do
          selector = @keyspace.create_selector
          column_predicate = Cassandra::SlicePredicate.new
          column_predicate.addToColumn_names(::Pelops::Bytes.new(column_key.to_java_bytes).get_bytes)
          byte_row_keys = row_keys.map { |rk| ::Pelops::Bytes.new(rk.to_java_bytes) }
          result = selector.get_columns_from_rows(@name, byte_row_keys, column_predicate, get_cl(options))
          result.reduce({}) do |acc, (row_key, columns)|
            columns_h = columns_to_h(columns)
            acc[String.from_java_bytes(row_key.to_byte_array)] = columns_h unless columns_h.empty?
            acc
          end
        end
      end
    
    private
  
      def columns_to_h(columns)
        columns.reduce({}) do |acc, column|
          key   = String.from_java_bytes(column.get_name)
          value = String.from_java_bytes(column.get_value)
          acc[key] = value
          acc
        end
      end
  
      def get_cl(options)
        cl = options.fetch(:consistency_level, options.fetch(:cl, :one))
        Cassandra::CONSISTENCY_LEVELS[cl]
      end
    end
  end
end
