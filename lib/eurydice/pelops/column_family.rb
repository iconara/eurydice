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
          types = options[:validations] || {}
          key_type = options[:comparator]
          mutator = @keyspace.create_mutator
          columns = properties.map do |k, v|
            key = to_pelops_bytes(k, key_type)
            value = to_pelops_bytes(v, types[k])
            ttl = options.fetch(:ttl, mutator.class::NO_TTL)
            mutator.new_column(key, value, ttl)
          end
          mutator.write_columns(@name, row_key, columns)
          mutator.execute(get_cl(options))
        end
      end
      alias_method :insert, :update
    
      def increment(row_key, column_key, amount=1, options={})
        thrift_exception_handler do
          mutator = @keyspace.create_mutator
          mutator.write_counter_column(@name, to_pelops_bytes(row_key), to_pelops_bytes(column_key), amount)
          mutator.execute(get_cl(options))
        end
      end
      alias_method :inc, :increment
      alias_method :incr, :increment
      alias_method :increment_column, :increment
    
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
          if counter_columns?
            column = selector.get_counter_column_from_row(@name, to_pelops_bytes(row_key), to_pelops_bytes(column_key), get_cl(options))
            column.get_value
          else
            column =selector.get_column_from_row(@name, to_pelops_bytes(row_key), to_pelops_bytes(column_key), get_cl(options))
            byte_array_to_s(column.get_value)
          end
        end
      rescue NotFoundError => e
        nil
      end
      
      def each_column(row_key, options={})
        thrift_exception_handler do
          reversed = options.fetch(:reversed, false)
          batch_size = options.fetch(:batch_size, 100)
          start_beyond = options.fetch(:start_beyond, nil)
          start_beyond = to_pelops_bytes(start_beyond) if start_beyond
          selector = @keyspace.create_selector
          iterator = selector.iterate_columns_from_row(@name, to_pelops_bytes(row_key), start_beyond, reversed, batch_size, get_cl(options))
          if block_given?
            iterator.each do |column|
              yield column_to_kv(column, options)
            end
          else
            Enumerator.new do |y|
              iterator.each do |column|
                y << column_to_kv(column, options)
              end
            end
          end
        end
      end
      
      def get_column_count(row_key, options={})
        thrift_exception_handler do
          selector = @keyspace.create_selector
          column_predicate = create_column_predicate(options)
          selector.get_column_count(@name, to_pelops_bytes(row_key), column_predicate, get_cl(options))
        end
      end
      
      def get_indexed(column_key, operator, value, options={})
        thrift_exception_handler do
          selector = @keyspace.create_selector
          op = Cassandra::INDEX_OPERATORS[operator]
          max_rows = options.fetch(:max_row_count, 20)
          types = options[:validations] || {}
          key_type = options[:comparator]
          raise ArgumentError, %(Unsupported index operator: "#{operator}") unless op
          index_expression = selector.class.new_index_expression(to_pelops_bytes(column_key, key_type), op, to_pelops_bytes(value, types[column_key]))
          index_clause = selector.class.new_index_clause(empty_pelops_bytes, max_rows, index_expression)
          column_predicate = create_column_predicate(options)
          rows = selector.get_indexed_columns(@name, index_clause, column_predicate, get_cl(options))
          rows_to_h(rows, options)
        end
      end
      
    private
    
      EMPTY_STRING = ''.freeze
    
      def counter_columns?
        @is_counter_cf ||= definition[:default_validation_class] == Cassandra::MARSHAL_TYPES[:counter]
      end
    
      def get_single(row_key, options={})
        thrift_exception_handler do
          selector = @keyspace.create_selector
          column_predicate = create_column_predicate(options)
          if counter_columns?
            columns = selector.get_counter_columns_from_row(@name, to_pelops_bytes(row_key), column_predicate, get_cl(options))
          else
            columns = selector.get_columns_from_row(@name, to_pelops_bytes(row_key), column_predicate, get_cl(options))
          end
          columns_to_h(columns, options)
        end
      end
    
      def get_multi(row_keys, options={})
        thrift_exception_handler do
          selector = @keyspace.create_selector
          column_predicate = create_column_predicate(options)
          byte_row_keys = row_keys.map { |rk| to_pelops_bytes(rk) }
          if counter_columns?
            rows = selector.get_counter_columns_from_rows(@name, byte_row_keys, column_predicate, get_cl(options))
          else
            rows = selector.get_columns_from_rows(@name, byte_row_keys, column_predicate, get_cl(options))
          end
          rows_to_h(rows, options)
        end
      end
      
      def create_column_predicate(options)
        max_column_count = options.fetch(:max_column_count, java.lang.Integer::MAX_VALUE)
        reversed = options.fetch(:reversed, false)
        if options.key?(:from_column)
          raise ArgumentError, %(You can set either :columns or :from_column, but not both) if options.key?(:columns)
          options[:columns] = options[:from_column]..EMPTY_STRING
        end
        case options[:columns]
        when Range
          ::Pelops::Selector.new_columns_predicate(to_pelops_bytes(options[:columns].begin), to_pelops_bytes(options[:columns].end), reversed, max_column_count)
        when Array
          ::Pelops::Selector.new_columns_predicate(*options[:columns].map { |col| to_pelops_bytes(col) })
        else
          ::Pelops::Selector.new_columns_predicate_all(reversed, max_column_count)
        end
      end
    
      def rows_to_h(rows, options)
        rows.reduce({}) do |acc, (row_key, columns)|
          columns_h = columns_to_h(columns, options)
          acc[pelops_bytes_to_s(row_key)] = columns_h if columns_h && !columns_h.empty?
          acc
        end
      end
  
      def columns_to_h(columns, options)
        if columns.empty?
          nil
        else
          columns.reduce({}) do |acc, column|
            key, value = column_to_kv(column, options)
            acc[key] = value
            acc
          end
        end
      end
      
      def column_to_kv(column, options)
        types = options[:validations] || {}
        key_type = options[:comparator]
        key = byte_array_to_s(column.get_name, key_type)
        value = if counter_columns? 
          then column.get_value 
          else byte_array_to_s(column.get_value, types[key])
        end
        return key, value
      end
  
      def get_cl(options)
        cl = options.fetch(:consistency_level, options.fetch(:cl, :one))
        Cassandra::CONSISTENCY_LEVELS[cl]
      end
    end
  end
end
