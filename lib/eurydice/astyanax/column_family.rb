# encoding: utf-8

module Eurydice
  module Astyanax
    class ColumnFamily
      attr_reader :name

      def initialize(keyspace, name)
        @keyspace = keyspace
        @name = name
      end

      def definition(reload=true)
        @definition = nil if reload
        @definition ||= @keyspace.definition(true)[:column_families][@name]
      end

      def create!(options={})
        cf_properties = options.merge(:keyspace => @keyspace.name, :name => @name)
        cf_def = Cassandra::CfDef.from_h(cf_properties)
        @keyspace.add_column_family(cf_def)
      end

      def exists?
        !!definition(true)
      end

      def drop!
        @keyspace.drop_column_family(@name)
      end
    
      def truncate!
        @keyspace.keyspace_client.truncate_column_family(cf_client)
      end
    
      def delete(row_key, options={})
        @keyspace.batch(options) do |b|
          row = b.with_row(cf_client, to_bytes(row_key))
          row.delete
        end
      end
    
      def delete_column(row_key, column_key, options={})
        delete_columns(row_key, [column_key], options)
      end
    
      def delete_columns(row_key, column_keys, options={})
        @keyspace.batch(options) do |b|
          row = b.with_row(cf_client, to_bytes(row_key))
          column_keys.each do |column_key|
            row.delete_column(to_bytes(column_key))
          end
        end
      end
    
      def update(row_key, properties, options={})
        @keyspace.batch(options) do |b|
          row = b.with_row(cf_client, to_bytes(row_key))
          properties.each do |key, value|
            case value
            when Integer
              row.put_column(to_bytes(key), value, options[:ttl])
            when nil
              row.put_empty_column(to_bytes(key), options[:ttl])
            else
              row.put_column(to_bytes(key), to_bytes(value), options[:ttl])
            end
          end
        end
      end
      alias_method :insert, :update
    
      def increment(row_key, column_key, amount=1, options={})
        @keyspace.batch(options) do |b|
          row = b.with_row(cf_client, to_bytes(row_key))
          row.increment_counter_column(to_bytes(column_key), amount)
        end
      end
      alias_method :inc, :increment
      alias_method :incr, :increment
      alias_method :increment_column, :increment
    
      def key?(row_key, options={})
        !!get(row_key, options.merge(:max_column_count => 1))
      end
      alias_method :row_exists?, :key?
    
      def get(row_or_rows, options={})
        case row_or_rows
        when Array
          query = prepare_query.get_key_slice(row_or_rows.map { |r| to_bytes(r) })
          query.with_column_slice(create_column_slice(options))
          rows_to_h(query.execute.result, options)
        else
          query = prepare_query.get_key(to_bytes(row_or_rows))
          query.with_column_slice(create_column_slice(options))
          columns_to_h(query.execute.result, options)
        end
      end
      alias_method :get_row, :get
      alias_method :get_rows, :get
      
      def get_column(row_key, column_key, options={})
        # TODO: is there a more optimal path for this?
        result = get(row_key, options.merge(:columns => [column_key]))
        result[column_key] if result
      end

      def each_column(row_key, options={})
        options = options.merge(:from_column => options[:start_beyond]) if options[:start_beyond]
        query = prepare_query.get_key(to_bytes(row_key))
        query.with_column_slice(create_column_slice(options))
        query.auto_paginate(true)

        # TODO: set page size (RangeBuilder#setMaxSize)

        enum = ColumnEnumerator.new(query, options)

        if block_given?
          enum.each(&Proc.new)
        else
          enum
        end
      end
      
      def get_column_count(row_key, options={})
        query = prepare_query.get_key(to_bytes(row_key))
        query.with_column_slice(create_column_slice(options))
        query.get_count.execute.result
      end
      
      def get_indexed(column_key, operator, value, options={})
        index_query = prepare_query.search_with_index
        index_query.set_row_limit(options[:max_row_count]) if options[:max_row_count]
        expr = index_query.add_expression.where_column(to_bytes(column_key))
        expr = begin
          case operator
          when :==, :eq  then expr.equals
          when :>,  :gt  then expr.greater_than
          when :>=, :gte then expr.greater_than_equals
          when :<,  :lt  then expr.less_than
          when :<=, :lte then expr.less_than_equals
          else
            raise ArgumentError, %(Unsupported index operator: "#{operator}")
          end
        end
        expr.value(value)
        rows_to_h(index_query.execute.result, options)
      end

    private

      FIRST_COLUMN = "\0".freeze
      LAST_COLUMN = ''.freeze
      UINT64 = 'Q'.freeze

      module BytesHelper
        def to_bytes(obj)
          case obj
          when String
            obj.to_java_bytes
          when Integer
            to_bytes([obj].pack(UINT64))
          else
            raise ArgumentError, %[Cannot convert #{obj.class} to bytes]
          end
        end

        def from_bytes(bytes, type)
          str = String.from_java_bytes(bytes)
          case type
          when :long
            str.unpack(UINT64).first
          else
            str
          end
        end
      end

      include BytesHelper

      class ColumnEnumerator
        include Enumerable
        include BytesHelper

        def initialize(query, options)
          @query = query
          @options = options
        end

        def each
          return self unless block_given?
          comparator = @options[:comparator]
          validations = @options[:validations] || {}
          last_column = nil
          until (column_list = @query.execute.result).empty?
            # TODO: this seems to be a bug when iterating with :reverse
            break if from_bytes(column_list.get_column_by_index(column_list.size - 1).name, comparator) == last_column
            column_list.each do |column|
              column_key = from_bytes(column.name, comparator)
              unless @options[:start_beyond] && column_key == @options[:start_beyond]
                column_value = from_bytes(column.byte_array_value, validations[column_key])
                yield [column_key, column_value]
                last_column = column_key
              end
            end
          end
        end
      end

      def counter_columns?
        @is_counter_cf ||= definition[:default_validation_class] == Cassandra::MARSHAL_TYPES[:counter]
      end

      def cf_client
        @cf_client ||= ::Astyanax::ColumnFamily.new_column_family(@name, ::Astyanax::BytesArraySerializer.get, ::Astyanax::BytesArraySerializer.get)
      end

      def prepare_query
        @keyspace.keyspace_client.prepare_query(cf_client)
      end

      def columns_to_h(column_list, options={})
        return nil if column_list.size == 0
        key_type = options[:comparator]
        value_types = options[:validations] || {}
        column_list.reduce({}) do |acc, column|
          column_key = from_bytes(column.name, key_type)
          value = begin
            if counter_columns?
              column.long_value
            elsif value_types[column_key] == :long
              column.long_value
            elsif value_types[column_key]
              raise ArgumentError, %[Unsupported validation for "#{column_key}": "#{value_types[column_key]}"]
            else
              v = String.from_java_bytes(column.byte_array_value)
              v = nil if v && v.empty?
              v
            end
          end
          acc[column_key] = value
          acc
        end
      end

      def rows_to_h(row_list, options={})
        return nil if row_list.size == 0
        row_list.reduce({}) do |row_acc, row|
          row_acc[from_bytes(row.key, nil)] = columns_to_h(row.columns, options) if row.columns.size > 0
          row_acc
        end
      end

      def create_column_slice(options)
        raise ArgumentError, %(You can set either :columns or :from_column, but not both) if options.key?(:from_column) && options.key?(:columns)

        first_column = options.fetch(:from_column, FIRST_COLUMN)
        last_column = LAST_COLUMN

        if options[:columns].is_a?(Range)
          first_column, last_column = options[:columns].begin, options[:columns].end
        end

        if options[:reversed]
          first_column, last_column = last_column, first_column
        end

        column_slice = begin
          case options[:columns]
          when Array
            ::Astyanax::ColumnSlice.new(options[:columns].map { |c| to_bytes(c) })
          else
            ::Astyanax::ColumnSlice.new(to_bytes(first_column), to_bytes(last_column))
          end
        end

        column_slice.set_limit(options[:max_column_count]) if options[:max_column_count]
        column_slice.set_reversed(options[:reversed]) if options.key?(:reversed)
        column_slice
      end
    end
  end
end