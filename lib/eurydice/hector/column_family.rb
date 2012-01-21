# encoding: utf-8

module Eurydice
  module Hector
    class ColumnFamily
      attr_reader :name, :keyspace
    
      def initialize(name, keyspace)
        @name = name
        @keyspace = keyspace
      end
    
      def definition(reload=true)
        @keyspace.definition(reload)[:column_families][@name]
      end
    
      def exists?
        !!definition(true)
      end
    
      def create!(options={})
        @keyspace.add_column_family(options.merge(:name => @name))
      end
    
      def drop!
        @keyspace.drop_column_family(@name)
      end
    
      def truncate!
        @keyspace.truncate_column_family(@name)
      end
    
      def delete(row_key, options={})
      end
    
      def delete_column(row_key, column_key, options={})
      end
    
      def delete_columns(row_key, column_keys, options={})
      end
    
      def update(row_key, properties, options={})
        raise NotImplementedError, %(Consistency level settings not yet supported) if options.key?(:cl) || options.key?(:consistency_level)
        updater = template.create_updater(to_byte_array(row_key))
        comparator = options[:comparator] || :bytes_array
        validations = options[:validations] || Hash.new(:bytes_array)
        properties.each do |k, v|
          key_serializer = create_serializer(comparator)
          value_serializer = create_serializer(validations[k])
          key = begin
            case comparator
            when :bytes_array then to_byte_array(k)
            else k
            end
          end
          value = begin
            case validations[k]
            when :bytes_array then to_byte_array(v)
            else v
            end
          end
          column = ::Hector::HFactory.create_column(key, value, key_serializer, value_serializer)
          column.set_ttl(options[:ttl]) if options[:ttl]
          updater.set_column(column)
        end
        template.update(updater)
      end
    
      def increment(row_key, column_key, amount=1, options={})
      end
    
      def key?(row_key, options={})
        template.columns_exist?(to_byte_array(row_key))
      end
    
      def get(row_or_rows, options={})
        result = template.query_columns(to_byte_array(row_or_rows))
        return nil unless result.has_results
        comparator = options[:comparator] || :bytes_array
        validations = options[:validations] || Hash.new(:bytes_array)
        row = {}
        result.column_names.each do |column_name|
          key = byte_array_to_s(column_name, comparator)
          value = nio_bytes_to_s(result.get_column(column_name).value_bytes, validations[key])
          row[key] = value
        end
        row
      end

      def get_column(row_key, column_key, options={})
      end
      
      def each_column(row_key, options={})
      end
      
      def get_column_count(row_key, options={})
      end
      
      def get_indexed(column_key, operator, value, options={})
      end
      
      include ColumnFamilyMethodAliases
      include Bytes
      
    private
      
      def template
        @template ||= begin
          row_key_serializer = create_serializer(:bytes_array)
          column_key_serializer = create_serializer(:bytes_array)
          @keyspace.create_template(@name, row_key_serializer, column_key_serializer)
        end
      end
      
      def create_serializer(type)
        case type
        when :string      then ::Hector::Serializer::StringSerializer.get
        when :long        then ::Hector::Serializer::LongSerializer.get
        when :bytes_array then ::Hector::Serializer::BytesArraySerializer.get
        else
          raise NotImplementedError, %(No serializer mapping for #{type})
        end
      end
      
      def deserialize(value, type)
        p value
        case type
        when :string then nil
        else
          raise NotImplementedError, %(No deserializer mapping for #{type})
        end
      end
    end
  end
end
