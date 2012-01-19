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
      end
    
      def drop!
      end
    
      def truncate!
      end
    
      def delete(row_key, options={})
      end
    
      def delete_column(row_key, column_key, options={})
      end
    
      def delete_columns(row_key, column_keys, options={})
      end
    
      def update(row_key, properties, options={})
      end
    
      def increment(row_key, column_key, amount=1, options={})
      end
    
      def key?(row_key, options={})
      end
    
      def get(row_or_rows, options={})
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
    end
  end
end
