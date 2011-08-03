# encoding: utf-8

module Cassandra
  import 'org.apache.cassandra.thrift.ConsistencyLevel'
  import 'org.apache.cassandra.thrift.IndexType'
  import 'org.apache.cassandra.thrift.Column'
  import 'org.apache.cassandra.thrift.KsDef'
  import 'org.apache.cassandra.thrift.CfDef'
  import 'org.apache.cassandra.thrift.ColumnDef'
  import 'org.apache.cassandra.thrift.InvalidRequestException'
  import 'org.apache.cassandra.thrift.SlicePredicate'
  
  class KsDef
    def to_h
      self.class.metaDataMap.reduce({}) do |acc, (field, field_meta_data)|
        field_name = field.field_name.to_sym
        case field_name
        when :cf_defs
          cf_hs = get_field_value(field).map { |cf_def| cf_def.to_h }
          acc[:column_families] = Hash[cf_hs.map { |cf_h| [cf_h[:name], cf_h] }]
        else
          acc[field_name] = get_field_value(field)
        end
        acc
      end
    end
  end
  
  class CfDef
    def to_h
      self.class.metaDataMap.reduce({:column_metadata => {}}) do |acc, (field, field_meta_data)|
        field_name = field.field_name.to_sym
        case field_name
        when :column_metadata
          column_hs = get_field_value(field).map { |col_def| col_def.to_h }
          acc[field_name] = Hash[column_hs.map { |col_h| [col_h[:name], col_h] }]
        when :column_type
          acc[field_name] = get_field_value(field).downcase.to_sym
        else
          acc[field_name] = get_field_value(field)
        end
        acc
      end
    end
  end
  
  class ColumnDef
    def to_h
      self.class.metaDataMap.reduce({}) do |acc, (field, field_meta_data)|
        field_name = field.field_name.to_sym
        acc[field_name] = begin
          case field_name
          when :name
            String.from_java_bytes(Pelops::Bytes.new(get_field_value(field)).to_byte_array)
          when :index_type
            value = get_field_value(field)
            value.toString.downcase.to_sym if value
          else
            get_field_value(field)
          end
        end
        acc
      end
    end
  end
end