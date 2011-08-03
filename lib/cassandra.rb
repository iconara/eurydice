# encoding: utf-8

module Thrift
  import 'org.apache.thrift.transport.TTransportException'
end

module Cassandra
  import 'org.apache.cassandra.thrift.ConsistencyLevel'
  import 'org.apache.cassandra.thrift.IndexType'
  import 'org.apache.cassandra.thrift.Column'
  import 'org.apache.cassandra.thrift.KsDef'
  import 'org.apache.cassandra.thrift.CfDef'
  import 'org.apache.cassandra.thrift.ColumnDef'
  import 'org.apache.cassandra.thrift.InvalidRequestException'
  import 'org.apache.cassandra.thrift.SlicePredicate'
  
  CONSISTENCY_LEVELS = {
    :one    => Cassandra::ConsistencyLevel::ONE,
    :quorum => Cassandra::ConsistencyLevel::QUORUM,
    :all    => Cassandra::ConsistencyLevel::ALL,
    :any    => Cassandra::ConsistencyLevel::ANY
  }.freeze

  MARSHAL_TYPES = {
    :bytes        => 'org.apache.cassandra.db.marshal.BytesType'.freeze,
    :ascii        => 'org.apache.cassandra.db.marshal.AsciiType'.freeze,
    :utf8         => 'org.apache.cassandra.db.marshal.UTF8Type'.freeze,
    :long         => 'org.apache.cassandra.db.marshal.LongType'.freeze,
    :lexical_uuid => 'org.apache.cassandra.db.marshal.LexicalUUIDType'.freeze,
    :time_uuid    => 'org.apache.cassandra.db.marshal.TimeUUIDType'.freeze
  }.freeze
  
  class KsDef
    def self.from_h(h)
      ks_def = h.reduce(self.new) do |ks_def, (field_name, field_value)|
        field = self::_Fields.find_by_name(field_name.to_s)
        ks_def.set_field_value(field, field_value)
        ks_def
      end
      ks_def.cf_defs = java.util.Collections.emptyList
      ks_def
    end
    
    def to_h
      self.class.metaDataMap.reduce({}) do |acc, (field, field_meta_data)|
        field_name = field.field_name.to_sym
        case field_name.to_sym
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
    def self.from_h(h)
      h.reduce(self.new) do |cf_def, (field_name, field_value)|
        case field_name.to_sym
        when :column_type
          field_value = field_value.to_s.capitalize
        when :default_validation_class
          field_value = Cassandra::MARSHAL_TYPES.fetch(field_value, field_value)
        when :comparator_type
          field_value = Cassandra::MARSHAL_TYPES.fetch(field_value, field_value)
        when :subcomparator_type
          field_value = Cassandra::MARSHAL_TYPES.fetch(field_value, field_value)
        when :column_metadata
          field_value = field_value.map do |column_name, column_def_h|
            Cassandra::ColumnDef.from_h(column_def_h.merge(:name => column_name))
          end
        end
        field = self::_Fields.find_by_name(field_name.to_s)
        cf_def.set_field_value(field, field_value)
        cf_def
      end
    end
    
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
    def self.from_h(h)
      h.reduce(self.new) do |col_def, (field_name, field_value)|
        case field_name.to_sym
        when :name
          field_value = Pelops::Bytes.new(field_value.to_s.to_java_bytes).bytes
        when :index_type
          field_value = Cassandra::IndexType.valueOf(field_value.to_s.upcase)
        when :validation_class
          field_value = MARSHAL_TYPES.fetch(field_value, field_value)
        end
        field = self::_Fields.find_by_name(field_name.to_s)
        col_def.set_field_value(field, field_value)
        col_def
      end
    end
    
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
