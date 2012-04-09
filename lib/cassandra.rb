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
  import 'org.apache.cassandra.thrift.SliceRange'
  import 'org.apache.cassandra.thrift.IndexOperator'
  
  CONSISTENCY_LEVELS = {
    :any          => Cassandra::ConsistencyLevel::ANY,
    :one          => Cassandra::ConsistencyLevel::ONE,
    :two          => Cassandra::ConsistencyLevel::TWO,
    :three        => Cassandra::ConsistencyLevel::THREE,
    :local_quorum => Cassandra::ConsistencyLevel::LOCAL_QUORUM,
    :each_quorum  => Cassandra::ConsistencyLevel::EACH_QUORUM,
    :quorum       => Cassandra::ConsistencyLevel::QUORUM,
    :all          => Cassandra::ConsistencyLevel::ALL
  }.freeze

  MARSHAL_TYPES = {
    :bytes          => 'org.apache.cassandra.db.marshal.BytesType'.freeze,
    :ascii          => 'org.apache.cassandra.db.marshal.AsciiType'.freeze,
    :utf8           => 'org.apache.cassandra.db.marshal.UTF8Type'.freeze,
    :long           => 'org.apache.cassandra.db.marshal.LongType'.freeze,
    :lexical_uuid   => 'org.apache.cassandra.db.marshal.LexicalUUIDType'.freeze,
    :time_uuid      => 'org.apache.cassandra.db.marshal.TimeUUIDType'.freeze,
    :counter        => 'org.apache.cassandra.db.marshal.CounterColumnType'.freeze,
    :counter_column => 'org.apache.cassandra.db.marshal.CounterColumnType'.freeze
  }.freeze
  
  INDEX_OPERATORS = {
    :==  => IndexOperator::EQ,
    :eq  => IndexOperator::EQ,
    :>   => IndexOperator::GT,
    :gt  => IndexOperator::GT,
    :>=  => IndexOperator::GTE,
    :gte => IndexOperator::GTE,
    :<   => IndexOperator::LT,
    :lt  => IndexOperator::LT,
    :<=  => IndexOperator::LTE,
    :lte => IndexOperator::LTE
  }.freeze
  
  LOCATOR_STRATEGY_CLASSES = {
    :simple           => 'org.apache.cassandra.locator.SimpleStrategy'.freeze,
    :network_topology => 'org.apache.cassandra.locator.NetworkTopologyStrategy'.freeze
  }.freeze
  
  class KsDef
    def self.from_h(h)
      ks_def = h.reduce(self.new) do |ks_def, (field_name, field_value)|
        case field_name.to_sym
        when :strategy_options
          field_value = Hash[field_value.map { |k, v| [k.to_s, v.to_s] }]
        when :column_families
          field_name = 'cf_defs'
          field_value = field_value.map { |cf_name, cf_def_h| CfDef.from_h(cf_def_h.merge(:name => cf_name, :keyspace => h[:name])) }
        end
        field = self::_Fields.find_by_name(field_name.to_s)
        raise ArgumentError, %(No field named "#{field_name}") unless field
        ks_def.set_field_value(field, field_value)
        ks_def
      end
      ks_def.cf_defs = java.util.Collections.emptyList unless ks_def.cf_defs
      ks_def
    end
    
    def to_h
      self.class.metaDataMap.reduce({}) do |acc, (field, field_meta_data)|
        field_name = field.field_name.to_sym
        field_value = get_field_value(field)
        case field_name.to_sym
        when :cf_defs
          cf_hs = field_value.map { |cf_def| cf_def.to_h }
          acc[:column_families] = Hash[cf_hs.map { |cf_h| [cf_h[:name], cf_h] }]
        when :strategy_options
          acc[field_name] = Hash[field_value.map { |pair| [pair.first.to_sym, pair.last] }] # JRuby 1.6.2 Java Map doesn't splat when yielding
        else
          acc[field_name] = field_value
        end
        acc
      end.tap do |h|
        if h[:strategy_class] == LOCATOR_STRATEGY_CLASSES[:simple] || h[:strategy_class] == LOCATOR_STRATEGY_CLASSES[:network_topology]
          h[:strategy_options].keys.each do |k|
            h[:strategy_options][k] = h[:strategy_options][k].to_i
          end
        end
      end
    end
  end
  
  class CfDef
    def self.from_h(h)
      h.reduce(self.new) do |cf_def, (field_name, field_value)|
        case field_name.to_sym
        when :column_type
          field_value = field_value.to_s.capitalize
        when :key_validation_class, :default_validation_class, :comparator_type, :subcomparator_type
          field_value = Cassandra::MARSHAL_TYPES.fetch(field_value, field_value)
        when :column_metadata
          field_value = field_value.map do |column_name, column_def_h|
            Cassandra::ColumnDef.from_h(column_def_h.merge(:name => column_name))
          end
        end
        field = self::_Fields.find_by_name(field_name.to_s)
        raise ArgumentError, %(No field named "#{field_name}") unless field
        cf_def.set_field_value(field, field_value)
        cf_def
      end
    end
    
    def to_h
      self.class.metaDataMap.reduce({:column_metadata => {}}) do |acc, (field, field_meta_data)|
        field_name = field.field_name.to_sym
        case field_name
        when :column_metadata
          value = get_field_value(field)
          if value
            column_hs = value.map { |col_def| col_def.to_h }
            acc[field_name] = Hash[column_hs.map { |col_h| [col_h[:name], col_h] }]
          end
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
          field_value = Eurydice::Pelops::ByteHelpers.to_nio_bytes(field_value)
        when :index_type
          field_value = Cassandra::IndexType.valueOf(field_value.to_s.upcase)
        when :validation_class
          field_value = MARSHAL_TYPES.fetch(field_value, field_value)
        end
        field = self::_Fields.find_by_name(field_name.to_s)
        raise ArgumentError, %(No field named "#{field_name}") unless field
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
            Eurydice::Pelops::ByteHelpers.nio_bytes_to_s(get_field_value(field))
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
