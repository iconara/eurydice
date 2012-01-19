# encoding: utf-8

require 'cassandra'


module Hector
  module Ddl
    import 'me.prettyprint.cassandra.model.BasicKeyspaceDefinition'
    
    import 'me.prettyprint.cassandra.service.ThriftKsDef'
    import 'me.prettyprint.cassandra.service.ThriftCfDef'
    import 'me.prettyprint.cassandra.service.ThriftColumnDef'

    import 'me.prettyprint.hector.api.ddl.KeyspaceDefinition'
    import 'me.prettyprint.hector.api.ddl.ColumnFamilyDefinition'
    import 'me.prettyprint.hector.api.ddl.ColumnDefinition'
  
    import 'me.prettyprint.hector.api.ddl.ComparatorType'
    
    COMPARATOR_TYPES = {
      :bytes                => ComparatorType.BYTESTYPE,
      :ascii                => ComparatorType.ASCIITYPE,
      :utf8                 => ComparatorType.UTF8TYPE,
      :integer              => ComparatorType.INTEGERTYPE,
      :long                 => ComparatorType.LONGTYPE,
      :uuid                 => ComparatorType.UUIDTYPE,
      :lexical_uuid         => ComparatorType.LEXICALUUIDTYPE,
      :time_uuid            => ComparatorType.TIMEUUIDTYPE,
      :counter              => ComparatorType.COUNTERTYPE,
      :counter_column       => ComparatorType.COUNTERTYPE,
      :composite            => ComparatorType.COMPOSITETYPE,
      :dynamic_composite    => ComparatorType.DYNAMICCOMPOSITETYPE,
      :local_by_partitioner => ComparatorType.LOCALBYPARTITIONERTYPE
    }.freeze
    
    REVERSE_COMPARATOR_TYPES = {
      ComparatorType.BYTESTYPE              => Cassandra::MARSHAL_TYPES[:bytes],
      ComparatorType.ASCIITYPE              => Cassandra::MARSHAL_TYPES[:ascii],
      ComparatorType.UTF8TYPE               => Cassandra::MARSHAL_TYPES[:utf8],
      ComparatorType.INTEGERTYPE            => Cassandra::MARSHAL_TYPES[:integer],
      ComparatorType.LONGTYPE               => Cassandra::MARSHAL_TYPES[:long],
      ComparatorType.UUIDTYPE               => Cassandra::MARSHAL_TYPES[:uuid],
      ComparatorType.LEXICALUUIDTYPE        => Cassandra::MARSHAL_TYPES[:lexical_uuid],
      ComparatorType.TIMEUUIDTYPE           => Cassandra::MARSHAL_TYPES[:time_uuid],
      ComparatorType.COUNTERTYPE            => Cassandra::MARSHAL_TYPES[:counter],
      ComparatorType.COUNTERTYPE            => Cassandra::MARSHAL_TYPES[:counter_column],
      ComparatorType.COMPOSITETYPE          => Cassandra::MARSHAL_TYPES[:composite],
      ComparatorType.DYNAMICCOMPOSITETYPE   => Cassandra::MARSHAL_TYPES[:dynamic_composite],
      ComparatorType.LOCALBYPARTITIONERTYPE => Cassandra::MARSHAL_TYPES[:local_by_partitioner]
    }.freeze
    
    module KeyspaceDefinition
      def self.from_h(h)
        ThriftKsDef.new(Cassandra::KsDef.from_h(h))
      end
      
      def to_h
        h = {}
        h[:name] = name
        h[:replication_factor] = replication_factor
        h[:strategy_class] = strategy_class
        h[:strategy_options] = strategy_options_h
        h[:durable_writes] = durable_writes?
        h[:column_families] = column_family_defs_h
        h
      end
      
    private
      
      def strategy_options_h
        strategy_options.reduce({}) do |acc, (property, value)|
          p = property.to_sym
          v = begin
            if numeric_strategy_options?
            then value.to_i
            else value
            end
          end
          acc[p] = v
          acc
        end
      end
      
      def numeric_strategy_options?
        @numeric_strategy_options ||= strategy_class == Cassandra::LOCATOR_STRATEGY_CLASSES[:simple] || strategy_class == Cassandra::LOCATOR_STRATEGY_CLASSES[:network_topology]
      end
      
      def column_family_defs_h
        h = {}
        cf_defs.each do |cf_def|
          h[cf_def.name] = cf_def.to_h
        end
        h
      end
    end
    
    module ColumnFamilyDefinition
      def self.from_h(h)
        ThriftCfDef.new(Cassandra::CfDef.from_h(h))
      end
      
      def to_h
        {
          :name => name,
          :comment => comment,
          :comparator_type => REVERSE_COMPARATOR_TYPES[comparator_type],
          :key_validation_class => REVERSE_COMPARATOR_TYPES[key_validation_class],
          :default_validation_class => REVERSE_COMPARATOR_TYPES[default_validation_class],
          :subcomparator_type => REVERSE_COMPARATOR_TYPES[sub_comparator_type],
          :column_metadata => column_metadata_h
        }
      end
      
    private
      
      def column_metadata_h
        h = {}
        column_metadata.each do |col_def|
          name = Eurydice::Bytes.nio_bytes_to_s(col_def.name)
          h[name] = col_def.to_h
        end
        h
      end
    end
    
    module ColumnDefinition
      def self.from_h(h)
        ThriftColumnDef.new(Cassandra::ColumnDef.from_h(h))
      end
      
      def to_h
        {
          :name => Eurydice::Bytes.nio_bytes_to_s(name),
          :validation_class => validation_class,
          :index_name => index_name,
          :index_type => index_type
        }
      end
    end
  end
end
