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
end