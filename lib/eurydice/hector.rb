# encoding: utf-8

require 'hector-jars'
require 'eurydice'
require 'cassandra'


module Hector
  import 'org.scale7.cassandra.pelops.Cluster'
  import 'org.scale7.cassandra.pelops.Pelops'
  import 'org.scale7.cassandra.pelops.Selector'
  import 'org.scale7.cassandra.pelops.Bytes'
  import 'org.scale7.cassandra.pelops.exceptions.InvalidRequestException'
  import 'org.scale7.cassandra.pelops.exceptions.NotFoundException'
  import 'org.scale7.cassandra.pelops.exceptions.ApplicationException'
end

module Eurydice
  module Hector
    def self.connect(options={})
    end
  
    def self.keyspace(keyspace_name)
    end
  
    def self.disconnect!
    end
  end
end

require_relative 'hector/cluster'
require_relative 'hector/keyspace'
require_relative 'hector/column_family'
