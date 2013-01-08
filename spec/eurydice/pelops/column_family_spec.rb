require_relative '../../spec_helper'


module Eurydice
  module Pelops
    describe ColumnFamily do
      before :all do
        @keyspace_name = "eurydice_test_space_#{rand(1000)}"
        @cf_name = "column_family_#{rand(1000)}"
        @cluster = Eurydice.connect(host: ENV['CASSANDRA_HOST'])
        @keyspace = @cluster.keyspace(@keyspace_name, :create => false)
        @keyspace.drop! rescue nil
        @keyspace.create!
      end
      
      after :all do
        @keyspace.drop! rescue nil
      end
  
      it_behaves_like 'ColumnFamily'
    end
  end
end
