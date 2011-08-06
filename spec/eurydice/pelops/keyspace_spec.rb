require_relative '../../spec_helper'


module Eurydice
  module Pelops
    describe Keyspace do
      before do
        @cluster = Eurydice.connect
        @keyspace_name = "eurydice_test_space_#{rand(1000)}"
        if @cluster.keyspaces.include?(@keyspace_name)
          @cluster.keyspace(@keyspace_name).drop!
        end
      end
      
      after do
        @keyspace.drop! rescue nil
      end
    
      describe '#create!' do
        it 'creates a keyspace with a specific strategy class' do
          @keyspace = @cluster.keyspace(@keyspace_name, :create => false)
          @keyspace.create!(:strategy_class => 'org.apache.cassandra.locator.NetworkTopologyStrategy')
          @keyspace.definition(true)[:strategy_class].should == 'org.apache.cassandra.locator.NetworkTopologyStrategy'
        end
        
        it 'creates a keyspace with specific strategy options' do
          @keyspace = @cluster.keyspace(@keyspace_name, :create => false)
          @keyspace.create!(:strategy_options => {:replication_factor => 2})
          @keyspace.definition(true)[:strategy_options][:replication_factor].should == '2'
        end
        
        it 'creates a whole schema' do
          @keyspace = @cluster.keyspace(@keyspace_name, :create => false)
          @keyspace.create!(
            :strategy_class => 'org.apache.cassandra.locator.NetworkTopologyStrategy',
            :strategy_options => {:replication_factor => 2},
            :column_families => {
              'some_family' => {
                :comparator_type => :ascii,
                :comment => 'This is some family'
              },
              'another_family' => {
                :comparator_type => :utf8,
                :comment => 'This is another family',
                :column_metadata => {
                  'first_col' => {
                    :validation_class => :ascii,
                    :index_name => 'first_index',
                    :index_type => :keys
                  },
                  'second_col' => {
                    :validation_class => :time_uuid
                  }
                }
              }
            }
          )
          definition = @keyspace.definition(true)
          definition[:strategy_class].should == 'org.apache.cassandra.locator.NetworkTopologyStrategy'
          definition[:strategy_options].should == {:replication_factor => '2'}
          definition[:column_families]['some_family'][:comparator_type].should == 'org.apache.cassandra.db.marshal.AsciiType'
          definition[:column_families]['some_family'][:comment].should == 'This is some family'
          definition[:column_families]['another_family'][:comment].should == 'This is another family'
          definition[:column_families]['another_family'][:column_metadata]['first_col'][:validation_class].should == 'org.apache.cassandra.db.marshal.AsciiType'
          definition[:column_families]['another_family'][:column_metadata]['first_col'][:index_name].should == 'first_index'
          definition[:column_families]['another_family'][:column_metadata]['second_col'][:validation_class].should == 'org.apache.cassandra.db.marshal.TimeUUIDType'
          @keyspace.column_family('some_family', :create => false).should exist
          @keyspace.column_family('another_family', :create => false).should exist
        end
      end
  
      describe '#drop!' do
        it 'drops a keyspace' do
          @keyspace = @cluster.keyspace(@keyspace_name)
          @keyspace.drop!
          @keyspace.exists?.should be_false
        end
      end
    
      describe '#definition' do
        it 'returns keyspace metadata' do
          @keyspace = @cluster.keyspace(@keyspace_name)
          definition = @keyspace.definition
          definition[:name].should == @keyspace_name
          definition[:strategy_class].should == 'org.apache.cassandra.locator.LocalStrategy'
        end
      end
    end
  end
end
    