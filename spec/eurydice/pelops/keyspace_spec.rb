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
    