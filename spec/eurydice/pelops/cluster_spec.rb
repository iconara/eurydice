require_relative '../../spec_helper'


module Eurydice
  module Pelops
    describe Cluster do
      it 'can connect' do
        @cluster = Eurydice.connect
        @cluster.should be_connected
      end
  
      describe '#keyspace' do
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
    
        it 'creates a keyspace' do
          @keyspace = @cluster.keyspace(@keyspace_name)
          @keyspace.exists?.should be_true
        end
  
        it 'defers the creation of a keyspace with :create => false' do
          @keyspace = @cluster.keyspace(@keyspace_name, :create => false)
          @keyspace.exists?.should be_false
          @keyspace.create!
          @keyspace.exists?.should be_true
        end
      end
    end
  end
end
