# encoding: utf-8

module Eurydice
  shared_examples 'Cluster' do |cluster|
    describe '#keyspace' do
      before do
        @keyspace_name = "eurydice_test_space_#{rand(1000)}"
        if @cluster.keyspaces.include?(@keyspace_name)
          @cluster.keyspace(@keyspace_name).drop!
        end
      end
      
      it 'can connect' do
        @cluster = Eurydice.connect
        @cluster.should be_connected
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