require_relative '../../spec_helper'


module Eurydice
  module Pelops
    describe Keyspace do
      before :all do
        @keyspace_name = "eurydice_test_space_#{rand(1000)}"
        @cluster = Eurydice.connect
        if @cluster.keyspaces.include?(@keyspace_name)
          @cluster.keyspace(@keyspace_name).drop!
        end
        @cluster
      end
        
      it_behaves_like 'Keyspace'
    end
  end
end
    