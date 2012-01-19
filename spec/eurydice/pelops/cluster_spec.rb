require_relative '../../spec_helper'


module Eurydice
  module Pelops
    describe Cluster do
      before :all do
        @cluster = Eurydice.connect
      end
        
      it_behaves_like 'Cluster', @cluster
    end
  end
end
