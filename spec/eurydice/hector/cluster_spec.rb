require_relative '../../spec_helper'


module Eurydice
  module Hector
    describe Cluster do
      before :all do
        @cluster = Eurydice::Hector.connect
      end
        
      it_behaves_like 'Cluster', @cluster
    end
  end
end
