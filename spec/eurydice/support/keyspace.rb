# encoding: utf-8

module Eurydice
  shared_examples 'Keyspace' do
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
        @keyspace.definition(true)[:strategy_options][:replication_factor].should == 2
      end
        
      it 'creates a whole schema' do
        @keyspace = @cluster.keyspace(@keyspace_name, :create => false)
        @keyspace.create!(
          :strategy_class => 'org.apache.cassandra.locator.NetworkTopologyStrategy',
          :strategy_options => {:dc1 => 1, :dc2 => 2},
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
        definition[:strategy_options].should == {:dc1 => 1, :dc2 => 2}
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
        definition[:strategy_class].should == 'org.apache.cassandra.locator.SimpleStrategy'
        definition[:strategy_options].should == {:replication_factor => 1}
      end
    end
  
    describe '#batch' do
      before do
        @keyspace = @cluster.keyspace(@keyspace_name)
        @cf1 = @keyspace.column_family('cf1')
        @cf2 = @keyspace.column_family('cf2')
      end
      
      it 'starts and executes a batch' do
        @keyspace.batch do
          @cf1.insert('row1', 'foo' => 'bar', 'baz' => 'qux')
          @cf1.insert('row2', 'xyz' => '123', 'abc' => '123')
          @cf2.insert('item1', 'hello' => 'world')
          @cf1.delete_column('row2', 'abc')
          @cf1.get('row1').should be_nil
          @cf1.get('row2').should be_nil
          @cf2.get('item1').should be_nil
        end
        @cf1.get('row1').should == {'foo' => 'bar', 'baz' => 'qux'}
        @cf1.get('row2').should == {'xyz' => '123'}
        @cf2.get('item1').should == {'hello' => 'world'}
      end
      
      it 'only has one active batch' do
        @keyspace.batch do
          @keyspace.batch do
            @keyspace.batch do
              @cf1.insert('row1', 'foo' => 'bar')
            end
            @cf1.get('row1').should be_nil
            @keyspace.batch do
              @cf1.insert('row1', 'baz' => 'qux')
            end
            @cf1.get('row1').should be_nil
          end
          @cf1.get('row1').should be_nil
        end
        @cf1.get('row1').should == {'foo' => 'bar', 'baz' => 'qux'}
      end
      
      context 'conflicting batch options' do
        it 'complains when the options given to the #batch contain different consistency levels' do
          expect {
            @keyspace.batch(:cl => :one) do
              @keyspace.batch(:cl => :quorum) do
                @cf1.insert('row1', 'foo' => 'bar')
              end
            end
          }.to raise_error(BatchError)
        end

        it 'complains when the options given to a mutation call has different consistency levels than the options to the #batch call' do
          expect {
            @keyspace.batch(:cl => :one) do
              @keyspace.batch do
                @cf1.insert('row1', {'foo' => 'bar'}, {:cl => :quorum})
              end
            end
          }.to raise_error(BatchError)
        end
      end
    end
  end
end