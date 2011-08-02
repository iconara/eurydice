require_relative '../spec_helper'

require 'eurydice/pelops'


module Eurydice
  describe Eurydice do
    before do
      @cluster = Eurydice.connect
    end

    describe Cluster do
      it 'can connect' do
        @cluster.should be_connected
      end
    
      describe '#keyspace' do
        before do
          if @cluster.keyspaces.include?('eurydice_test_space')
            @cluster.keyspace('eurydice_test_space').drop!
          end
        end
      
        it 'creates a keyspace' do
          keyspace = @cluster.keyspace('eurydice_test_space')
          keyspace.exists?.should be_true
        end
    
        it 'defers the creation of a keyspace with :create => false' do
          keyspace = @cluster.keyspace('eurydice_test_space', :create => false)
          keyspace.exists?.should be_false
          keyspace.create!
          keyspace.exists?.should be_true
        end
      end
    end
  
    describe Keyspace do
      before do
        if @cluster.keyspaces.include?('eurydice_test_space')
          @cluster.keyspace('eurydice_test_space').drop!
        end
      end
      
      describe '#create!' do
        it 'creates a keyspace with a specific strategy class' do
          keyspace = @cluster.keyspace('eurydice_test_space', :create => false)
          keyspace.create!(:strategy_class => 'org.apache.cassandra.locator.NetworkTopologyStrategy')
          keyspace.definition(true)[:strategy_class].should == 'org.apache.cassandra.locator.NetworkTopologyStrategy'
        end
      end
    
      describe '#drop!' do
        it 'drops a keyspace' do
          keyspace = @cluster.keyspace('eurydice_test_space')
          keyspace.drop!
          keyspace.exists?.should be_false
        end
      end
      
      describe '#definition' do
        it 'returns keyspace metadata' do
          definition = @cluster.keyspace('eurydice_test_space').definition
          definition[:name].should == 'eurydice_test_space'
          definition[:strategy_class].should == 'org.apache.cassandra.locator.LocalStrategy'
        end
      end
    end
  
    describe ColumnFamily do
      before do
        @keyspace = @cluster.keyspace('eurydice_test_space')
        if @keyspace.column_families.include?('test_family')
          @keyspace.column_family('test_family').drop!
        end
      end
      
      describe '#create!' do
        it 'can create a column family' do
          cf = @keyspace.column_family('test_family')
          cf.exists?.should be_true
        end
        
        it 'defers the creation of a keyspace with :create => false' do
          cf = @keyspace.column_family('test_family', :create => false)
          cf.exists?.should be_false
          cf.create!
          cf.exists?.should be_true
        end
        
        it 'creates a column family with a specific default validation class' do
          cf = @keyspace.column_family('test_family', :create => false)
          cf.create!(:default_validation_class => 'org.apache.cassandra.db.marshal.AsciiType')
          cf.definition(true)[:default_validation_class].should == 'org.apache.cassandra.db.marshal.AsciiType'
        end
      end
      
      describe '#drop!' do
        it 'drops the column family' do
          cf = @keyspace.column_family('test_family')
          cf.drop!
          cf.exists?.should_not be_true
        end
      end
      
      describe '#truncate!' do
        before do
          @cf = @keyspace.column_family('test_family')
        end
        
        it 'removes all rows' do
          @cf.insert('ABC', {'test' => 'abc'})
          @cf.insert('DEF', {'test' => 'def'})
          @cf.insert('GHI', {'test' => 'ghi'})
          @cf.truncate!
          @cf.get('ABC').should be_nil
          @cf.get('DEF').should be_nil
          @cf.get('GHI').should be_nil
        end
      end
      
      describe '#definition' do
        before do
          @cf = @keyspace.column_family('test_family')
        end
        
        it 'returns column family metadata' do
          definition = @cf.definition
          definition[:name].should == 'test_family'
          definition[:default_validation_class].should == 'org.apache.cassandra.db.marshal.BytesType'
        end
      end
      
      describe '#key?' do
        before do
          @cf = @keyspace.column_family('test_family')
          @cf.truncate!
        end
        
        it 'returns true if a row with the specified key exists' do
          @cf.insert('ABC', 'xyz' => 'def')
          @cf.key?('ABC').should be_true
        end

        it 'returns false if a row with the specified key does not exist' do
          @cf.key?('XYZ').should be_false
        end

        it 'returns false if a row has no columns' do
          @cf.insert('ABC', 'xyz' => 'def')
          @cf.delete_column('ABC', 'xyz')
          @cf.key?('ABC').should be_false
        end
        
        it 'is aliased as #row_exists?' do
          @cf.insert('ABC', 'xyz' => 'def')
          @cf.row_exists?('ABC').should be_true
        end
      end
      
      context 'loading, storing and removing' do
        before do
          @cf = @keyspace.column_family('test_family')
          @cf.truncate!
        end
        
        describe '#update/#insert' do
          it 'writes a column' do
            @cf.insert('ABC', 'xyz' => 'abc')
            @cf.get('ABC').should == {'xyz' => 'abc'}
          end

          it '#update and #insert are synonyms' do
            @cf.update('ABC', 'foo' => 'bar')
            @cf.insert('ABC', 'xyz' => 'abc')
            @cf.get('ABC').should == {'xyz' => 'abc', 'foo' => 'bar'}
          end

          it 'writes many columns' do
            @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
            @cf.get('ABC').should == {'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar'}
          end
        
          it 'writes with a custom consistency level' do
            @cf.insert('ABC', {'xyz' => 'abc'}, {:consistency_level => :quorum})
            @cf.get('ABC').should == {'xyz' => 'abc'}
          end

          it 'writes with a custom consistency level (:cl is an alias for :consistency_level)' do
            @cf.insert('ABC', {'xyz' => 'abc'}, {:cl => :one})
            @cf.get('ABC').should == {'xyz' => 'abc'}
          end
        end
      
        describe '#get' do
          it 'loads a row' do
            @cf.insert('ABC', 'xyz' => 'abc')
            @cf.get('ABC').should == {'xyz' => 'abc'}
          end
        
          it 'loads all columns for a row' do
            @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
            @cf.get('ABC').should == {'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar'}
          end
        
          it 'loads with a custom consistency level' do
            @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
            @cf.get('ABC', :consistency_level => :quorum).should == {'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar'}
          end

          it 'loads with a custom consistency level (:cl is an alias for :consistency_level)' do
            @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
            @cf.get('ABC', :cl => :one).should == {'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar'}
          end
        
          it 'returns nil if no row was found' do
            @cf.get('XYZ').should be_nil
          end
        end
      
        describe '#get_column' do
          it 'loads a single column for a row' do
            @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
            @cf.get_column('ABC', 'hello').should == 'world'
          end
        
          it 'loads with a custom consistency level' do
            @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
            @cf.get_column('ABC', 'hello', :consistency_level => :quorum).should == 'world'
          end

          it 'loads with a custom consistency level (:cl is an alias for :consistency_level)' do
            @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
            @cf.get_column('ABC', 'hello', :cl => :one).should == 'world'
          end
        
          it 'returns nil if no row was found' do
            @cf.get_column('XYZ', 'abc').should be_nil
          end

          it 'returns nil if no column was found' do
            @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
            @cf.get_column('XYZ', 'abc').should be_nil
          end
        end
      
        describe '#get_column_multi' do
          it 'loads a column for multiple rows' do
            @cf.insert('ABC', 'xyz' => 'abc', 'foo' => 'bar')
            @cf.insert('DEF', 'xyz' => 'def', 'hello' => 'world')
            @cf.insert('GHI', 'xyz' => 'ghi', 'foo' => 'oof')
            @cf.get_column_multi(%w(ABC GHI), 'xyz').should == {'ABC' => {'xyz' => 'abc'}, 'GHI' => {'xyz' => 'ghi'}}
          end
        
          it 'does not include rows that do not have the specified column' do
            @cf.insert('ABC', 'foo' => 'bar')
            @cf.insert('DEF', 'xyz' => 'def', 'hello' => 'world')
            @cf.insert('GHI', 'xyz' => 'ghi', 'foo' => 'oof')
            @cf.get_column_multi(%w(ABC GHI), 'xyz').should == {'GHI' => {'xyz' => 'ghi'}}
          end

          it 'does not include rows that do not exist in the results' do
            @cf.insert('DEF', 'xyz' => 'def', 'hello' => 'world')
            @cf.insert('GHI', 'xyz' => 'ghi', 'foo' => 'oof')
            @cf.get_column_multi(%w(ABC GHI), 'xyz').should == {'GHI' => {'xyz' => 'ghi'}}
          end
        
          it 'loads with a custom consistency level' do
            @cf.insert('ABC', 'xyz' => 'abc', 'foo' => 'bar')
            @cf.insert('DEF', 'xyz' => 'def', 'hello' => 'world')
            @cf.insert('GHI', 'xyz' => 'ghi', 'foo' => 'oof')
            @cf.get_column_multi(%w(ABC GHI), 'xyz', :consistency_level => :quorum).should == {'ABC' => {'xyz' => 'abc'}, 'GHI' => {'xyz' => 'ghi'}}
          end

          it 'loads with a custom consistency level (:cl is an alias for :consistency_level)' do
            @cf.insert('ABC', 'xyz' => 'abc', 'foo' => 'bar')
            @cf.insert('DEF', 'xyz' => 'def', 'hello' => 'world')
            @cf.insert('GHI', 'xyz' => 'ghi', 'foo' => 'oof')
            @cf.get_column_multi(%w(ABC GHI), 'xyz', :cl => :quorum).should == {'ABC' => {'xyz' => 'abc'}, 'GHI' => {'xyz' => 'ghi'}}
          end
        
          it 'returns an empty hash if no rows exist' do
            @cf.get_column_multi(%w(ABC GHI), 'xyz').should == {}
          end
        end
        
        describe '#delete' do
          it 'removes a row' do
            @cf.insert('ABC', 'xyz' => 'abc')
            @cf.delete('ABC')
            @cf.get('ABC').should == nil
          end
        end
        
        describe '#delete_column' do
          it 'removes a single column' do
            @cf.insert('ABC', 'xyz' => 'abc', 'foo' => 'bar')
            @cf.delete_column('ABC', 'foo')
            @cf.get('ABC').should == {'xyz' => 'abc'}
          end
        end

        describe '#delete_columns' do
          it 'removes multiple columns' do
            @cf.insert('ABC', 'xyz' => 'abc', 'foo' => 'bar', 'hello' => 'world')
            @cf.delete_columns('ABC', %w(foo xyz))
            @cf.get('ABC').should == {'hello' => 'world'}
          end
        end
      end
    end
  end
end
