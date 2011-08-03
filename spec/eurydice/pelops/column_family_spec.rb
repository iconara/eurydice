require_relative '../../spec_helper'


module Eurydice
  module Pelops
    describe ColumnFamily do
      before do
        @cluster = Eurydice.connect
        @keyspace_name = "eurydice_test_space_#{rand(1000)}"
        @keyspace = @cluster.keyspace(@keyspace_name)
        if @keyspace.column_families.include?('test_family')
          sleep(1) # dropping too soon after creating confuses Cassandra
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

        marshal_types = {
          'a fully qualified name' => 'org.apache.cassandra.db.marshal.UTF8Type',
          'the package name omitted' => 'UTF8Type',
          'an alias' => :utf8
        }

        context 'creating a column family with a specific comparator type' do
          marshal_types.each do |desc, type|
            it "with #{desc}" do
              cf = @keyspace.column_family('test_family', :create => false)
              cf.create!(:comparator_type => type)
              cf.definition(true)[:comparator_type].should == 'org.apache.cassandra.db.marshal.UTF8Type'
            end
          end
        end

        context 'creating a column family with a specific subcomparator type' do
          marshal_types.each do |desc, type|
            it "with #{desc}" do
              cf = @keyspace.column_family('test_family', :create => false)
              cf.create!(:column_type => :super, :subcomparator_type => type)
              cf.definition(true)[:subcomparator_type].should == 'org.apache.cassandra.db.marshal.UTF8Type'
            end
          end
        end

        context 'creating a column family with a specific default validation class' do
          marshal_types.each do |desc, type|
            it "with #{desc}" do
              cf = @keyspace.column_family('test_family', :create => false)
              cf.create!(:default_validation_class => type)
              cf.definition(true)[:default_validation_class].should == 'org.apache.cassandra.db.marshal.UTF8Type'
            end
          end
        end
    
        context 'creating a column family with a specific validation class for a column' do
          marshal_types.each do |desc, type|
            it "with #{desc}" do
              cf = @keyspace.column_family('test_family', :create => false)
              cf.create!(:column_metadata => {'xyz' => {:validation_class => type}})
              cf.definition(true)[:column_metadata]['xyz'][:validation_class].should == 'org.apache.cassandra.db.marshal.UTF8Type'
            end
          end
        end
    
        it 'creates a column family with an index' do
          cf = @keyspace.column_family('test_family', :create => false)
          cf.create!(:column_metadata => {'xyz' => {:index_name => 'abc', :index_type => :keys, :validation_class => :ascii}})
          cf.definition(true)[:column_metadata]['xyz'][:index_name].should == 'abc'
        end
    
        it 'creates a column family with a specific column type' do
          cf = @keyspace.column_family('test_family', :create => false)
          cf.create!(:column_type => :super)
          cf.definition(true)[:column_type].should == :super
        end
      end
  
      describe '#drop!' do
        it 'drops the column family' do
          cf = @keyspace.column_family('test_family')
          sleep(1) # dropping soon after creating confuses Cassandra
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
        
        describe '#get_multi' do
          it 'loads multiple rows' do
            @cf.insert('ABC', 'xyz' => 'abc', 'foo' => 'bar')
            @cf.insert('DEF', 'xyz' => 'def', 'hello' => 'world')
            @cf.insert('GHI', 'xyz' => 'ghi', 'foo' => 'oof')
            @cf.get_multi(%w(ABC GHI)).should == {
              'ABC' => {'xyz' => 'abc', 'foo' => 'bar'},
              'GHI' => {'xyz' => 'ghi', 'foo' => 'oof'}
            }
          end

          it 'does not include rows that do not exist in the result' do
            @cf.insert('ABC', 'xyz' => 'abc', 'foo' => 'bar')
            @cf.insert('DEF', 'xyz' => 'def', 'hello' => 'world')
            @cf.insert('GHI', 'xyz' => 'ghi', 'foo' => 'oof')
            @cf.get_multi(%w(ABC GHI XYZ)).should == {
              'ABC' => {'xyz' => 'abc', 'foo' => 'bar'},
              'GHI' => {'xyz' => 'ghi', 'foo' => 'oof'}
            }
          end

          it 'returns an empty hash if no rows exist' do
            @cf.get_multi(%w(ABC GHI XYZ)).should == {}
          end
        end

        describe '#get_column_range' do
          it 'loads all columns in a range' do
            @cf.insert('ABC', 'a' => 'A', 'b' => 'B', 'c' => 'C', 'd' => 'D')
            @cf.get_column_range('ABC', 'b', 'c').should == {'b' => 'B', 'c' => 'C'}
          end
          
          it 'returns nil if no row was found' do
            @cf.get_column_range('ABC', 'b', 'c').should be_nil
          end

          it 'returns nil if no columns were found' do
            @cf.insert('ABC', 'a' => 'A', 'b' => 'B', 'c' => 'C', 'd' => 'D')
            @cf.get_column_range('ABC', 'x', 'z').should be_nil
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
