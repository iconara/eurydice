require_relative '../../spec_helper'


module Eurydice
  module Pelops
    describe ColumnFamily do
      before :all do
        @cluster = Eurydice.connect
        @keyspace_name = "eurydice_test_space_#{rand(1000)}"
        @keyspace = @cluster.keyspace(@keyspace_name, :create => false)
        @keyspace.drop! rescue nil
        @keyspace.create!
      end
      
      after :all do
        @keyspace.drop! rescue nil
      end
  
      describe '#create!' do
        before do
          @cf_name = "test_family_#{rand(1000)}"
        end
        
        after do
          @cf.drop! rescue nil
        end
        
        it 'can create a column family' do
          @cf = @keyspace.column_family(@cf_name)
          @cf.exists?.should be_true
        end
    
        it 'defers the creation of a keyspace with :create => false' do
          @cf = @keyspace.column_family(@cf_name, :create => false)
          @cf.exists?.should be_false
          @cf.create!
          @cf.exists?.should be_true
        end

        marshal_types = {
          'a fully qualified name' => 'org.apache.cassandra.db.marshal.UTF8Type',
          'the package name omitted' => 'UTF8Type',
          'an alias' => :utf8
        }

        context 'creating a column family with a specific comparator type' do
          marshal_types.each do |desc, type|
            it "with #{desc}" do
              @cf = @keyspace.column_family(@cf_name, :create => false)
              @cf.create!(:comparator_type => type)
              @cf.definition(true)[:comparator_type].should == 'org.apache.cassandra.db.marshal.UTF8Type'
            end
          end
        end

        context 'creating a column family with a specific subcomparator type' do
          marshal_types.each do |desc, type|
            it "with #{desc}" do
              @cf = @keyspace.column_family(@cf_name, :create => false)
              @cf.create!(:column_type => :super, :subcomparator_type => type)
              @cf.definition(true)[:subcomparator_type].should == 'org.apache.cassandra.db.marshal.UTF8Type'
            end
          end
        end

        context 'creating a column family with a specific default validation class' do
          marshal_types.each do |desc, type|
            it "with #{desc}" do
              @cf = @keyspace.column_family(@cf_name, :create => false)
              @cf.create!(:default_validation_class => type)
              @cf.definition(true)[:default_validation_class].should == 'org.apache.cassandra.db.marshal.UTF8Type'
            end
          end
        end
    
        context 'creating a column family with a specific validation class for a column' do
          marshal_types.each do |desc, type|
            it "with #{desc}" do
              @cf = @keyspace.column_family(@cf_name, :create => false)
              @cf.create!(:column_metadata => {'xyz' => {:validation_class => type}})
              @cf.definition(true)[:column_metadata]['xyz'][:validation_class].should == 'org.apache.cassandra.db.marshal.UTF8Type'
            end
          end
        end
    
        it 'creates a column family with an index' do
          @cf = @keyspace.column_family(@cf_name, :create => false)
          @cf.create!(:column_metadata => {'xyz' => {:index_name => 'abc', :index_type => :keys, :validation_class => :ascii}})
          @cf.definition(true)[:column_metadata]['xyz'][:index_name].should == 'abc'
        end
    
        it 'creates a column family with a specific column type' do
          @cf = @keyspace.column_family(@cf_name, :create => false)
          @cf.create!(:column_type => :super)
          @cf.definition(true)[:column_type].should == :super
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
          @cf = @keyspace.column_family('test_family', :create => false)
          @cf.drop! rescue nil
          @cf.create!
        end
        
        after do
          @cf.drop!
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
          @cf = @keyspace.column_family('test_family', :create => false)
          @cf.drop! rescue nil
          @cf.create!
        end
        
        after do
          @cf.drop!
        end
    
        it 'returns column family metadata' do
          definition = @cf.definition
          definition[:name].should == 'test_family'
          definition[:default_validation_class].should == 'org.apache.cassandra.db.marshal.BytesType'
        end
      end
  
      describe '#key?' do
        before do
          @cf = @keyspace.column_family('test_family', :create => false)
          @cf.drop! rescue nil
          @cf.create!
        end
        
        after do
          @cf.drop!
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
            # TODO: not sure how to test, this just tests that no error is raised
            @cf.insert('ABC', {'xyz' => 'abc'}, {:consistency_level => :quorum})
            @cf.get('ABC').should == {'xyz' => 'abc'}
          end

          it 'writes with a custom consistency level (:cl is an alias for :consistency_level)' do
            # TODO: not sure how to test, this just tests that no error is raised
            @cf.insert('ABC', {'xyz' => 'abc'}, {:cl => :one})
            @cf.get('ABC').should == {'xyz' => 'abc'}
          end
          
          it 'writes a column with a TTL' do
            # TODO: not sure how to test without actually waiting for the TTL to expire
            @cf.insert('ABC', {'xyz' => 'abc'}, {:ttl => 1})
            sleep(1.5)
            @cf.get('ABC').should be_nil
          end
          
          context 'with explicit column data types' do
            it 'writes integer columns keys as longs' do
              @cf.insert('ABC', {42 => 'foo'}, :comparator => :long)
              @cf.get('ABC', :comparator => :long).should == {42 => 'foo'}
            end
            
            it 'writes integer values as longs' do
              @cf.insert('ABC', {'xyz' => 3}, :validations => {'xyz' => :long})
              @cf.get('ABC', :validations => {'xyz' => :long}).should == {'xyz' => 3}
            end
          end
        end
  
        describe '#get' do
          context 'with a single row key' do
            it 'loads a row' do
              @cf.insert('ABC', 'xyz' => 'abc')
              @cf.get('ABC').should == {'xyz' => 'abc'}
            end
    
            it 'loads all columns for a row by default' do
              @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
              @cf.get('ABC').should == {'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar'}
            end
            
            it 'loads the specified columns' do
              @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
              @cf.get('ABC', :columns => %w(hello foo)).should == {'hello' => 'world', 'foo' => 'bar'}
            end
            
            it 'loads the specified range of columns' do
              @cf.insert('ABC', 'a' => 'A', 'd' => 'D', 'f' => 'F', 'g' => 'G', 'b' => 'B', 'x' => 'X')
              @cf.get('ABC', :columns => 'b'...'f').should == {'b' => 'B', 'd' => 'D', 'f' => 'F'}
            end
            
            it 'loads a max number of columns' do
              @cf.insert('ABC', Hash[('a'..'z').map { |a| [a, a.upcase] }.shuffle])
              @cf.get('ABC', :max_column_count => 10).should == Hash[('a'..'z').take(10).map { |a| [a, a.upcase] }]
            end
            
            it 'loads a page of columns' do
              @cf.insert('ABC', Hash[('a'..'z').map { |a| [a, a.upcase] }.shuffle])
              @cf.get('ABC', :from_column => 'm', :max_column_count => 10).should == Hash[('m'..'z').take(10).map { |a| [a, a.upcase] }]
            end
            
            it 'raises an error if both :columns and :from_column are given' do
              expect { @cf.get('ABC', :columns => 'a'..'z', :from_column => 'm') }.to raise_error(ArgumentError)
            end
            
            it 'loads columns in reverse order with :reversed => true' do
              @cf.insert('ABC', Hash[('a'..'f').map { |a| [a, a.upcase] }.shuffle])
              @cf.get('ABC', :reversed => true).keys.should == ('a'..'f').to_a.reverse
            end
    
            it 'returns nil if no row was found' do
              @cf.get('XYZ').should be_nil
            end
          end
          
          context 'with multiple row keys' do
            it 'loads multiple rows' do
              @cf.insert('ABC', 'xyz' => 'abc', 'foo' => 'bar')
              @cf.insert('DEF', 'xyz' => 'def', 'hello' => 'world')
              @cf.insert('GHI', 'xyz' => 'ghi', 'foo' => 'oof')
              @cf.get(%w(ABC GHI)).should == {
                'ABC' => {'xyz' => 'abc', 'foo' => 'bar'},
                'GHI' => {'xyz' => 'ghi', 'foo' => 'oof'}
              }
            end

            it 'does not include rows that do not exist in the result' do
              @cf.insert('ABC', 'xyz' => 'abc', 'foo' => 'bar')
              @cf.insert('DEF', 'xyz' => 'def', 'hello' => 'world')
              @cf.insert('GHI', 'xyz' => 'ghi', 'foo' => 'oof')
              @cf.get(%w(ABC GHI XYZ)).should == {
                'ABC' => {'xyz' => 'abc', 'foo' => 'bar'},
                'GHI' => {'xyz' => 'ghi', 'foo' => 'oof'}
              }
            end
            
            it 'loads columns for multiple rows' do
              @cf.insert('ABC', 'xyz' => 'abc', 'foo' => 'bar')
              @cf.insert('DEF', 'xyz' => 'def', 'hello' => 'world')
              @cf.insert('GHI', 'xyz' => 'ghi', 'foo' => 'oof')
              @cf.get(%w(ABC GHI), :columns => %w(xyz foo)).should == {'ABC' => {'xyz' => 'abc', 'foo' => 'bar'}, 'GHI' => {'xyz' => 'ghi', 'foo' => 'oof'}}
            end

            it 'does not include rows that do not have the specified column' do
              @cf.insert('ABC', 'foo' => 'bar')
              @cf.insert('DEF', 'xyz' => 'def', 'hello' => 'world')
              @cf.insert('GHI', 'xyz' => 'ghi', 'foo' => 'oof', 'abc' => '123')
              @cf.get(%w(ABC GHI), :columns => %w(xyz abc)).should == {'GHI' => {'xyz' => 'ghi', 'abc' => '123'}}
            end

            it 'does not include rows that do not exist in the results' do
              @cf.insert('DEF', 'xyz' => 'def', 'hello' => 'world')
              @cf.insert('GHI', 'xyz' => 'ghi', 'foo' => 'oof')
              @cf.get(%w(ABC GHI), :columns => %w(xyz foo)).should == {'GHI' => {'xyz' => 'ghi', 'foo' => 'oof'}}
            end
            
            it 'loads all columns in a range from multiple rows' do
              @cf.insert('ABC', 'a' => 'A', 'b' => 'B', 'c' => 'C', 'd' => 'D')
              @cf.insert('DEF', 'a' => 'A', 'b' => 'B', 'c' => 'C', 'd' => 'D', 'f' => 'F')
              @cf.insert('GHI', 'a' => 'A', 'b' => 'B', 'd' => 'D')
              @cf.get(%w(ABC GHI), :columns => 'b'...'d').should == {
                'ABC' => {'b' => 'B', 'c' => 'C', 'd' => 'D'},
                'GHI' => {'b' => 'B', 'd' => 'D'}
              }
            end
            
            it 'returns an empty hash if no rows exist' do
              @cf.get(%w(ABC GHI)).should == {}
            end
          end
          
          context 'with options' do
            it 'loads with a custom consistency level' do
              # TODO: not sure how to test, this just tests that no error is raised
              @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
              @cf.get('ABC', :consistency_level => :quorum).should == {'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar'}
            end

            it 'loads with a custom consistency level (:cl is an alias for :consistency_level)' do
              @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
              @cf.get('ABC', :cl => :one).should == {'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar'}
            end
          end
        end
  
        describe '#get_column' do
          it 'loads a single column for a row' do
            @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
            @cf.get_column('ABC', 'hello').should == 'world'
          end
    
          it 'loads with a custom consistency level' do
            # TODO: not sure how to test, this just tests that no error is raised
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

        describe '#get_column_count' do
          it 'returns the number of columns in the specified row' do
            @cf.insert('ABC', Hash[('a'..'z').zip(0..9 * 10)])
            @cf.get_column_count('ABC').should == 26
          end
          
          it 'returns zero if the row does not exist' do
            @cf.get_column_count('X').should == 0
          end
          
          it 'returns the number of columns in the specified range' do
            @cf.insert('ABC', Hash[('a'..'z').zip(0..9 * 10)])
            @cf.get_column_count('ABC', :columns => 'm'..'q').should == 5
          end

          it 'returns the number of columns after the specified column' do
            @cf.insert('ABC', Hash[('a'..'z').zip(0..9 * 10)])
            @cf.get_column_count('ABC', :from_column => 's').should == 8
          end
        end

        describe '#each_column' do
          before do
            @cf.insert('ABC', Hash[('a'..'z').map { |a| [a, a.upcase] }.shuffle])
          end
          
          it 'yields each column in a row' do
            row = {}
            @cf.each_column('ABC') do |k, v|
              row[k] = v
            end
            row.should == Hash[('a'..'z').map { |a| [a, a.upcase] }]
          end

          it 'returns an Enumerator that yields each column in a row' do
            row = {}
            enum = @cf.each_column('ABC')
            enum.each do |pair|
              k, v = *pair # JRuby 1.6.4 Enumerator#each does not splat the arguments
              row[k] = v
            end
            row.should == Hash[('a'..'z').map { |a| [a, a.upcase] }]
          end
          
          it 'yields each column in reverse order with :reversed => true' do
            column_keys = []
            @cf.each_column('ABC', :reversed => true) do |k, v|
              column_keys << k
            end
            column_keys.should == ('a'..'z').to_a.reverse
          end
          
          it 'can start after a specified key' do
            column_keys = []
            @cf.each_column('ABC', :start_beyond => 'w') do |k, v|
              column_keys << k
            end
            column_keys.should == ('x'..'z').to_a
          end
          
          it 'can use a custom batch size' do
            # TODO: not sure how to test, this just tests that no error is raised
            row = {}
            @cf.each_column('ABC', :batch_size => 2) do |k, v|
              row[k] = v
            end
            row.should == Hash[('a'..'z').map { |a| [a, a.upcase] }]
          end
          
          it 'loads with a custom consistency level' do
            # TODO: not sure how to test, this just tests that no error is raised
            @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
            @cf.each_column('ABC', :consistency_level => :quorum) do |k, v|
            end
          end

          it 'loads with a custom consistency level (:cl is an alias for :consistency_level)' do
            @cf.insert('ABC', 'xyz' => 'abc', 'hello' => 'world', 'foo' => 'bar')
            @cf.each_column('ABC', :cl => :one) do |k, v|
            end
          end
        end

        describe '#get_indexed' do
          before do
            @cf = @keyspace.column_family('indexed_test_family', :create => false)
            @cf.drop! rescue nil
            @cf.create!(:column_metadata => {
              'name' => {
                :validation_class => :ascii,
                :index_name => 'name_index',
                :index_type => :keys
              },
              'age' => {
                :validation_class => :long,
                :index_name => 'age_index',
                :index_type => :keys
              }
            })
          end
          
          it 'loads rows by index' do
            @cf.insert('user1', {'name' => 'sue'})
            @cf.insert('user2', {'name' => 'phil'})
            @cf.get_indexed('name', :==, 'sue').should == {'user1' => {'name' => 'sue'}}
          end
          
          it 'loads rows by index (using :eq instead of :==)' do
            @cf.insert('user1', {'name' => 'sue'})
            @cf.insert('user2', {'name' => 'phil'})
            @cf.get_indexed('name', :eq, 'sue').should == {'user1' => {'name' => 'sue'}}
          end
          
          it 'limits the number of returned rows' do
            names = %w(sue phil sam jim)
            100.times do |i|
              row = {'name' => names[i % names.size], 'age' => i % names.size}
              @cf.insert("user:#{i}", row, :validations => {'age' => :long})
            end
            @cf.get_indexed('age', :==, 3, :max_row_count => 3, :validations => {'age' => :long}).should have(3).items
          end
          
          it 'raises an error if the index operator is not supported' do
            expect { @cf.get_indexed('name', :%, 'me') }.to raise_error(ArgumentError)
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
