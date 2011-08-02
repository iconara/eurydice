require_relative '../spec_helper'

require 'eurydice/pelops'


describe Eurydice do
  context 'keyspaces' do
    before do
      @keyspace = Eurydice.connect('eurydice_test_space')
      @keyspace.drop! rescue nil
    end
    
    it 'can create a keyspace' do
      @keyspace.create!
    end

    it 'can drop a keyspace' do
      @keyspace.create!
      @keyspace.drop!
    end
  end

  context 'column families' do
    before do
      @keyspace = Eurydice.connect('eurydice_test_space')
      @keyspace.drop! rescue nil
      @keyspace.create!
    end

    after do
      @keyspace.drop!
    end
    
    it 'can create a column family' do
      cf = @keyspace.column_family('test_family')
      cf.create!
    end
  
    it 'can drop a column family' do
      cf = @keyspace.column_family('test_family')
      cf.create!
      cf.drop!
    end
  end
end