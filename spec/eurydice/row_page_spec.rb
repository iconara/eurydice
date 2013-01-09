# encoding: utf-8

require_relative '../spec_helper'


module Eurydice
  describe PagingHelper do
    include PagingHelper

    before :all do
      @keyspace_name = "eurydice_test_space_#{rand(1000)}"
      @cf_name = "column_family_#{rand(1000)}"
      @cluster = Eurydice.connect(host: ENV['CASSANDRA_HOST'])
      @keyspace = @cluster.keyspace(@keyspace_name, :create => false)
      @keyspace.drop! rescue nil
      @keyspace.create!
    end

    let :column_family do
      cf = @keyspace.column_family(@cf_name)
      cf.insert('xyz', {'a' => "1", 'b' => "2", 'c' => "3", 'd' => "4", 'e' => "5", 'f' => "6", 'g' => "7", 'h' => "8"})
      cf.insert('SHORTROW', {"A" => "1", "B" => "2"})
      cf
    end

    describe '#next_page' do
      it 'loads the first page' do
        page, next_id, prev_id = next_page(column_family, 'xyz', 3, nil)
        page.should == {'a' => "1", 'b' => "2", 'c' => "3"}
        next_id.should == 'c'
        prev_id.should be_nil
      end
      
      it 'loads the second page' do
        page, next_id, prev_id = next_page(column_family, 'xyz', 3, 'c')
        page.should == {'d' => "4", 'e' => "5", 'f' => "6"}
        next_id.should == 'f'
        prev_id.should == 'c'
      end
      
      it 'loads the last page' do
        page, next_id, prev_id = next_page(column_family, 'xyz', 3, 'f')
        page.should == {'g' => "7", 'h' => "8"}
        next_id.should be_nil
        prev_id.should == 'f'
      end
      
      it 'loads the last page, when the pages add up evenly' do
        page, next_id, prev_id = next_page(column_family, 'xyz', 2, 'f')
        page.should == {'g' => "7", 'h' => "8"}
        next_id.should be_nil
        prev_id.should == 'f'
      end
      
      it 'returns an empty page when there is no data' do
        page, next_id, prev_id = next_page(column_family, 'abc', 2, nil)
        page.should be_empty
        next_id.should be_nil
        prev_id.should be_nil
      end
    end
    
    describe '#previous_page' do
      it 'loads the previous page' do
        page, next_id, prev_id = previous_page(column_family, 'xyz', 3, 'f')
        page.should == {'d' => "4", 'e' => "5", 'f' => "6"}
        next_id.should == 'f'
        prev_id.should == 'c'
      end

      it 'loads the first page' do
        page, next_id, prev_id = previous_page(column_family, 'xyz', 3, 'c')
        page.should == {'a' => "1", 'b' => "2", 'c' => "3"}
        next_id.should == 'c'
        prev_id.should be_nil
      end
      
      it 'returns an empty page when there is no data' do
        page, next_id, prev_id = previous_page(column_family, 'abc', 2, nil)
        page.should be_empty
        next_id.should be_nil
        prev_id.should be_nil
      end

      it 'loads the last page when key is empty' do
        page, next_id, prev_id = previous_page(column_family, 'xyz', 3, nil)
        page.should == {'f' => "6", 'g' => "7", 'h' => "8"}
        next_id.should == nil
        prev_id.should == 'e'
      end

      it 'handles when a page is smaller than the requested page size' do
        page, next_id, prev_id = previous_page(column_family, 'SHORTROW', 100, nil)
        page.should == {"B" => "2", "A" => "1"}
      end
    end
    
    describe '#next_page/#previous_page' do
      it 'reliably navigates forwards and backwards' do
        first_page = {'a' => "1", 'b' => "2", 'c' => "3"}
        second_page = {'d' => "4", 'e' => "5", 'f' => "6"}
        third_page = {'g' => "7", 'h' => "8"}
        pages = []
        page, next_id, prev_id = next_page(column_family, 'xyz', 3, nil)
        pages << page
        page, next_id, prev_id = next_page(column_family, 'xyz', 3, next_id)
        pages << page
        page, next_id, prev_id = previous_page(column_family, 'xyz', 3, prev_id)
        pages << page
        page, next_id, prev_id = next_page(column_family, 'xyz', 3, next_id)
        pages << page
        page, next_id, prev_id = next_page(column_family, 'xyz', 3, next_id)
        pages << page
        page, next_id, prev_id = previous_page(column_family, 'xyz', 3, prev_id)
        pages << page
        pages.should == [first_page, second_page, first_page, second_page, third_page, second_page]
      end
    end
  end
end