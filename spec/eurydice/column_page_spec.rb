# encoding: utf-8

require_relative '../spec_helper'


module Eurydice
  describe ColumnPage do
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
      cf.insert('xyz', {'a' => '1', 'b' => '2', 'c' => '3', 'd' => '4', 'e' => '5', 'f' => '6', 'g' => '7', 'h' => '8'})
      cf
    end

    describe '#next_page' do
      it 'returns a new page with the correct offset' do
        page = described_class.new(column_family, 'xyz', 3)
        page.next_page.offset.should == 'd'
      end

      it 'returns a new page with the same page size' do
        page = described_class.new(column_family, 'xyz', 3)
        page.next_page.page_size.should == 3
      end

      it 'returns nil if there is no next page' do
        page = described_class.new(column_family, 'xyz', 6)
        page.next_page.next_page.should be_nil
      end

      it 'picks the right offsets when paging through the row' do
        page = described_class.new(column_family, 'xyz', 3)
        offsets = []
        loop do
          offsets << page.offset
          page = page.next_page
          break unless page
        end
        offsets.should == ['a','d','g']
      end
    end

    describe '#prev_page' do
      it 'returns a reverse page' do
        page = described_class.new(column_family, 'xyz', 3).next_page
        page.prev_page.should be_reverse
      end

      it 'returns a new page with the correct offset' do
        page = described_class.new(column_family, 'xyz', 3).next_page
        page.prev_page.offset.should == 'd'
      end

      it 'returns a new page with the same page size' do
        page = described_class.new(column_family, 'xyz', 3).next_page
        page.prev_page.page_size.should == 3
      end

      it 'returns nil if there is no previous page' do
        page = described_class.new(column_family, 'xyz', 3)
        page.prev_page.should be_nil
      end

      it 'picks the right offsets when paging through the row' do
        page = described_class.new(column_family, 'xyz', 3)
        page = page.next_page until page.last?
        offsets = []
        loop do
          offsets << page.offset
          page = page.prev_page
          break unless page
        end
        offsets.should == ['g','g','d']
      end
    end

    context 'navigating back and forth' do
      it 'next + prev + next == next' do
        page = described_class.new(column_family, 'xyz', 3)
        page.next_page.prev_page.next_page.offset.should == 'd'
      end
    end

    context 'with edge cases' do
      it 'handles the case when row size is divisible by page size' do
        [2,4,8].each do |page_size|
          expected_page_count = 8/page_size
          page = described_class.new(column_family, 'xyz', page_size)
          page_count = 1
          until page.last?
            page = page.next_page
            page_count += 1
            page_count.should_not > expected_page_count
          end
          page_count.should == expected_page_count

          page_count = 1
          until page.first?
            page = page.prev_page
            page_count += 1
            page_count.should_not > expected_page_count
          end
          page_count.should == expected_page_count
        end
      end

      it 'handles the case when previous page is short' do
        items = []
        described_class.new(column_family, 'xyz', 3, 'b').prev_page.each_column do |key,value|
          items << [key,value]
        end
        items.should == [%w[a 1]]
      end
    end

    describe '#each_column' do
      it 'yields every item in page' do
        items = []
        described_class.new(column_family, 'xyz', 3).each_column do |key,value|
          items << [key,value]
        end
        items.should == [%w[a 1], %w[b 2], %w[c 3]]
      end

      it 'returns an enumerable unless block is given' do
        enum = described_class.new(column_family, 'xyz', 3).each_column
        enum.map { |k,v| v }.should == %w[1 2 3]
      end
    end
  end
end
