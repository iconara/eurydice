# encoding: utf-8

module Eurydice
  class ColumnPage
    include Enumerable

    attr_reader :page_size

    def initialize(column_family, row_key, page_size, offset=FIRST_OFFSET)
      @column_family = column_family
      @row_key = row_key
      @offset = offset
      @page_size = page_size
    end

    def offset
      if @offset == FIRST_OFFSET
        slice.keys.first
      else
        @offset
      end
    end

    def next_page
      return nil if last?
      ForwardColumnPage.new(@column_family, @row_key, @page_size, next_page_offset)
    end

    def prev_page
      return nil if first?
      ReverseColumnPage.new(@column_family, @row_key, @page_size, prev_page_offset)
    end

    def first?
      @offset == FIRST_OFFSET
    end

    def last?
      slice.size <= @page_size
    end

    def reverse?
      false
    end

    def each_column(&block)
      return self unless block_given?
      slice.each.with_index do |item,i|
        yield item if i < @page_size
      end
    end
    alias_method :each, :each_column

    private

    def next_page_offset
      slice.keys[-2]
    end

    def prev_page_offset
      @offset
    end

    FIRST_OFFSET = "\0".freeze

    def slice
      @slice ||= @column_family.get(@row_key, from_column: @offset, max_column_count: @page_size + 2, reversed: reverse?)
    end
  end

  class ForwardColumnPage < ColumnPage
    def first?
      false
    end
  end

  class ReverseColumnPage < ColumnPage
    def last?
      false
    end

    def first?
      slice.size <= @page_size + 1
    end

    def reverse?
      true
    end

    def each_column(&block)
      return self unless block_given?
      first = slice.size == @page_size + 2 ? 1 : 0
      last = slice.size - 2
      slice.reverse_each.with_index do |item,i|
        yield item if first <= i && i <= last
      end
    end

    private

    def next_page_offset
      @offset
    end

    def prev_page_offset
      slice.keys[-2]
    end
  end
end
