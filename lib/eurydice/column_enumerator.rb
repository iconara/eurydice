# encoding: utf-8

module Eurydice
  class ColumnEnumerator
    include Enumerable

    def initialize(column_family, row_key, options={})
      @column_family, @row_key, @options = column_family, row_key, DEFAULT_OPTIONS.merge(options)
      @max_retries = @options.delete(:max_retries)
      rewind
    end

    def each
      loop do
        yield self.next
      end
    end

    def next
      if @buffer.empty? && @exhausted
        raise StopIteration
      elsif @buffer.empty?
        result = @column_family.get(@row_key, @options.merge(from_column: @offset))
        keys = result.keys if result
        if result.nil? || result.empty? || keys.last == @offset
          @exhausted = true
          raise StopIteration
        end
        result.shift if keys.first == @offset
        result.each do |pair|
          @buffer << pair
        end
        @offset = keys.last
      end
      @buffer.shift
    end

    def rewind
      @offset = @options[:from_column] || (@options[:reversed] ? LAST_KEY : FIRST_KEY)
      @reversed = @options[:reversed]
      @buffer = []
      @exhausted = false
    end

    private

    FIRST_KEY = "\0".freeze
    LAST_KEY = ''.freeze
    DEFAULT_OPTIONS = {:max_column_count => 10_000, :max_retries => 3}.freeze
  end
end