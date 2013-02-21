# encoding: utf-8

module Eurydice
  class ColumnEnumeratorBase
    include Enumerable

    def initialize(column_family, row_key, options={})
      @column_family, @row_key, @options = column_family, row_key, DEFAULT_OPTIONS.merge(options)
      @max_retries = @options.delete(:max_retries)
    end

    def each
      loop do
        yield self.next
      end
    end

    def rewind
    end

    private

    FIRST_KEY = "\0".freeze
    LAST_KEY = ''.freeze
    DEFAULT_OPTIONS = {:max_column_count => 10_000, :max_retries => 3}.freeze
  end

  class ColumnEnumerator < ColumnEnumeratorBase
    def initialize(*args)
      super
      rewind
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
  end

  class ConcurrentColumnEnumerator < ColumnEnumeratorBase
    java_import 'java.util.concurrent.Executors'
    java_import 'java.util.concurrent.ArrayBlockingQueue'
    java_import 'java.util.concurrent.atomic.AtomicBoolean'
    java_import 'org.jruby.threading.DaemonThreadFactory'

    def initialize(*args)
      super
      @standard_enumerator = ColumnEnumerator.new(*args)
      @queue = ArrayBlockingQueue.new(@options[:max_column_count] * 2)
      @fetch_pool = Executors.new_single_thread_executor(DaemonThreadFactory.new(self.class.name))
      @exhausted = true
      rewind
    end

    def next
      unless @running.get_and_set(true)
        @fetch_pool.execute do
          begin
            while @running.get
              pair = @standard_enumerator.next
              @queue.put(pair)
            end
          rescue StopIteration
          end
          @queue.put(:stop_iteration)
        end
      end
      
      raise StopIteration if @exhausted
        
      value = @queue.take
      if value == :stop_iteration
        @exhausted = true
        raise StopIteration
      end

      value
    end

    def rewind
      @running = AtomicBoolean.new(false)
      unless @exhausted
        until @queue.take == :stop_iteration; end
      end
      @standard_enumerator.rewind
      @exhausted = false
    end
  end
end