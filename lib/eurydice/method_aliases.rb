# encoding: utf-8


module Eurydice
  module ColumnFamilyMethodAliases
    def self.included(m)
      m.send(:alias_method, :insert, :update)
      m.send(:alias_method, :inc, :increment)
      m.send(:alias_method, :incr, :increment)
      m.send(:alias_method, :increment_column, :increment)
      m.send(:alias_method, :row_exists?, :key?)
      m.send(:alias_method, :get_row, :get)
      m.send(:alias_method, :get_rows, :get)
    end
  end
end
