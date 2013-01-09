# encoding: utf-8

module Eurydice
  module PagingHelper
    def next_page(collection, key, page_size, reference_id, preresolved_keys=false)
      options = {:max_column_count => page_size + 2}
      options[:from_column] = reference_id if reference_id
      result = collection.get(key, options)
      return [], nil, nil unless result
      full_page = result.size == page_size + 2
      keys = preresolved_keys ? result : result.keys
      result.delete(keys.pop) if full_page
      result.delete(keys.pop) if keys.first != reference_id
      result.delete(reference_id)
      next_id = keys.last if full_page
      prev_id = reference_id
      return result, next_id, prev_id
    end

    def previous_page(collection, key, page_size, reference_id, preresolved_keys=false)
      options = {:max_column_count => page_size + 1, :reversed => true}
      options[:from_column] = reference_id if reference_id
      result = collection.get(key, options)
      return [], nil, nil unless result
      keys = preresolved_keys ? result.reverse : result.keys.reverse
      prev_id = keys.shift unless result.size <= page_size
      next_id = reference_id
      result = keys.reduce({}) { |acc, k| acc[k] = result[k]; acc }
      return result, next_id, prev_id
    end
  end
end
