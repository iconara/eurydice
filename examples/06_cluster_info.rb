# encoding: utf-8

require_relative 'common'


cluster = Eurydice.connect

# List the keyspaces
puts 'Keyspaces:'
cluster.keyspaces.each_with_index do |keyspace_name, i|
  puts "#{i + 1}: #{keyspace_name}"
end

puts '---'

# List the nodes in the cluster
puts 'Nodes:'
cluster.nodes.each_with_index do |node_address, i|
  puts "#{i + 1}: #{node_address}"
end