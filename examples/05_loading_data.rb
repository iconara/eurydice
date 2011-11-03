# encoding: utf-8

require_relative 'common'


cluster = Eurydice.connect
keyspace = cluster.keyspace('my_keyspace')
column_family = keyspace.column_family('employees', :create => false)
column_family.drop! if column_family.exists?
column_family.create!(:column_metadata => {'name' => {:validation_class => :utf8, :index_name => 'name_index', :index_type => :keys}})
column_family.insert('employee:1', {'name' => 'Sam', 'role' => 'Developer'})
column_family.insert('employee:2', {'name' => 'Phil', 'role' => 'Accountant'})
column_family.insert('employee:3', {'name' => 'Steve', 'role' => 'Developer'})
column_family.insert('employee:4', {'name' => 'Julie', 'role' => 'CEO'})
column_family.update('employee:3', {'email' => 'steve@acme.com'})
column_family.update('employee:3', {'role' => 'tester'})
column_family.update('employee:2', {'age' => 44}, :validations => {'age' => :long})
column_family.update('employee:4', {'email' => 'boss@acme.com'}, :consistency_level => :quorum)
column_family.update('employee:1', {'email' => 'sam@acme.com'}, :cl => :one)

# Load a single row
employee1 = column_family.get('employee:1')
puts "employee:1 => #{employee1['name']}, #{employee1['role']}"

# Load multiple rows
employees = column_family.get(%w(employee:2 employee:3 employee:4))
employees.each do |row_key, employee|
  puts "#{row_key} => #{employee['name']}, #{employee['role']}"
end

puts '---'

# Load only the specified columns
employee1 = column_family.get('employee:1', :columns => %w(name))
puts "employee:1 => #{employee1['name']}"

employees = column_family.get(%w(employee:2 employee:3 employee:4), :columns => %w(name))
employees.each do |row_key, employee|
  puts "#{row_key} => #{employee['name']}"
end

puts '---'

column_family.insert('letters', ('a'..'z').to_a.zip(('A'..'Z').to_a))

# Load a page of columns
result = column_family.get('letters', :max_column_count => 10)
puts result.keys.join(', ')
# For the next page you need to tell Eurydice which the last key you saw was
result = column_family.get('letters', :from_column => result.keys.last, :max_column_count => 11)
# The last key is included in the next page (so we load one extra column, and shift off the first column)
puts result.keys[1..-1].join(', ')

puts '---'

# Load only the value from a single column
employee1_name = column_family.get_column('employee:1', 'name')
puts "employee:1 => #{employee1_name}"

puts '---'

# If you've stored a number you have to specify :validations when loading, too
employee2 = column_family.get('employee:2', :validations => {'age' => :long})
puts "employee:2 => #{employee2['name']}, #{employee2['age']}"

puts '---'

# You can check if a row exists
puts "Is there a employee:5? #{column_family.key?('employee:0') ? 'yes' : 'no'}"

# #row_exists? is an alias for #key?
puts "Is there a employee:1? #{column_family.row_exists?('employee:1') ? 'yes' : 'no'}"

puts '---'

# You can specify :consistency_level as :one, :quorum, :all or :any
employee1 = column_family.get('employee:1', :consistency_level => :quorum)
puts "employee:1 => #{employee1['email']}"

puts '---'

# If you have a row with lots of columns, you can iterate over them (in order)
# with #each_column. Under the hood they will be loaded in batches
column_family.update('employee:5', Hash[(0...1000).map { |i| ["property#{i}", "value#{i}"] }])
count = 0
column_family.each_column('employee:5') do |column_name, column_value|
  count += 1
end
puts "There were #{count} columns"

puts '---'

# Cassandra has basic secondary indexes, which can be used when querying
column_family.insert('employees:6', {'name' => 'Sam'})
column_family.insert('employees:7', {'name' => 'Sam'})
employees_named_sam = column_family.get_indexed('name', :==, 'Sam')
puts "There are #{employees_named_sam.size} employees named 'Sam'"

keyspace.drop!
