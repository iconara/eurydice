# encoding: utf-8

require_relative 'common'


cluster = Eurydice.connect
keyspace = cluster.keyspace('my_keyspace')
column_family = keyspace.column_family('employees')

# Insert a few rows representing employees
column_family.insert('employee:1', {'name' => 'Sam', 'role' => 'Developer'})
column_family.insert('employee:2', {'name' => 'Phil', 'role' => 'Accountant'})
column_family.insert('employee:3', {'name' => 'Steve', 'role' => 'Developer'})
column_family.insert('employee:4', {'name' => 'Julie', 'role' => 'CEO'})

# #insert is actually an alias for #update, in some cases it feels more 
# natural to say "insert" than "update", but in the end the operations are the
# same -- adding a column to a row (and adding a column that is already there
# replaces the old value).
column_family.update('employee:3', {'email' => 'steve@acme.com'})
column_family.update('employee:3', {'role' => 'tester'})

# If you want to insert numbers you must be explicit, unfortunately. Use the
# :validations option to pass a hash of property types. Currently the only one
# besides the default is :long (the default is to make the value a string and
# then creating a byte array from the string, this works with the :bytes, 
# :ascii and :utf8 validations [read "column value types"] if the string has
# the right encoding).
column_family.update('employee:2', {'age' => 44}, :validations => {'age' => :long})

# You can specify :consistency_level as :one, :quorum, :all or :any (default is :one)
column_family.update('employee:4', {'email' => 'boss@acme.com'}, :consistency_level => :quorum)

# :cl is an alias for :consistency_level
column_family.update('employee:1', {'email' => 'sam@acme.com'}, :cl => :one)

keyspace.drop!
