# encoding: utf-8

require_relative 'common'


cluster = Eurydice.connect
keyspace = cluster.keyspace('my_keyspace')

# Get a reference to a column family, but don't automatically create it, 
# instead we will create it explicitly, and with a few options.
column_family = keyspace.column_family('my_family', :create => false)

# Create the column family with some options, the possible options can be found
# here: http://www.datastax.com/docs/0.8/configuration/storage_configuration
column_family.create!(
  :key_validation_class => :ascii,    # the type of the row keys
  :comparator_type => :ascii,         # the type of the column keys
  :default_validation_class => :utf8, # the type of the column values
  :column_metadata => {
    'name' => {
      :validation_class => :utf8,     # you can declare the types of columns
      :index_name => 'name_index',    # and set up indexing
      :index_type => :keys
    }
  }
)

keyspace.drop!
