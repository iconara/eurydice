# encoding: utf-8

require_relative 'common'


cluster = Eurydice.connect

# Get a reference to a keyspace, but don't automatically create it, instead
# we will create it explicitly, and with a few options.
keyspace = cluster.keyspace('my_keyspace', :create => false)

# Create the keyspace with some options, the possible options can be found
# here: http://www.datastax.com/docs/0.8/configuration/storage_configuration
keyspace.create!(
  :strategy_class => 'org.apache.cassandra.locator.NetworkTopologyStrategy',
  :strategy_options => {:replication_factor => 3}
)

keyspace.drop!
