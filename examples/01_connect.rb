# encoding: utf-8

require_relative 'common'


# Connect to the default host (localhost) and port (9160), these can be
# overridden by passing then :host and :port options.
cluster = Eurydice.connect

# Get a reference to a keyspace, it will be created if it does not exist
# (pass the option :create => false to not automatically create the keyspace).
keyspace = cluster.keyspace('my_keyspace')

# Clean up by dropping the keyspace
keyspace.drop!

# Finally disconnect everything
Eurydice.disconnect!
