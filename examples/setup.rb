$: << File.expand_path('../../lib', __FILE__)

require 'eurydice'


begin
  cluster = Eurydice.connect
  keyspace = cluster.keyspace('blurgh')
  cf = keyspace.column_family('foo')
ensure
  keyspace.drop!
end