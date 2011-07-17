$: << File.expand_path('../../lib', __FILE__)

require 'eurydice'


begin
  keyspace = Eurydice.connect('blurgh')
  keyspace.create!

  cf = keyspace.column_family('foo')
  cf.create!
ensure
  keyspace.drop!
end