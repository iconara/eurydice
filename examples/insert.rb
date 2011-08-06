# encoding: utf-8

$: << File.expand_path('../../lib', __FILE__)

require 'eurydice'


begin
  cluster = Eurydice.connect
  keyspace = cluster.keyspace('blurgh', :create => false)
  keyspace.drop! rescue nil
  keyspace.create!
  cf = keyspace.column_family('foo', :create => false)
  cf.create!(:key_validation_class => :ascii, :comparator_type => :ascii, :default_validation_class => :utf8)
  cf.update('HELLOWORLD', {'foo' => 'bar', 'hello' => 'wörld'})
  p cf.get('HELLOWORLD', :columns => %w(hello))
rescue Eurydice::EurydiceError => e
  $stderr.puts("#{e.message} (#{e.class})")
  $stderr.puts("\t" + e.backtrace.join("\n\t"))
ensure
  keyspace.drop!
end