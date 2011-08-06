# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'eurydice/version'


Gem::Specification.new do |s|
  s.name        = 'eurydice'
  s.version     = Eurydice::VERSION
  s.platform    = 'java'
  s.authors     = ['Theo Hultberg']
  s.email       = ['theo@burtcorp.com']
  s.homepage    = 'http://github.com/iconara/eurydice'
  s.summary     = %q{Ruby wrapper for the Pelops library}
  s.description = %q{}

  s.rubyforge_project = 'eurydice'
  
  s.add_dependency 'pelops-jars', '= 1.2'

  s.files         = `git ls-files`.split("\n")
# s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
# s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = %w(lib)
end
