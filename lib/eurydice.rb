# encoding: utf-8

require 'java'

EURYDICE_EXT_HOME = File.expand_path('../ext', __FILE__)

$CLASSPATH << EURYDICE_EXT_HOME

Dir["#{EURYDICE_EXT_HOME}/*.jar"].each { |jar| require(jar) }


require 'eurydice/pelops'

module Eurydice
  class EurydiceError < StandardError; end
  class InvalidRequestError < EurydiceError; end
  class KeyspaceExistsError < InvalidRequestError; end
  class NotFoundError < EurydiceError; end
  class TimeoutError < EurydiceError; end
end
