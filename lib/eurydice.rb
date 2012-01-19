# encoding: utf-8

require 'java'
require 'eurydice/pelops'
# require 'eurydice/hector'

module Eurydice
  class EurydiceError < StandardError; end
  class InvalidRequestError < EurydiceError; end
  class KeyspaceExistsError < InvalidRequestError; end
  class NotFoundError < EurydiceError; end
  class TimeoutError < EurydiceError; end
  
  def self.connect(*args)
    Pelops.connect(*args)
  end
  
  def self.disconnect!
    Pelops.disconnect!
  end
end
