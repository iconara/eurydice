# encoding: utf-8

require 'java'
require 'eurydice/pelops'

module Eurydice
  class EurydiceError < StandardError; end
  class InvalidRequestError < EurydiceError; end
  class KeyspaceExistsError < InvalidRequestError; end
  class NotFoundError < EurydiceError; end
  class TimeoutError < EurydiceError; end
  class BatchError < EurydiceError; end
end
