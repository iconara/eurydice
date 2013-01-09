# encoding: utf-8

require 'eurydice/column_enumerator'
require 'eurydice/column_page'
require 'eurydice/pelops'

module Eurydice
  class EurydiceError < StandardError; end
  class ConnectionError < EurydiceError; end
  class InvalidRequestError < EurydiceError; end
  class KeyspaceExistsError < InvalidRequestError; end
  class NotFoundError < EurydiceError; end
  class TimeoutError < EurydiceError; end
  class BatchError < EurydiceError; end
end
