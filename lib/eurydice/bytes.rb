# encoding: utf-8


module Eurydice
  module Bytes
    extend self
      
    def to_nio_bytes(str)
      to_pelops_bytes(str).bytes
    end
      
    def to_byte_array(str)
      str.to_java_bytes
    end
      
    def nio_bytes_to_s(nb)
      raise NotImplementedError, 'Only implemented in Eurydice::Pelops::Bytes'
    end
      
    def byte_array_to_s(ba, type=nil)
      case type
      when :long
        raise NotImplementedError, 'Only implemented in Eurydice::Pelops::Bytes'
      else
        String.from_java_bytes(ba)
      end
    end
  end
end