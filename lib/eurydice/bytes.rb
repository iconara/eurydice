# encoding: utf-8


module Eurydice
  module Bytes
    extend self
      
    def to_nio_bytes(str)
      java.nio.ByteBuffer.wrap(to_byte_array(str))
    end
      
    def to_byte_array(str)
      str.to_java_bytes
    end
      
    def nio_bytes_to_s(nb, type=:string)
      case type
      when :long then nb.get_long
      else
        byte_array_to_s(java.util.Arrays.copy_of_range(nb.array, nb.position, nb.limit))
      end
    end
      
    def byte_array_to_s(ba, type=:string)
      case type
      when :long
        java.nio.ByteBuffer.wrap(ba).get_long
      else
        String.from_java_bytes(ba)
      end
    end
  end
end