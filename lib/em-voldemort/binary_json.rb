module EM::Voldemort
  # Codec for Voldemort's custom binary serialization format. The Voldemort codebase itself refers
  # to this format as "json", even though it has virtually nothing in common with JSON. It's
  # actually more like Avro, but with less sophisticated schema evolution, and less compact. We're
  # only using it because the Hadoop job for building read-only stores requires it. The format is
  # roughly documented at https://github.com/voldemort/voldemort/wiki/Binary-JSON-Serialization
  #
  # This code is adapted from Alejandro Crosa's voldemort-rb gem (MIT License).
  # https://github.com/acrosa/voldemort-rb
  class BinaryJson

    attr_reader :has_version_tag
    attr_reader :schema_versions

    BYTE_MIN_VAL   = -2**7
    BYTE_MAX_VAL   =  2**7 - 1
    SHORT_MIN_VAL  = -2**15
    SHORT_MAX_VAL  =  2**15 - 1
    INT_MIN_VAL    = -2**31
    INT_MAX_VAL    =  2**31 - 1
    LONG_MIN_VAL   = -2**63
    LONG_MAX_VAL   =  2**63 - 1
    FLOAT_MIN_VAL  = 2.0**-149
    DOUBLE_MIN_VAL = 2.0**-1074
    STRING_MAX_LEN = 0x3FFFFFFF

    def initialize(schema_by_version, has_version_tag=true)
      @has_version_tag = has_version_tag
      @schema_versions = schema_by_version.each_with_object({}) do |(version, schema), hash|
        hash[version.to_i] = parse_schema(schema)
      end
    end

    # Serializes a Ruby object to binary JSON
    def encode(object)
      ''.force_encoding(Encoding::BINARY).tap do |bytes|
        newest_version = schema_versions.keys.max
        schema = schema_versions[newest_version]
        bytes << newest_version.chr if has_version_tag
        write(object, bytes, schema)
      end
    end

    # Parses a binary JSON string into Ruby objects
    def decode(bytes)
      bytes.force_encoding(Encoding::BINARY)
      input = StringIO.new(bytes)
      version = has_version_tag ? input.read(1).ord : 0
      schema = schema_versions[version]
      raise ClientError, "no registered schema for version #{version}" unless schema
      read(input, schema)
    end

    private

    def parse_schema(schema)
      # tolerate use of single quotes in place of double quotes in the schema
      schema = schema.gsub("'", '"')

      if schema =~ /\A[\{\[]/
        # check if the json is a list or string, since these are
        # the only ones that JSON.parse() will work with
        JSON.parse(schema)
      else
        # otherwise it's a primitive, so just strip the quotes
        schema.gsub('"', '')
      end
    end

    # serialization

    def write(object, bytes, schema)
      case schema
      when Hash
        if object.is_a? Hash
          write_map(object, bytes, schema)
        else
          raise ClientError, "serialization error: #{object.inspect} does not match schema #{schema.inspect}"
        end
      when Array
        if object.is_a? Array
          write_list(object, bytes, schema)
        else
          raise ClientError, "serialization error: #{object.inspect} does not match schema #{schema.inspect}"
        end
      when 'string'  then write_bytes(  object, bytes)
      when 'int8'    then write_int8(   object, bytes)
      when 'int16'   then write_int16(  object, bytes)
      when 'int32'   then write_int32(  object, bytes)
      when 'int64'   then write_int64(  object, bytes)
      when 'float32' then write_float32(object, bytes)
      when 'float64' then write_float64(object, bytes)
      when 'date'    then write_date(   object, bytes)
      when 'bytes'   then write_bytes(  object, bytes)
      when 'boolean' then write_boolean(object, bytes)
      else raise ClientError, "unrecognised binary json schema: #{schema.inspect}"
      end
    end

    def write_boolean(object, bytes)
      if object.nil?
        bytes << [BYTE_MIN_VAL].pack('c')
      elsif object
        bytes << 1.chr
      else
        bytes << 0.chr
      end
    end

    def write_string(object, bytes)
      write_bytes(object, bytes)
    end

    def write_int8(object, bytes)
      if object.nil?
        bytes << [BYTE_MIN_VAL].pack('c')
      elsif object > BYTE_MIN_VAL && object <= BYTE_MAX_VAL
        bytes << [object].pack('c')
      else
        raise ClientError, "value out of int8 range: #{object}"
      end
    end

    def write_int16(object, bytes)
      if object.nil?
        bytes << [SHORT_MIN_VAL].pack('n')
      elsif object > SHORT_MIN_VAL && object <= SHORT_MAX_VAL
        bytes << [object].pack('n')
      else
        raise ClientError, "value out of int16 range: #{object}"
      end
    end

    def write_int32(object, bytes)
      if object.nil?
        bytes << [INT_MIN_VAL].pack('N')
      elsif object > INT_MIN_VAL && object <= INT_MAX_VAL
        bytes << [object].pack('N')
      else
        raise ClientError, "value out of int32 range: #{object}"
      end
    end

    def write_int64(object, bytes)
      if object.nil?
        bytes << [INT_MIN_VAL, 0].pack('NN')
      elsif object > LONG_MIN_VAL && object <= LONG_MAX_VAL
        bytes << [object / 2**32, object % 2**32].pack('NN')
      else
        raise ClientError, "value out of int64 range: #{object}"
      end
    end

    def write_float32(object, bytes)
      if object == FLOAT_MIN_VAL
        raise ClientError, "Can't use #{FLOAT_MIN_VAL} because it is used to represent nil"
      else
        bytes << [object || FLOAT_MIN_VAL].pack('g')
      end
    end

    def write_float64(object, bytes)
      if object == DOUBLE_MIN_VAL
        raise ClientError, "Can't use #{DOUBLE_MIN_VAL} because it is used to represent nil"
      else
        bytes << [object || DOUBLE_MIN_VAL].pack('G')
      end
    end

    def write_date(object, bytes)
      if object.nil?
        write_int64(nil, bytes)
      else
        write_int64((object.to_f * 1000).to_i, bytes)
      end
    end

    def write_length(length, bytes)
      if length < SHORT_MAX_VAL
        bytes << [length].pack('n')
      elsif length < STRING_MAX_LEN
        bytes << [length | 0xC0000000].pack('N')
      else
        raise ClientError, 'string is too long to be serialized'
      end
    end

    def write_bytes(object, bytes)
      if object.nil?
        write_int16(-1, bytes)
      else
        write_length(object.length, bytes)
        bytes << object
      end
    end

    def write_map(object, bytes, schema)
      if object.nil?
        bytes << [-1].pack('c')
      else
        bytes << [1].pack('c')
        if object.size != schema.size
          raise ClientError, "Fields of object #{object.inspect} do not match schema #{schema.inspect}"
        end

        schema.sort.each do |key, value_type|
          if object.has_key?(key.to_s)
            write(object[key.to_s], bytes, value_type)
          elsif object.has_key?(key.to_sym)
            write(object[key.to_sym], bytes, value_type)
          else
            raise ClientError, "Object #{object.inspect} does not have #{key} field required by the schema"
          end
        end
      end
    end

    def write_list(object, bytes, schema)
      if schema.length != 1
        raise ClientError, "Schema error: a list must have one item, unlike #{schema.inspect}"
      elsif object.nil?
        write_int16(-1, bytes)
      else
        write_length(object.length, bytes)
        object.each {|item| write(item, bytes, schema.first) }
      end
    end

    # parsing

    def read(input, schema)
      case schema
      when Hash      then read_map(input, schema)
      when Array     then read_list(input, schema)
      when 'string'  then read_bytes(input)
      when 'int8'    then read_int8(input)
      when 'int16'   then read_int16(input)
      when 'int32'   then read_int32(input)
      when 'int64'   then read_int64(input)
      when 'float32' then read_float32(input)
      when 'float64' then read_float64(input)
      when 'date'    then read_date(input)
      when 'bytes'   then read_bytes(input)
      when 'boolean' then read_boolean(input)
      else raise ClientError, "unrecognised binary json schema: #{schema.inspect}"
      end
    end

    def read_map(input, schema)
      return nil if input.read(1).unpack('c') == [-1]
      schema.sort.each_with_object({}) do |(key, value_type), object|
        object[key.to_sym] = read(input, value_type)
      end
    end

    def read_length(input)
      size = input.read(2).unpack('n').first
      if size == 0xFFFF
        -1
      elsif size & 0x8000 > 0
        (size & 0x3FFF) << 16 | input.read(2).unpack('n').first
      else
        size
      end
    end

    def read_list(input, schema)
      size = read_length(input)
      return nil if size < 0
      [].tap do |object|
        size.times { object << read(input, schema.first) }
      end
    end

    def read_boolean(input)
      value = input.read(1).unpack('c').first
      return nil if value < 0
      value > 0
    end

    def read_int8(input)
      value = input.read(1).unpack('c').first
      value unless value == BYTE_MIN_VAL
    end

    def to_signed(value, bits)
      if value >= 2 ** (bits - 1)
        value - 2 ** bits
      else
        value
      end
    end

    def read_int16(input)
      value = to_signed(input.read(2).unpack('n').first, 16)
      value unless value == SHORT_MIN_VAL
    end

    def read_int32(input)
      value = to_signed(input.read(4).unpack('N').first, 32)
      value unless value == INT_MIN_VAL
    end

    def read_int64(input)
      high, low = input.read(8).unpack('NN')
      value = to_signed(high << 32 | low, 64)
      value unless value == LONG_MIN_VAL
    end

    def read_float32(input)
      value = input.read(4).unpack('g').first
      value unless value == FLOAT_MIN_VAL
    end

    def read_float64(input)
      value = input.read(8).unpack('G').first
      value unless value == DOUBLE_MIN_VAL
    end

    def read_date(input)
      timestamp = read_int64(input)
      timestamp && Time.at(timestamp / 1000.0)
    end

    def read_bytes(input)
      length = read_length(input)
      input.read(length) if length >= 0
    end
  end
end
