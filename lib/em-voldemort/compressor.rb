module EM::Voldemort
  # Compression/decompression codec for keys and values in a store
  class Compressor

    attr_reader :type, :options

    def initialize(xml)
      @type = xml && xml.xpath('type').text
      @options = xml && xml.xpath('options').text
      if type != nil && type != 'gzip'
        raise "Unsupported compression codec: #{type}"
      end
    end

    def encode(data)
      case type
      when nil
        data
      when 'gzip'
        buffer = StringIO.new
        buffer.set_encoding(Encoding::BINARY)
        gz = Zlib::GzipWriter.new(buffer)
        gz.write(data)
        gz.close
        buffer.rewind
        buffer.string
      end
    end

    def decode(data)
      case type
      when nil
        data
      when 'gzip'
        Zlib::GzipReader.new(StringIO.new(data)).read.force_encoding(Encoding::BINARY)
      end
    end
  end
end
