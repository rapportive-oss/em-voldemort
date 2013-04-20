module EM::Voldemort
  # Implementation of Voldemort's pb0 (protocol buffers) protocol.
  # Very incomplete -- currently only supports the get command.
  module Protocol
    def get_request(store, key)
      Protobuf::Request.new(
        :type => Protobuf::RequestType::GET,
        :should_route => false,
        :store => store.to_s,
        :get => Protobuf::GetRequest.new(:key => key.to_s)
      ).encode.to_s
    end

    def get_response(bytes)
      response = Protobuf::GetResponse.decode(bytes.dup)
      raise "GetResponse error #{response.error.error_code}: #{response.error.error_message}" if response.error
      raise "GetResponse contained no values" if response.versioned.nil? || response.versioned.empty?
      response.versioned.max{|a, b| a.version.timestamp <=> b.version.timestamp }.value
    end
  end
end
