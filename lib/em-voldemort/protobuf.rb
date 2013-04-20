module EM::Voldemort
  # https://github.com/voldemort/voldemort/blob/master/src/proto/voldemort-client.proto
  class Protobuf
    class ClockEntry
      include Beefcake::Message
      required :node_id, :int32, 1
      required :version, :int64, 2
    end

    class VectorClock
      include Beefcake::Message
      repeated :entries, ClockEntry, 1
      optional :timestamp, :int64, 2
    end

    class Versioned
      include Beefcake::Message
      required :value, :bytes, 1
      required :version, VectorClock, 2
    end

    class Error
      include Beefcake::Message
      required :error_code, :int32, 1
      required :error_message, :string, 2
    end

    class KeyedVersions
      include Beefcake::Message
      required :key, :bytes, 1
      repeated :versions, Versioned, 2
    end

    class GetRequest
      include Beefcake::Message
      optional :key, :bytes, 1
    end

    class GetResponse
      include Beefcake::Message
      repeated :versioned, Versioned, 1
      optional :error, Error, 2
    end

    class GetVersionResponse
      include Beefcake::Message
      repeated :versions, VectorClock, 1
      optional :error, Error, 2
    end

    class GetAllRequest
      include Beefcake::Message
      repeated :keys, :bytes, 1
    end

    class GetAllResponse
      include Beefcake::Message
      repeated :values, KeyedVersions, 1
      optional :error, Error, 2
    end

    class PutRequest
      include Beefcake::Message
      required :key, :bytes, 1
      required :versioned, Versioned, 2
    end

    class PutResponse
      include Beefcake::Message
      optional :error, Error, 1
    end

    class DeleteRequest
      include Beefcake::Message
      required :key, :bytes, 1
      required :version, VectorClock, 2
    end

    class DeleteResponse
      include Beefcake::Message
      required :success, :bool, 1
      optional :error, Error, 2
    end

    module RequestType
      GET = 0
      GET_ALL = 1
      PUT = 2
      DELETE = 3
      GET_VERSION = 4
    end

    class Request
      include Beefcake::Message
      required :type, RequestType, 1
      required :should_route, :bool, 2, :default => false
      required :store, :string, 3
      optional :get, GetRequest, 4
      optional :getAll, GetAllRequest, 5
      optional :put, PutRequest, 6
      optional :delete, DeleteRequest, 7
      optional :requestRouteType, :int32, 8
    end
  end
end
