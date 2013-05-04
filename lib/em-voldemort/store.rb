module EM::Voldemort
  # Provides access to one particular store on a Voldemort cluster. Deals with encoding of keys and
  # values.
  class Store

    attr_reader :store_name

    # Internal -- don't call this from application code. Use {Cluster#store} instead.
    def initialize(cluster, store_name)
      @cluster = cluster
      @store_name = store_name.to_s
    end

    # Internal
    def load_config(xml)
      @persistence = xml.xpath('persistence').text
      @router = Router.new(xml.xpath('routing-strategy').text, xml.xpath('replication-factor').text.to_i)
      @key_serializer = make_serializer(xml.xpath('key-serializer').first)
      @key_compressor = Compressor.new(xml.xpath('key-serializer/compression').first)
      @value_serializer = make_serializer(xml.xpath('value-serializer').first)
      @value_compressor = Compressor.new(xml.xpath('value-serializer/compression').first)
    end

    # Fetches the value associated with a particular key in this store. Returns a deferrable that
    # succeeds with the value, or fails with an exception object. If a serializer is configured for
    # the store, the key is automatically serialized and the value automatically unserialized.
    def get(key)
      EM::DefaultDeferrable.new.tap do |request|
        if @persistence
          get_after_bootstrap(key, request)
        else
          bootstrap = @cluster.connect
          if bootstrap
            bootstrap.callback { get_after_bootstrap(key, request) }
            bootstrap.errback do
              request.fail(ServerError.new('Could not bootstrap Voldemort cluster'))
            end
          else
            request.fail(ClientError.new("Store #{store_name} is not configured on the cluster"))
          end
        end
      end
    end

    private

    def get_after_bootstrap(key, deferrable)
      if @persistence.nil?
        deferrable.fail(ClientError.new("Store #{store_name} is not configured on the cluster"))
      elsif @persistence != 'read-only'
        deferrable.fail(ClientError.new("Sorry, accessing #{persistence} stores is not yet supported"))
      else
        begin
          encoded_key = encode_key(key)
        rescue => error
          deferrable.fail(error)
        else
          request = @cluster.get(store_name, encoded_key, @router)
          request.errback {|error| deferrable.fail(error) }

          request.callback do |response|
            begin
              value = decode_value(response)
            rescue => error
              deferrable.fail(error)
            else
              deferrable.succeed(value)
            end
          end
        end
      end
    end

    def encode_key(key)
      @key_compressor.encode(@key_serializer.encode(key))
    end

    def decode_value(value)
      @value_serializer.decode(@value_compressor.decode(value))
    end

    def make_serializer(xml)
      if xml.xpath('type').text == 'json'
        has_version_tag = true
        schemas = xml.xpath('schema-info').each_with_object({}) do |schema, hash|
          has_version_tag = false if schema['version'] == 'none'
          hash[schema['version'].to_i] = schema.text
        end
        BinaryJson.new(schemas, has_version_tag)
      else
        NullSerializer.new
      end
    end

    class NullSerializer
      def encode(data); data; end
      def decode(data); data; end
    end
  end
end
