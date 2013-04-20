module EM::Voldemort
  # TCP connection to one Voldemort node. The connection can be used to access multiple stores.
  # Does not automatically reconnect on failure -- that's the cluster's job.
  class Connection
    attr_reader :host, :port, :protocol, :logger

    DEFAULT_PROTOCOL = 'pb0' # Voldemort's protobuf-based protocol

    def initialize(options={})
      @host = options[:host] or raise "#{self.class.name} requires :host"
      @port = options[:port] or raise "#{self.class.name} requires :port"
      @protocol = options[:protocol] || DEFAULT_PROTOCOL
      @logger = options[:logger] || Logger.new($stdout)
    end

    def send_request(request)
      handler.send_request(request)
    end

    # Waits for any outstanding requests to complete, then gracefully shuts down the connection.
    # Returns a deferrable that succeeds once the connection is closed (never fails).
    def close
      return @closing_deferrable if @closing_deferrable
      @closing_deferrable = EM::DefaultDeferrable.new

      if @handler && @handler.in_flight
        @handler.in_flight.callback { @handler.close_connection }
        @handler.in_flight.errback  { @handler.close_connection }
      elsif @handler
        @handler.close_connection
      else
        @closing_deferrable.succeed
      end

      @closing_deferrable
    end

    # Called by the connection handler when the connection is closed for any reason (closed by us,
    # closed by peer, rejected, timeout etc). Do not call from application code.
    def connection_closed(reason=nil)
      logger.info ["Connection to Voldemort node #{host}:#{port} closed", reason].compact.join(': ')
      @handler = nil
      @closing_deferrable.succeed if @closing_deferrable
    end

    private

    def handler
      @handler ||= EM.connect(host, port, Handler, self)
    rescue EventMachine::ConnectionError => e
      # A synchronous exception is typically thrown on DNS resolution failure
      logger.warn "Cannot connect to Voldemort node: #{e.class.name}: #{e.message}"
      connection_closed
      @handler = FailHandler.new(self)
    end


    # EventMachine handler for a Voldemort node connection
    module Handler
      # The EM::Voldemort::Connection object for which we're handling the connection
      attr_reader :connection

      # If a request is currently in flight, this is a deferrable that will succeed or fail when the
      # request completes. The protocol requires that only one request can be in flight at once.
      attr_reader :in_flight

      def initialize(connection)
        @connection = connection
        @state = :connecting
        @in_flight = EM::DefaultDeferrable.new
      end

      def send_protocol_proposal(protocol)
        raise ArgumentError, 'protocol must be 3 bytes long' if protocol.bytesize != 3
        raise "unexpected state before protocol proposal: #{@state.inspect}" unless @state == :connecting
        send_data(protocol)
        @state = :protocol_proposal
      end

      def send_request(request)
        deferrable = EM::DefaultDeferrable.new
        when_ready do
          send_data([request.size, request].pack('NA*'))
          @recv_buf = ''.force_encoding('BINARY')
          @state = :request
          @in_flight = deferrable
        end
        deferrable
      end

      def when_ready(&block)
        if in_flight
          in_flight.callback(&block)
          in_flight.errback(&block)
        else
          yield
        end
      end

      # Connection established (called by EventMachine)
      def post_init
        connection.logger.info "Connected to Voldemort node at #{connection.host}:#{connection.port}"
        send_protocol_proposal(connection.protocol)
        in_flight.errback do |response|
          connection.logger.warn "Voldemort node rejected protocol #{connection.protocol} with response #{response.inspect}"
          close_connection
        end
      end

      # The Voldemort node is talking to us (called by EventMachine)
      def receive_data(data)
        case @state
        when :protocol_proposal
          deferrable = @in_flight
          @state = :idle
          @in_flight = nil
          if data == 'ok'
            deferrable.succeed
          else
            deferrable.fail(data)
          end

        when :request
          @recv_buf << data
          response_size = @recv_buf.unpack('N').first || 0
          if @recv_buf.bytesize >= response_size
            response = @recv_buf[4, response_size]
            deferrable = @in_flight
            @state = :idle
            @in_flight = @recv_buf = nil
            deferrable.succeed(response)
          end

        else
          raise "Received data in unexpected state: #{@state.inspect}"
        end
      end

      # Connection closed (called by EventMachine)
      def unbind(reason=nil)
        in_flight.fail if in_flight
        connection.connection_closed(reason)
      end
    end


    # Quacks like a EM::Voldemort::Connection::Handler, but fails all requests.
    # Useful for representing a connection in an error state.
    class FailHandler
      attr_reader :in_flight

      def initialize(connection)
        @connection = connection
      end

      def send_request(request)
        EM::DefaultDeferrable.new.tap(&:fail)
      end

      def close_connection_after_writing
        @connection.connection_closed
      end
    end
  end
end
