module EM::Voldemort
  # TCP connection to one Voldemort node. The connection can be used to access multiple stores.
  # Automatically reconnects if the connection is lost, but does not automatically retry failed
  # requests (that is the cluster's job).
  class Connection
    attr_reader :host, :port, :protocol, :logger

    DEFAULT_PROTOCOL = 'pb0' # Voldemort's protobuf-based protocol
    STATUS_CHECK_PERIOD = 5 # Every 5 seconds, check on the health of the connection
    REQUEST_TIMEOUT = 5 # If a request takes longer than 5 seconds, close the connection

    def initialize(options={})
      @host = options[:host] or raise "#{self.class.name} requires :host"
      @port = options[:port] or raise "#{self.class.name} requires :port"
      @protocol = options[:protocol] || DEFAULT_PROTOCOL
      @logger = options[:logger] || Logger.new($stdout)
      @timer = setup_status_check_timer(&method(:status_check))
    end

    # Establishes a connection to the node. Calling #connect is optional, since it also happens
    # automatically when you start making requests.
    def connect
      force_connect unless @handler
    end

    # Sends a request to the node, given as a binary string (not including the request size prefix).
    # Establishes a connection if necessary. If a request is already in progress, this request is
    # queued up. Returns a deferrable that succeeds with the node's response (again without the size
    # prefix), or fails if there was a network-level error.
    def send_request(request)
      connect
      @handler.enqueue_request(request)
    end

    # Waits for the outstanding request (if any) to complete, then gracefully shuts down the
    # connection.  Returns a deferrable that succeeds once the connection is closed (never fails).
    def close
      return @closing_deferrable if @closing_deferrable
      @closing_deferrable = EM::DefaultDeferrable.new

      if @handler
        @handler.close_gracefully
      else
        @closing_deferrable.succeed
      end

      @handler = FailHandler.new(self)
      @closing_deferrable
    end

    # Called by the connection handler when the connection is closed for any reason (closed by us,
    # closed by peer, rejected, timeout etc). Do not call from application code.
    def connection_closed(handler, reason=nil)
      logger.info ["Connection to Voldemort node #{host}:#{port} closed", reason].compact.join(': ')
      @handler = FailHandler.new(self) if handler.equal? @handler
      @closing_deferrable.succeed if @closing_deferrable
    end

    private

    def setup_status_check_timer
      EM.add_periodic_timer(STATUS_CHECK_PERIOD) { yield }
    end

    def status_check
      if @closing_deferrable
        # Do nothing (don't reconnect once we've been asked to shut down).
      elsif !@handler || @handler.is_a?(FailHandler)
        # Connect for the first time, or reconnect after failure.
        force_connect
      elsif @handler.in_flight && Time.now - @handler.last_request >= REQUEST_TIMEOUT
        # Request timed out. Pronounce the connection dead, and reconnect.
        @handler.close_connection
        force_connect
      end
    end

    def force_connect
      @handler = EM.connect(host, port, Handler, self)
    rescue EventMachine::ConnectionError => e
      # A synchronous exception is typically thrown on DNS resolution failure
      logger.warn "Cannot connect to Voldemort node: #{e.class.name}: #{e.message}"
      connection_closed(@handler)
      @handler = FailHandler.new(self)
    end


    # EventMachine handler for a Voldemort node connection
    module Handler
      # The EM::Voldemort::Connection object for which we're handling the connection
      attr_reader :connection

      # State machine. One of :connecting, :protocol_proposal, :idle, :request, :disconnected
      attr_reader :state

      # If a request is currently in flight, this is a deferrable that will succeed or fail when the
      # request completes. The protocol requires that only one request can be in flight at once.
      attr_reader :in_flight

      # The time at which the request currently in flight was sent
      attr_reader :last_request

      # Array of [request_data, deferrable] pairs, containing requests that have not yet been sent
      attr_reader :request_queue

      def initialize(connection)
        @connection = connection
        @state = :connecting
        @in_flight = EM::DefaultDeferrable.new
        @last_request = Time.now
        @request_queue = []
      end

      def enqueue_request(request)
        EM::DefaultDeferrable.new.tap do |deferrable|
          request_queue << [request, deferrable]
          send_next_request unless in_flight
        end
      end

      # First action when the connection is established: client tells the server which version of
      # the Voldemort protocol it wants to use
      def send_protocol_proposal(protocol)
        raise ArgumentError, 'protocol must be 3 bytes long' if protocol.bytesize != 3
        raise "unexpected state before protocol proposal: #{@state.inspect}" unless @state == :connecting
        send_data(protocol)
        @state = :protocol_proposal
      end

      # Takes the request at the front of the queue and sends it to the Voldemort node
      def send_next_request
        return if request_queue.empty?
        raise "cannot make a request while in #{@state.inspect} state" unless @state == :idle
        request, @in_flight = request_queue.shift
        send_data([request.size, request].pack('NA*'))
        @recv_buf = ''.force_encoding('BINARY')
        @last_request = Time.now
        @state = :request
      end

      # Connection established (called by EventMachine)
      def post_init
        connection.logger.info "Connected to Voldemort node at #{connection.host}:#{connection.port}"
        send_protocol_proposal(connection.protocol)
        in_flight.errback do |response|
          connection.logger.warn "Voldemort protocol #{connection.protocol} not accepted: #{response.inspect}"
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
            send_next_request
          else
            deferrable.fail("server response: #{data.inspect}")
            close_connection
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
            send_next_request
          end

        else
          raise "Received data in unexpected state: #{@state.inspect}"
        end
      end

      # Connection is asking us to shut down. Wait for the currently in-flight request to complete,
      # but fail any unsent requests in the queue.
      def close_gracefully
        if in_flight
          in_flight.callback { close_gracefully }
          in_flight.errback  { close_gracefully }
        else
          @request_queue.each {|request, deferrable| deferrable.fail('shutdown requested') }
          @request_queue = []
          close_connection
        end
      end

      # Connection closed (called by EventMachine)
      def unbind(reason=nil)
        @state = :disconnected
        deferrable = @in_flight
        @in_flight = nil
        deferrable.fail('connection closed') if deferrable
        connection.connection_closed(self, reason)
      end
    end


    # Quacks like a EM::Voldemort::Connection::Handler, but fails all requests.
    # Useful for representing a connection in an error state.
    class FailHandler
      attr_reader :in_flight

      def initialize(connection)
        @connection = connection
      end

      def enqueue_request(request)
        EM::DefaultDeferrable.new.tap {|deferrable| deferrable.fail('Connection to Voldemort node closed') }
      end

      def close_gracefully
        @connection.connection_closed(self)
      end
    end
  end
end
