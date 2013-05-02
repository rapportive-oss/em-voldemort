module EM::Voldemort
  # A client for a Voldemort cluster. The cluster is initialized by giving the hostname and port of
  # one of its nodes, and the other nodes are autodiscovered.
  #
  # TODO: The cluster should automatically route requests to the right node, and transparently
  # reconnect on failure.
  class Cluster
    include Protocol

    attr_reader :bootstrap_host, :bootstrap_port, :logger, :cluster_name

    RETRY_BOOTSTRAP_PERIOD = 10 # seconds
    METADATA_STORE_NAME = 'metadata'.freeze
    CLUSTER_INFO_KEY = 'cluster.xml'.freeze

    def initialize(options={})
      @bootstrap_host = options[:host] or raise "#{self.class.name} requires :host"
      @bootstrap_port = options[:port] or raise "#{self.class.name} requires :port"
      @logger = options[:logger] || Logger.new($stdout)
      @bootstrap_state = :not_started
      @bootstrap_timer = setup_bootstrap_timer do
        start_bootstrap if @bootstrap_state == :not_started || @bootstrap_state == :failed
      end
    end

    # Bootstraps the cluster (discovers all cluster nodes and metadata by connecting to the one node
    # that was specified). Calling #connect is optional, since it also happens automatically when
    # you start making requests.
    def connect
      start_bootstrap if @bootstrap_state == :not_started
      @bootstrap
    end

    # Fetches the value associated with a particular key in a particular store. Returns a deferrable
    # that succeeds with the value, or fails with an exception object.
    def get(store_name, key)
      when_ready do |deferrable|
        get_from_connection(choose_connection(key), store_name, key, deferrable)
      end
    end

    private

    def setup_bootstrap_timer
      EM.add_periodic_timer(RETRY_BOOTSTRAP_PERIOD) { yield }
    end

    def start_bootstrap
      @bootstrap_state = :started
      @bootstrap_conn = Connection.new(:host => bootstrap_host, :port => bootstrap_port, :logger => logger)

      @bootstrap = get_from_connection(@bootstrap_conn, METADATA_STORE_NAME, CLUSTER_INFO_KEY)
      @bootstrap.callback do |cluster_xml|
        parse_cluster_info(cluster_xml)
        finish_bootstrap
      end
      @bootstrap.errback do |error|
        logger.warn "Could not bootstrap Voldemort cluster: #{error}"
        @bootstrap_state = :failed
        finish_bootstrap
      end
    end

    def finish_bootstrap
      @bootstrap_conn.close
      @bootstrap_conn = nil
      @bootstrap = nil
      if @bootstrap_state == :complete
        @bootstrap_timer.cancel
        @bootstrap_timer = nil
      end
    end

    # Delays execution of the block until bootstrap has completed. Returns a new deferrable, and
    # passes the same deferrable to the block when it is executed (it's up to the block to make the
    # deferrable succeed or fail).
    def when_ready(&block)
      connect
      request = EM::DefaultDeferrable.new
      case @bootstrap_state
      when :started
        @bootstrap.callback { yield request }
        @bootstrap.errback { request.fail(ServerError.new('Could not bootstrap Voldemort cluster')) }
      when :complete
        yield request
      when :failed
        request.fail(ServerError.new('Could not bootstrap Voldemort cluster'))
      else
        raise "bad bootstrap_state: #{@bootstrap_state.inspect}"
      end
      request
    end

    # Parses Voldemort's cluster.xml configuration file, as obtained in the bootstrap process.
    def parse_cluster_info(cluster_xml)
      doc = Nokogiri::XML(cluster_xml)
      @cluster_name = doc.xpath('/cluster/name').text
      @node_by_id = {}
      @nodes_by_partition = {}
      doc.xpath('/cluster/server').each do |node|
        node_id = node.xpath('id').text
        connection = Connection.new(
          :host => node.xpath('host').text,
          :port => node.xpath('socket-port').text.to_i,
          :logger => logger
        )
        @node_by_id[node_id] = connection
        node.xpath('partitions').text.split(/\W+/).each do |partition|
          @nodes_by_partition[partition] ||= []
          @nodes_by_partition[partition] << connection
        end
      end
      @bootstrap_state = :complete
    rescue => e
      logger.warn "Error processing cluster.xml: #{e}"
      @bootstrap_state = :failed
    end

    def choose_connection(key)
      # TODO route to the right node, based on key
      @node_by_id.values.first
    end

    # Makes a 'get' request for a particular key to a particular Voldemort store, using a particular
    # connection. Returns a deferrable that succeeds with the value in the store if successful, or
    # fails with an exception object if not.
    def get_from_connection(connection, store_name, key, deferrable=EM::DefaultDeferrable.new)
      request = connection.send_request(get_request(store_name, key))

      request.callback do |response|
        begin
          parsed_response = get_response(response)
        rescue => error
          deferrable.fail(error)
        else
          deferrable.succeed(parsed_response)
        end
      end

      request.errback {|response| deferrable.fail(response) }
      deferrable
    end
  end
end
