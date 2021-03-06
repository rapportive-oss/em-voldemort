module EM::Voldemort
  # A client for a Voldemort cluster. The cluster is initialized by giving the hostname and port of
  # one of its nodes, and the other nodes are autodiscovered.
  #
  # TODO if one node is down, a request should be retried on a replica.
  class Cluster
    include Protocol

    attr_reader :bootstrap_host, :bootstrap_port, :logger, :cluster_name

    RETRY_BOOTSTRAP_PERIOD = 10 # seconds
    METADATA_STORE_NAME = 'metadata'.freeze
    CLUSTER_INFO_KEY = 'cluster.xml'.freeze
    STORES_INFO_KEY = 'stores.xml'.freeze

    def initialize(options={})
      @bootstrap_host = options[:host] or raise "#{self.class.name} requires :host"
      @bootstrap_port = options[:port] or raise "#{self.class.name} requires :port"
      @logger = options[:logger] || Logger.new($stdout)
      @bootstrap_state = :not_started
      @stores = {}
    end

    # Bootstraps the cluster (discovers all cluster nodes and metadata by connecting to the one node
    # that was specified). Calling #connect is optional, since it also happens automatically when
    # you start making requests.
    def connect
      @bootstrap_timer ||= setup_bootstrap_timer do
        start_bootstrap if @bootstrap_state == :not_started || @bootstrap_state == :failed
      end
      start_bootstrap if @bootstrap_state == :not_started
      @bootstrap
    end

    # Fetches the value associated with a particular key in a particular store. Returns a deferrable
    # that succeeds with the value, or fails with an exception object.
    def get(store_name, key, router=nil)
      when_ready do |deferrable|
        connections = choose_connections(key, router)
        if connections.size == 0
          deferrable.fail(ServerError.new('No connection can handle the request'))
        elsif connections.first.health == :good
          # Send the request to the preferred node, but fall back to others if it fails.
          get_in_sequence(connections, store_name, key, deferrable)
        else
          # The request to the first node will probably fail, but we send it anyway, so that the
          # connection can discover when the node comes back up. Make the request to other
          # connections at the same time to avoid delaying the request.
          get_in_parallel(connections, store_name, key, deferrable)
        end
      end
    end

    # Returns a {Store} object configured for accessing a particular store on the cluster.
    def store(store_name)
      @stores[store_name.to_s] ||= Store.new(self, store_name)
    end

    private

    def setup_bootstrap_timer
      EM.add_periodic_timer(RETRY_BOOTSTRAP_PERIOD) { yield }
    end

    def start_bootstrap
      @bootstrap_state = :started
      @bootstrap_conn = Connection.new(:host => bootstrap_host, :port => bootstrap_port, :logger => logger)
      @bootstrap = EM::DefaultDeferrable.new

      cluster_req = get_from_connection(@bootstrap_conn, METADATA_STORE_NAME, CLUSTER_INFO_KEY)

      cluster_req.callback do |cluster_xml|
        if parse_cluster_info(cluster_xml) == :cluster_info_ok
          stores_req = get_from_connection(@bootstrap_conn, METADATA_STORE_NAME, STORES_INFO_KEY)
          stores_req.callback do |stores_xml|
            parse_stores_info(stores_xml)
            finish_bootstrap
          end
          stores_req.errback do |error|
            logger.warn "Could not load Voldemort's stores.xml: #{error}"
            @bootstrap_state = :failed
            finish_bootstrap
          end
        else
          finish_bootstrap
        end
      end

      cluster_req.errback do |error|
        logger.warn "Could not load Voldemort's cluster.xml: #{error}"
        @bootstrap_state = :failed
        finish_bootstrap
      end
    end

    def finish_bootstrap
      @bootstrap_conn.close
      @bootstrap_conn = nil
      deferrable = @bootstrap
      @bootstrap = nil
      if @bootstrap_state == :complete
        @bootstrap_timer.cancel
        @bootstrap_timer = nil
        deferrable.succeed
      else
        deferrable.fail
      end
    end

    # Delays execution of the block until bootstrap has completed. Returns a new deferrable, and
    # passes the same deferrable to the block when it is executed (it's up to the block to make the
    # deferrable succeed or fail).
    def when_ready(&block)
      connect
      request = EM::DefaultDeferrable.new
      case @bootstrap_state
      when :started, :cluster_info_ok
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
      @node_by_partition = {}
      doc.xpath('/cluster/server').each do |node|
        node_id = node.xpath('id').text.to_i
        connection = Connection.new(
          :host => node.xpath('host').text,
          :port => node.xpath('socket-port').text.to_i,
          :node_id => node_id,
          :logger => logger
        )
        @node_by_id[node_id] = connection
        node.xpath('partitions').text.split(/\s*,\s*/).map(&:to_i).each do |partition|
          raise "duplicate assignment of partition #{partition}" if @node_by_partition[partition]
          @node_by_partition[partition] = connection
        end
      end
      raise 'no partitions defined on cluster' if @node_by_partition.empty?
      (0...@node_by_partition.size).each do |partition|
        raise "missing node assignment for partition #{partition}" unless @node_by_partition[partition]
      end
      @bootstrap_state = :cluster_info_ok
    rescue => e
      logger.warn "Error processing cluster.xml: #{e}"
      @bootstrap_state = :failed
    end

    def parse_stores_info(stores_xml)
      doc = Nokogiri::XML(stores_xml)
      doc.xpath('/stores/store').each do |store|
        store_name = store.xpath('name').text
        @stores[store_name] ||= Store.new(self, store_name)
        @stores[store_name].load_config(store)
      end
      @bootstrap_state = :complete
    rescue => e
      logger.warn "Error processing stores.xml: #{e}"
      @bootstrap_state = :failed
    end

    def choose_connections(key, router=nil)
      if router
        router.partitions(key, @node_by_partition).map do |partition|
          @node_by_partition[partition]
        end.compact
      else
        @node_by_id.values.sample(2) # choose two random connections
      end
    end

    # Makes a 'get' request to the first connection in the given list of connections. If the request
    # fails with a ServerError, retries the request on the next connection in the list, etc,
    # eventually failing if none of the connections can successfully handle the request.
    def get_in_sequence(connections, store_name, key, deferrable)
      raise ArgumentError, 'connections must not be empty' if connections.empty?
      request = get_from_connection(connections.first, store_name, key)
      request.callback {|response| deferrable.succeed(response) }
      request.errback do |error|
        if error.is_a?(ServerError) && connections.size > 1
          get_in_sequence(connections.drop(1), store_name, key, deferrable)
        else
          deferrable.fail(error)
        end
      end
    end

    # Makes a 'get' request to all given connections in parallel. Succeeds with the first successful
    # response, or fails if all connections' requests fail.
    def get_in_parallel(connections, store_name, key, deferrable)
      raise ArgumentError, 'connections must not be empty' if connections.empty?
      responses = 0
      done = false
      connections.each do |connection|
        request = get_from_connection(connection, store_name, key)
        request.callback do |response|
          deferrable.succeed(response) unless done
          done = true
        end
        request.errback do |error|
          if error.is_a?(ClientError) && !done
            deferrable.fail(error)
            done = true
          elsif !done
            responses += 1
            deferrable.fail(error) if responses == connections.size
          end
        end
      end
    end

    # Makes a 'get' request for a particular key to a particular Voldemort store, using a particular
    # connection. Returns a deferrable that succeeds with the value in the store if successful, or
    # fails with an exception object if not.
    def get_from_connection(connection, store_name, key, deferrable=EM::DefaultDeferrable.new)
      request = connection.send_request(get_request(store_name, key))

      request.callback do |response|
        begin
          parsed_response = get_response(response)
        rescue ClientError => error
          deferrable.fail(error)
        rescue => error
          message = "protocol error #{error.class}: #{error.message} while parsing response: #{response.inspect}"
          logger.error(message)
          deferrable.fail(ServerError.new(message))
        else
          deferrable.succeed(parsed_response)
        end
      end

      request.errback {|response| deferrable.fail(response) }
      deferrable
    end
  end
end
