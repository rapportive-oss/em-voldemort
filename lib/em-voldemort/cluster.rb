module EM::Voldemort
  # A client for a Voldemort cluster. The cluster is initialized by giving the hostname and port of
  # one of its nodes, and the other nodes are autodiscovered.
  #
  # TODO: The cluster should automatically route requests to the right node, and transparently
  # reconnect on failure.
  class Cluster
    include Protocol

    attr_reader :bootstrap_host, :bootstrap_port, :logger, :cluster_name

    METADATA_STORE_NAME = 'metadata'.freeze
    CLUSTER_INFO_KEY = 'cluster.xml'.freeze

    def initialize(options={})
      @bootstrap_host = options[:host] or raise "#{self.class.name} requires :host"
      @bootstrap_port = options[:port] or raise "#{self.class.name} requires :port"
      @logger = options[:logger] || Logger.new($stdout)
    end

    def connect
      @bootstrap = EM::DefaultDeferrable.new
      @bootstrap_conn = Connection.new(:host => bootstrap_host, :port => bootstrap_port, :logger => logger)

      request = get_from_connection(@bootstrap_conn, METADATA_STORE_NAME, CLUSTER_INFO_KEY)
      request.callback do |cluster_xml|
        parse_cluster_info(cluster_xml)
        @bootstrap.succeed
      end
      request.errback do |error|
        logger.warn "Could not bootstrap Voldemort cluster: #{error}"
        @bootstrap.fail
      end

      @bootstrap
    end

    def get(store_name, key)
      raise 'Cluster is not connected' unless @bootstrap
      request = EM::DefaultDeferrable.new
      @bootstrap.callback do
        do_request = get_from_connection(choose_connection(key), store_name, key)
        do_request.callback {|response| request.succeed(response) }
        do_request.errback  {|response| request.fail(response)    }
      end
      @bootstrap.errback do
        request.fail 'Voldemort bootstrap failed'
      end
      request
    end

    private

    def get_from_connection(connection, store_name, key)
      get = EM::DefaultDeferrable.new
      request = connection.send_request(get_request(store_name, key))

      request.callback do |response|
        begin
          get.succeed(get_response(response))
        rescue => e
          get.fail(e)
        end
      end

      request.errback do |response|
        get.fail(response)
      end

      get
    end

    def parse_cluster_info(cluster_xml)
      doc = Nokogiri::XML(cluster_xml)
      @cluster_name = doc.xpath('/cluster/name').text
      @node_by_id = {}
      @nodes_by_partition = {}
      doc.xpath('/cluster/server').each do |node|
        node_id = node.xpath('id').text
        connection = make_connection(node.xpath('host').text, node.xpath('socket-port').text.to_i)
        @node_by_id[node_id] = connection
        node.xpath('partitions').text.split(/\W+/).each do |partition|
          @nodes_by_partition[partition] ||= []
          @nodes_by_partition[partition] << connection
        end
      end
    rescue => e
      logger.warn "Error processing cluster.xml: #{e}"
    end

    def make_connection(host, port)
      if host == @bootstrap_host && port == @bootstrap_port
        @bootstrap_conn
      else
        Connection.new(:host => host, :port => port, :logger => logger)
      end
    end

    def choose_connection(key)
      # TODO route to the right node, based on key
      @node_by_id.values.first
    end
  end
end
