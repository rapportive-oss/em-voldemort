require 'spec_helper'

describe EM::Voldemort::Router do

  before do
    @logger = Logger.new($stdout)
    @logger.level = Logger::ERROR

    @node_by_partition = {}
    xml = File.read(File.expand_path('fixtures/cluster.xml', File.dirname(__FILE__)))
    Nokogiri::XML(xml).xpath('/cluster/server').each do |node|
      connection = EM::Voldemort::Connection.new(
        :host => node.xpath('host').text,
        :port => node.xpath('socket-port').text.to_i,
        :node_id => node.xpath('id').text.to_i,
        :logger => @logger
      )
      node.xpath('partitions').text.split(/\s*,\s*/).map(&:to_i).each do |partition|
        @node_by_partition[partition] = connection
      end
    end

    @router = EM::Voldemort::Router.new('consistent-routing', 2)
  end

  # Examples generated by loading the Voldemort jar (and dependencies) into JRuby and running:
  #
  # require 'java'
  # cluster_mapper = Java::VoldemortXml::ClusterMapper.new
  # cluster = cluster_mapper.read_cluster(java.io.File.new('fixtures/cluster.xml'))
  # routing = Java::VoldemortRouting::ConsistentRoutingStrategy.new(cluster.nodes, 2)
  # logger = org.apache.log4j.Logger.get_logger('voldemort')
  # logger.level = org.apache.log4j.Level::DEBUG
  # logger.add_appender(org.apache.log4j.ConsoleAppender.new(org.apache.log4j.SimpleLayout.new))
  # routing.route_request('asdf'.to_java_bytes) # should log partition/node IDs to stdout

  it 'should route an empty key' do
    @router.partitions('', @node_by_partition).should == [375, 376]
  end

  it 'should route a key of null bytes' do
    @router.partitions("\x00\x00\x00", @node_by_partition).should == [155, 156]
  end

  it 'should route a binary key' do
    @router.partitions("\xff\x00\xfe\x01\xfd\x02", @node_by_partition).should == [308, 309]
  end

  it 'should route a long key' do
    @router.partitions('long' * 10_000, @node_by_partition).should == [41, 42]
  end

  it 'should route a key whose hash is -2**31' do
    # This tests a special case in voldemort.routing.ConsistentRoutingStrategy.abs which ensures
    # that we always take a positive value mod number of partitions
    @router.partitions([2, 87, 150, 223, 77].pack('C*'), @node_by_partition).should == [307, 308]
  end

  it 'should route to replicas on different nodes' do
    reconfigured_partitions = {
      0 => @node_by_partition[14], # node 0
      1 => @node_by_partition[14],
      2 => @node_by_partition[38], # node 1
      3 => @node_by_partition[38],
      4 => @node_by_partition[5],  # node 2
      5 => @node_by_partition[5],
      6 => @node_by_partition[18], # node 3
      7 => @node_by_partition[18],
    }
    @router.partitions('abcdef', reconfigured_partitions).should == [6, 0]
  end

end
