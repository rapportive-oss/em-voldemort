require 'spec_helper'

describe EM::Voldemort::Cluster do
  before do
    @timer_double = double('timer', :cancel => nil)
    EM::Voldemort::Cluster.any_instance.stub(:setup_bootstrap_timer) do |&timer|
      @bootstrap_timer = timer
      @timer_double
    end

    @logger = Logger.new($stdout)
    @logger.level = Logger::ERROR
    @cluster = EM::Voldemort::Cluster.new :host => 'localhost', :port => 6666, :logger => @logger

    Timecop.freeze
  end

  def request(store, key)
    EM::Voldemort::Protobuf::Request.new(
      :type => EM::Voldemort::Protobuf::RequestType::GET,
      :should_route => false,
      :store => store.to_s,
      :get => EM::Voldemort::Protobuf::GetRequest.new(:key => key.to_s)
    ).encode.to_s
  end

  def success_response(value)
    EM::Voldemort::Protobuf::GetResponse.new(
      :versioned => [EM::Voldemort::Protobuf::Versioned.new(
        :value => value,
        :version => EM::Voldemort::Protobuf::VectorClock.new(
          :entries => [], # read-only stores leave this empty
          :timestamp => (Time.now.to_r * 1000).to_i
        )
      )]
    ).encode.to_s
  end

  def error_response(code, message)
    EM::Voldemort::Protobuf::GetResponse.new(
      :error => EM::Voldemort::Protobuf::Error.new(:error_code => code, :error_message => message)
    ).encode.to_s
  end

  def cluster_xml(partitions_by_host={})
    servers = ''
    partitions_by_host.each_with_index do |(host, partitions), index|
      servers << '<server>'
      servers << "<id>#{index}</id>"
      servers << "<host>#{host}</host>"
      servers << '<http-port>8081</http-port><socket-port>6666</socket-port><admin-port>6667</admin-port>'
      servers << "<partitions>#{partitions.join(', ')}</partitions>"
      servers << '</server>'
    end
    "<cluster><name>example-cluster</name>#{servers}</cluster>"
  end

  def stores_xml(properties_by_name={})
    stores = ''
    properties_by_name.each_pair do |name, properties|
      stores << '<store>'
      stores << "<name>#{name}</name>"
      stores << '<persistence>read-only</persistence>'
      stores << '<routing-strategy>consistent-routing</routing-strategy>'
      stores << '<routing>client</routing>'
      stores << '<replication-factor>2</replication-factor>'
      stores << '<required-reads>1</required-reads>'
      stores << '<required-writes>1</required-writes>'
      stores << '<key-serializer>'
      stores << "<type>#{properties[:key_type]}</type>"
      stores << "<schema-info version=\"0\">#{properties[:key_schema]}</schema-info>"
      stores << '</key-serializer>'
      stores << '<value-serializer>'
      stores << "<type>#{properties[:value_type]}</type>"
      properties[:value_schemas].each_pair do |version, schema|
        stores << "<schema-info version=\"#{version}\">#{schema}</schema-info>"
      end
      stores << "<compression><type>#{properties[:compression]}</type></compression>"
      stores << '</value-serializer>'
      stores << '</store>'
    end
    "<stores>#{stores}</stores>"
  end

  def expect_bootstrap(cluster_info={}, stores_info={})
    @bootstrap_connection = double('bootstrap connection')
    EM::Voldemort::Connection.should_receive(:new).
      with(:host => 'localhost', :port => 6666, :logger => @logger).
      and_return(@bootstrap_connection)

    cluster_request = EM::DefaultDeferrable.new
    @bootstrap_connection.should_receive(:send_request).with(request('metadata', 'cluster.xml')) do
      EM.next_tick do
        stores_request = EM::DefaultDeferrable.new
        @bootstrap_connection.should_receive(:send_request).with(request('metadata', 'stores.xml')) do
          EM.next_tick do
            stores_request.succeed(success_response(stores_xml(stores_info)))
          end
          stores_request
        end
        cluster_request.succeed(success_response(cluster_xml(cluster_info)))
      end
      cluster_request
    end

    @bootstrap_connection.should_receive(:close) { yield if block_given? }
  end


  it 'should request cluster.xml and stores.xml when bootstrapping' do
    expect_bootstrap
    EM.run { @cluster.connect.callback { EM.stop } }
  end

  it 'should make a connection to each node in the cluster' do
    expect_bootstrap('voldemort0.example.com' => [0, 1, 2, 3], 'voldemort1.example.com' => [4, 5, 6, 7])
    EM::Voldemort::Connection.should_receive(:new).
      with(:host => 'voldemort0.example.com', :port => 6666, :logger => @logger).
      and_return(double('Connection 0'))
    EM::Voldemort::Connection.should_receive(:new).
      with(:host => 'voldemort1.example.com', :port => 6666, :logger => @logger).
      and_return(double('Connection 1'))
    EM.run { @cluster.connect.callback { EM.stop } }
  end

  it 'should retry bootstrapping if it fails' do
    EM.run do
      request1 = EM::DefaultDeferrable.new
      connection1 = double('connection attempt 1', :send_request => request1, :close => nil)
      EM::Voldemort::Connection.should_receive(:new).and_return(connection1)
      @cluster.connect
      @bootstrap_timer.call
      EM.next_tick do
        request1.fail(EM::Voldemort::ServerError.new('connection refused'))
        EM.next_tick do
          request2 = EM::DefaultDeferrable.new
          connection2 = double('connection attempt 2', :send_request => request2, :close => nil)
          EM::Voldemort::Connection.should_receive(:new).and_return(connection2)
          @bootstrap_timer.call
          EM.next_tick do
            request2.fail(EM::Voldemort::ServerError.new('connection refused'))
            EM.next_tick do
              expect_bootstrap { EM.stop }
              @timer_double.should_receive(:cancel)
              @bootstrap_timer.call
            end
          end
        end
      end
    end
  end

  it 'should delay requests until bootstrapping is complete' do
    metadata_request = EM::DefaultDeferrable.new
    bootstrap = double('bootstrap connection', :send_request => metadata_request, :close => nil)
    EM::Voldemort::Connection.should_receive(:new).and_return(bootstrap)
    connection = double('connection')
    EM::Voldemort::Connection.should_receive(:new).and_return(connection)
    EM.run do
      @cluster.get('store1', 'request1').callback {|response| @response1 = response }
      @cluster.get('store1', 'request2').callback do |response|
        @response1.should == 'response1'
        response.should == 'response2'
        EM.stop
      end
      EM.next_tick do
        connection.should_receive(:send_request).with(request('store1', 'request1')) do
          EM::DefaultDeferrable.new.tap do |deferrable|
            EM.next_tick { deferrable.succeed(success_response('response1')) }
          end
        end
        connection.should_receive(:send_request).with(request('store1', 'request2')) do
          EM::DefaultDeferrable.new.tap do |deferrable|
            EM.next_tick { deferrable.succeed(success_response('response2')) }
          end
        end
        metadata_request.succeed(success_response(cluster_xml('voldemort0.example.com' => [0, 1, 2, 3])))
      end
    end
  end

  it 'should fail requests if bootstrapping failed' do
    EM.run do
      metadata_request = EM::DefaultDeferrable.new
      bootstrap = double('bootstrap connection', :send_request => metadata_request, :close => nil)
      EM::Voldemort::Connection.should_receive(:new).and_return(bootstrap)
      @cluster.get('store1', 'request1').errback {|error| @error = error }
      EM.next_tick do
        @error.should be_nil
        metadata_request.fail(EM::Voldemort::ServerError.new('connection refused'))
        @error.should be_a(EM::Voldemort::ServerError)
        EM.stop
      end
    end
  end
end
