require 'spec_helper'

describe EM::Voldemort::Connection do
  before do
    EM::Voldemort::Connection.any_instance.stub(:setup_status_check_timer) do |&timer|
      @status_check_timer = timer
      double('timer', :cancel => nil)
    end

    @logger = Logger.new($stdout)
    @logger.level = Logger::ERROR
    @connection = EM::Voldemort::Connection.new :host => 'localhost', :port => 6666, :logger => @logger

    Timecop.freeze
  end

  def expect_connect(&block)
    EM.should_receive(:connect).once do |host, port, handler_module, *args|
      Class.new(Object) { include handler_module }.new(*args).tap do |handler|
        yield handler
        handler.post_init
      end
    end
  end

  def setup_connection(&block)
    expect_connect do |handler|
      handler.should_receive(:send_data).with('pb0') do |request|
        EM.next_tick do
          handler.receive_data('ok')
          yield handler if block_given?
        end
      end
    end
  end

  def later(elapsed_seconds)
    Timecop.freeze(elapsed_seconds)
    @elapsed_time ||= 0
    if ((@elapsed_time + elapsed_seconds) / EM::Voldemort::Connection::STATUS_CHECK_PERIOD).floor >
        (@elapsed_time / EM::Voldemort::Connection::STATUS_CHECK_PERIOD).floor
      @status_check_timer.call
    end
    @elapsed_time += elapsed_seconds
  end


  it 'should negotiate the protocol at the start of the connection' do
    setup_connection do |handler|
      handler.state.should == :idle
      @connection.health.should == :good
      EM.stop
    end
    EM.run { @connection.connect }
  end

  it 'should disconnect if the server does not support the protocol' do
    expect_connect do |handler|
      handler.should_receive(:send_data) do |request|
        request.should == 'pb0'
        EM.next_tick do
          handler.should_receive(:close_connection) { handler.unbind }
          handler.receive_data('no')
          handler.state.should == :disconnected
          @connection.health.should == :bad
          EM.stop
        end
      end
    end
    EM.run { @connection.connect }
  end

  it 'should try reconnecting after a delay if the host is unresolvable' do
    EM.run do
      EM.should_receive(:connect).once.and_raise(EventMachine::ConnectionError, 'unable to resolve server address')
      @connection.connect # first connection attempt
      @connection.health.should == :bad
      later(2) # no reconnect only 2 seconds after first attempt
      setup_connection do |handler| # second attempt is successful
        handler.state.should == :idle
        @connection.health.should == :good
        EM.stop
      end
      later(4) # 6 seconds after first attempt, status check timer has fired
    end
  end

  it 'should immediately fail requests if the host is unresolvable' do
    EM.run do
      EM.should_receive(:connect).once.and_raise(EventMachine::ConnectionError, 'unable to resolve server address')
      failed1, failed2 = false, false
      @connection.send_request('foo').errback { failed1 = true }
      @connection.send_request('bar').errback { failed2 = true }
      failed1.should be_true
      failed2.should be_true
      EM.stop
    end
  end

  it 'should disconnect and reconnect if protocol negotiation times out' do
    expect_connect do |handler|
      handler.should_receive(:send_data).with('pb0')
      EM.next_tick do
        later(2) # no timeout after only 2 seconds
        handler.should_receive(:close_connection) { handler.unbind }
        EM.should_receive(:connect) do
          @connection.health.should == :bad
          EM.stop
          double('connection', :in_flight => EM::DefaultDeferrable.new)
        end
        later(4) # 6 seconds after sending protocol request, give up
      end
    end
    EM.run { @connection.connect }
  end

  it 'should try reconnecting after a delay if the connection is closed' do
    setup_connection do |handler|
      later(16) # sit idle for a while
      handler.unbind 'connection reset by peer'
      @connection.health.should == :bad
      later(2) # no reconnect only 2 seconds after disconnection
      setup_connection do
        @connection.health.should == :good
        EM.stop
      end
      later(4) # 6 seconds after disconnection, status check timer has fired
    end
    EM.run { @connection.connect }
  end

  it 'should immediately fail requests while the connection is closed' do
    setup_connection do |handler|
      handler.should_receive(:send_data).with([8, 'request1'].pack('NA*')).once do
        EM.next_tick { handler.receive_data([9, 'response1'].pack('NA*')) }
      end
      @connection.send_request('request1').callback do
        handler.unbind
        failed1, failed2 = false, false
        @connection.send_request('request2').errback { failed1 = true }
        @connection.send_request('request3').errback { failed2 = true }
        failed1.should be_true
        failed2.should be_true
        EM.stop
      end
    end
    EM.run { @connection.connect }
  end

  it 'should queue up requests made before the previous request returns' do
    setup_connection do |handler|
      handler.should_receive(:send_data).with([8, 'request1'].pack('NA*')).once do
        EM.next_tick do
          handler.should_receive(:send_data).with([8, 'request2'].pack('NA*')).once do
            EM.next_tick do
              handler.should_receive(:send_data).with([8, 'request3'].pack('NA*')).once do
                EM.next_tick do
                  handler.receive_data([9, 'response3'].pack('NA*'))
                end
              end
              handler.receive_data([9, 'response2'].pack('NA*'))
            end
          end
          handler.receive_data([9, 'response1'].pack('NA*'))
        end
      end
      @connection.send_request('request1').callback {|response| @response1 = response }
      @connection.send_request('request2').callback {|response| @response2 = response }
      @connection.send_request('request3').callback do |response|
        @response1.should == 'response1'
        @response2.should == 'response2'
        response.should == 'response3'
        EM.stop
      end
    end
    EM.run { @connection.connect }
  end

  it 'should queue up requests made while protocol negotiation is in progress' do
    expect_connect do |handler|
      handler.should_receive(:send_data).with('pb0')
      EM.next_tick do
        @connection.health.should == :good
        @connection.send_request('request1')
        later(2)
        EM.next_tick do
          handler.should_receive(:send_data).with([8, 'request1'].pack('NA*')) { EM.stop }
          handler.receive_data('ok')
        end
      end
    end
    EM.run { @connection.connect }
  end

  it 'should close the connection and reconnect if a request takes too long' do
    setup_connection do |handler|
      handler.should_receive(:send_data).with([8, 'request1'].pack('NA*')).once
      @connection.send_request('request1').errback { EM.stop }
      later(2) # not timed out yet after 2 seconds
      @connection.health.should == :good
      handler.should_receive(:close_connection) { handler.unbind }
      EM.should_receive(:connect).and_return(double('handler', :in_flight => EM::DefaultDeferrable.new))
      later(4) # after 6 seconds, should time out and call the errback
      @connection.health.should == :bad
    end
    EM.run { @connection.connect }
  end

  it 'should close the connection when asked to shut down' do
    setup_connection do |handler|
      later(16) # sit idle for a while
      handler.should_receive(:close_connection) { EM.next_tick { handler.unbind } }
      deferrable = @connection.close
      @connection.health.should == :bad
      deferrable.callback { EM.stop }
    end
    EM.run { @connection.connect }
  end

  it 'should handle outstanding requests when asked to shut down' do
    setup_connection do |handler|
      handler.should_receive(:send_data).with([8, 'request1'].pack('NA*')).once do
        EM.next_tick do
          @connection.close.callback do
            @response1.should == 'response1'
            @error2.should be_true
            @error3.should be_true
            EM.stop
          end
          EM.next_tick do
            handler.should_receive(:close_connection) { EM.next_tick { handler.unbind } }
            handler.receive_data([9, 'response1'].pack('NA*'))
          end
        end
      end
      @connection.send_request('request1').callback {|response| @response1 = response }
      @connection.send_request('request2').errback { @error2 = true }
      @connection.send_request('request3').errback { @error3 = true }
    end
    EM.run { @connection.connect }
  end

  it 'should fail outstanding requests when the connection is closed' do
    setup_connection do |handler|
      handler.should_receive(:send_data).with([8, 'request1'].pack('NA*')).once do
        EM.next_tick do
          handler.unbind
          @error1.should be_true
          @error2.should be_true
          EM.stop
        end
      end
      @connection.send_request('request1').errback { @error1 = true }
      @connection.send_request('request2').errback { @error2 = true }
    end
    EM.run { @connection.connect }
  end

  it 'should handle a shutdown request while in error state' do
    EM.run do
      EM.should_receive(:connect).once.and_raise(EventMachine::ConnectionError, 'unable to resolve server address')
      @connection.send_request('foo')
      EM.next_tick do
        @connection.close.callback { EM.stop }
      end
    end
  end
end
