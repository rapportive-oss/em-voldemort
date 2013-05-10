require 'spec_helper'

describe EM::Voldemort::BinaryJson do
  describe 'encoding strings' do
    before do
      @codec = EM::Voldemort::BinaryJson.new(0 => '"string"')
    end

    it 'should encode short strings' do
      @codec.encode('hello').should == "\x00\x00\x05hello"
    end

    it 'should decode short strings' do
      @codec.decode("\x00\x00\x05hello").should == 'hello'
    end

    it 'should encode strings between 16kB and 32kB in length' do
      @codec.encode('hellohello' * 1700).should == "\x00\x42\x68" + 'hellohello' * 1700
    end

    it 'should decode strings between 16kB and 32kB in length' do
      @codec.decode("\x00\x42\x68" + 'hellohello' * 1700).should == 'hellohello' * 1700
    end

    it 'should encode strings above 32kB in length' do
      @codec.encode('hellohello' * 3400).should == "\x00\xC0\x00\x84\xd0" + 'hellohello' * 3400
    end

    it 'should decode strings above 32kB in length' do
      @codec.decode("\x00\xC0\x00\x84\xd0" + 'hellohello' * 3400).should == 'hellohello' * 3400
    end
  end
end
