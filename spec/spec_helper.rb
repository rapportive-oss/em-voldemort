require File.expand_path('../lib/em-voldemort', File.dirname(__FILE__))
require 'rspec'
require 'timecop'

RSpec.configure do |config|
  config.after :each do
    Timecop.return
  end
end
