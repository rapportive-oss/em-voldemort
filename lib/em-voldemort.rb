require 'logger'
require 'eventmachine'
require 'beefcake'
require 'nokogiri'

%w(protobuf protocol connection cluster).each do |file|
  require File.join(File.dirname(__FILE__), 'em-voldemort', file)
end
