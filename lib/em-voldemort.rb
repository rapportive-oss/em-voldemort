require 'uri'
require 'zlib'
require 'logger'
require 'eventmachine'
require 'beefcake'
require 'nokogiri'
require 'json'

%w(protobuf protocol errors store router connection cluster compressor binary_json).each do |file|
  require File.join(File.dirname(__FILE__), 'em-voldemort', file)
end
