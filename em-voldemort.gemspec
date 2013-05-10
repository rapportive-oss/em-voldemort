require 'date'

Gem::Specification.new do |s|
  s.name = 'em-voldemort'
  s.authors = ['Rapportive Team']
  s.email = 'rapportive@linkedin.com'
  s.version = '0.1.2'
  s.summary = %q{Client for Voldemort}
  s.description = %q{EventMachine implementation of a Voldemort client. Currently limited to read-only stores.}
  s.homepage = 'https://github.com/rapportive/em-voldemort'
  s.date = Date.today.to_s
  s.files = `git ls-files`.split("\n")
  s.require_paths = %w(lib)

  s.add_dependency 'eventmachine'
  s.add_dependency 'beefcake'
  s.add_dependency 'nokogiri'
  s.add_dependency 'json'

  s.add_development_dependency 'rspec'
  s.add_development_dependency 'timecop'
  s.add_development_dependency 'pry-rescue'
  s.add_development_dependency 'pry-stack_explorer'
end
