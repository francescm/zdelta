#encoding: utf-8

require 'ldap'
require 'ldap/ldif'
require 'parser'
require 'rubygems'
require 'ffi-rzmq'

context = ZMQ::Context.new(1)

puts "Ready to parse ldif ..."
subscriber = context.socket(ZMQ::PULL)
#subscriber.connect("tcp://localhost:5556")
subscriber.connect("ipc://weather.ipc")
#rc = subscriber.setsockopt(ZMQ::SUBSCRIBE, "")
#ZMQ::Util.resultcode_ok?(rc) ? puts("succeeded") : puts("failed")

start_time = Time.new

entries = []
parsed = 0
while true
  parsed += 1

  buffer = ""
  subscriber.recv_string(buffer)
  break if buffer.eql? "__SHUTDOWN__"
  buffer = buffer.split("\n")
  buffer << ""
  parsed_entries = Parser.parse(buffer)
  entries << parsed_entries.first
  STDOUT.write  "\r#{parsed}"
end

puts
puts "parsed #{entries.size} entries in #{Time.new - start_time}"
