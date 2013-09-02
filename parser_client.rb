#encoding: utf-8

require 'ldap'
require 'ldap/ldif'
require 'parser'
require 'rubygems'
require 'ffi-rzmq'
require 'yaml'

context = ZMQ::Context.new(1)

puts "Ready to parse ldif ..."
subscriber = context.socket(ZMQ::PULL)
subscriber.connect("ipc://loader.ipc")

forwarder = context.socket(ZMQ::PUSH)
forwarder.setsockopt(ZMQ::IDENTITY, 'parser')
forwarder.bind("ipc://assembler.ipc")

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

#suffix = (0..10).to_a.map {(65 + rand(21)).chr}.join
#filename = "dump-#{suffix}.yaml"
#File.open(filename, "w") {|f| YAML.dump(entries, f)}

forwarder.send_string YAML.dump(entries)

puts
puts "forwarded #{parsed} entries in #{Time.new - start_time}"
