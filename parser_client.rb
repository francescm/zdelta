#encoding: utf-8

require 'ldap'
require 'ldap/ldif'
require 'rubygems'
require 'ffi-rzmq'
require 'yaml'

context = ZMQ::Context.new(1)

receiver = context.socket(ZMQ::PULL)
receiver.connect ENV['LOADER_SOCKET']

identity = "parser-#{(0..10).to_a.map {(65 + rand(21)).chr}.join}"

forwarder = context.socket(ZMQ::PUSH)
forwarder.setsockopt(ZMQ::IDENTITY, identity)
forwarder.connect ENV['CATALOG_SOCKET']

start_time = Time.new

entries = []

def parse(buffer)
  record = LDAP::LDIF.parse_entry(buffer)
end

parsed = 0
while true
  parsed += 1

  buffer = ""
  receiver.recv_string(buffer = "")
  if buffer.eql? "__SHUTDOWN__"
#    puts "#{identity}: #{buffer}"
    break
  end
  buffer = buffer.split("\n")
  buffer << "\n"
  if record = parse(buffer)
    entries << { record.dn => record }
    forwarder.send_string record.dn
  end
end


#filename = "dump-#{suffix}.yaml"
#File.open(filename, "w") {|f| YAML.dump(entries, f)}

forwarder.send_string "__END_OF_DATA__"
puts "#{identity}: __END_OF_DATA__"
#puts "forwarded #{parsed} entries in #{Time.new - start_time} (#{identity})"

