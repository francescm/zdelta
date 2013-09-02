#encoding: utf-8

#require 'ldap'
#require 'ldap/ldif'
require 'parser'
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
  #  record = LDAP::LDIF.parse_entry(buffer)
  entry = Parser.parse(buffer).first
  if entry
    {entry["dn"].first => entry}
  else
    nil
  end
end

parsed = 0
while true
  parsed += 1

  buffer = ""
  receiver.recv_string(buffer = "")
  if buffer.eql? "__SHUTDOWN__"
    break
  end
  buffer = buffer.split("\n")
  buffer << "\n"
  if entry = parse(buffer)
    entries << entry
    forwarder.send_string entry.keys.first # e' il dn
  end
end

forwarder.send_string "__END_OF_DATA__"

