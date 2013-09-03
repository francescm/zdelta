#encoding: utf-8

require 'ldap'
require 'ldap/ldif'
#require 'parser'
require 'rubygems'
require 'ffi-rzmq'
#require 'yaml'

context = ZMQ::Context.new(1)

identity = "parser-#{(0..10).to_a.map {(65 + rand(21)).chr}.join}"

receiver = context.socket(ZMQ::DEALER)
receiver.setsockopt(ZMQ::IDENTITY, identity)
receiver.connect ENV['LOADER_SOCKET']

forwarder = context.socket(ZMQ::PUSH)
forwarder.setsockopt(ZMQ::IDENTITY, identity)
forwarder.connect ENV['CATALOG_SOCKET']

start_time = Time.new

receiver.send_string "#{identity} says: hallo master"

entries = []

def parse_ok(buffer)
  #  record = LDAP::LDIF.parse_entry(buffer)
  entry = Parser.parse(buffer).first
  if entry
    {entry["dn"].first => entry}
  else
    nil
  end
end

def parse(buffer)
  record = LDAP::LDIF.parse_entry(buffer)
  if record
    {record.dn => buffer}
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

