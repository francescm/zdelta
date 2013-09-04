#encoding: utf-8

require 'ldap'
require 'ldap/ldif'
#require 'parser'
require 'ldif'
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
  data = Marshal.load(Marshal.dump(buffer)) # array deep copy
  record = LDAP::LDIF.parse_entry(buffer)
  if record
    {:dn => record.dn, :data => data}
  else
    nil
  end
end

def calculate_diff(buffer, entries)
  new = LDAP::LDIF.parse_entry(buffer)
  old = LDAP::LDIF.parse_entry(entries[new.dn])
  new_ldif = Ldif.new(new.dn, new.attrs)
  old_ldif = Ldif.new(old.dn, old.attrs)
  diff = (old_ldif - new_ldif).to_ldif
  diff
end

# qui ricevo la prima copia dei dati (OLD)
parsed = 0
entries = {}
while true
  parsed += 1

  buffer = ""
  receiver.recv_string(buffer = "")
  if buffer.eql? "__NEXT_STEP__"
    break
  end
  buffer = buffer.split("\n")
  buffer << "\n"
  if entry = parse(buffer)
    entries[entry[:dn]] = entry[:data]
  end
end

# qui i dati di confronto
while true
  parsed += 1
  buffer = ""
  receiver.recv_string(buffer = "")
  if buffer.eql? "__SHUTDOWN__"
    break
  end
  buffer = buffer.split("\n")
  buffer << "\n"
  diff = calculate_diff(buffer, entries)
  if diff
    forwarder.send_string diff
  end
end


forwarder.send_string "__END_OF_DATA__"

