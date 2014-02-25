#encoding: utf-8

require 'ldap'
require 'ldap/ldif'
require 'ldif'
require 'rubygems'
require 'ffi-rzmq'

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

def parse(buffer)
  data = Marshal.load(Marshal.dump(buffer)) # array deep copy
  dn = data.first.split(": ").last
  if dn.match /uid=.*,ou=people,dc=unimore,dc=it/
    {:dn => dn, :data => data}
  else
    raise RuntimeError, "wrong dn format: #{dn}"
  end
end

def calc_diff(mode, buffer, entries = nil)
  case mode
  when :mod
    begin
      new = LDAP::LDIF.parse_entry(buffer)
      old = LDAP::LDIF.parse_entry(entries[new.dn])
      new_ldif = Ldif.new(new.dn, new.attrs)
      old_ldif = Ldif.new(old.dn, old.attrs)
    rescue Exception => e
      puts "eccezione (mod): #{e.to_s}"
      puts "buffer: #{buffer}"
      puts "dn: #{new.dn}"
      puts "old: #{old_ldif}"
      puts "new: #{new_ldif}"
    end

  when :del
    begin
      old = LDAP::LDIF.parse_entry(buffer)
      old_ldif = Ldif.new(old.dn, old.attrs)
      new_ldif = Ldif.new(old.dn, {})
    rescue Exception => e
      puts "eccezione (del): #{e.to_s}"
      puts "dn: #{old.dn}"
      puts "old: #{old_ldif}"
    end
      
  when :add
    begin
      new = LDAP::LDIF.parse_entry(buffer)
      new_ldif = Ldif.new(new.dn, new.attrs)
      old_ldif = Ldif.new(new.dn, {})
    rescue Exception => e
      puts "eccezione (add): #{e.to_s}"
      puts "dn: #{new.dn}"
      puts "new: #{new_ldif}"
    end

  else raise RuntimeError, "mode #{mode} unknown"
  end
  diff = (old_ldif - new_ldif).to_ldif
  {:diff => diff, :dn => new.respond_to?(:dn) ? new.dn : old.dn }
end

# first receive old data
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

  entry = parse(buffer)
  entries[entry[:dn]] = entry[:data]
end

# then receive matching new data
while true
  parsed += 1
  buffer = ""
  receiver.recv_string(buffer = "")
  if buffer.eql? "__ADD_STEP__"
    break
  end
  buffer = buffer.split("\n")
  buffer << "\n"
  res = calc_diff(:mod, buffer, entries)

  entries.delete res[:dn]
  forwarder.send_string res[:diff]
end

# now process add entries
while true
  parsed += 1
  buffer = ""
  receiver.recv_string(buffer = "")
  if buffer.eql? "__SHUTDOWN__"
    break
  end
  buffer = buffer.split("\n")
  buffer << "\n"
  res = calc_diff(:add, buffer)
  forwarder.send_string res[:diff]
end

# and last delete entries
entries.each do |dn, data|
  res = calc_diff(:del, data)
  forwarder.send_string res[:diff]
end

forwarder.send_string "__END_OF_DATA__"

