#encoding: utf-8

require 'ldap'
require 'ldap/ldif'
require 'ldif'
require 'rubygems'
require 'ffi-rzmq'
require 'json'
require 'logger'

context = ZMQ::Context.new(1)

identity = "parser-#{(0..10).to_a.map {(65 + rand(21)).chr}.join}"
logger = Logger.new("logs/parser_client.log")
logger.progname = identity
logger.sev_threshold = Logger::DEBUG
logger.debug("hallo")

receiver = context.socket(ZMQ::DEALER)
receiver.setsockopt(ZMQ::IDENTITY, identity)
receiver.connect ENV['LOADER_SOCKET']

forwarder = context.socket(ZMQ::PUSH)
forwarder.setsockopt(ZMQ::IDENTITY, identity)
forwarder.connect ENV['CATALOG_SOCKET']

start_time = Time.new

receiver.send_string "#{identity} says: hallo master"

def read_buffer(json_buffer)
  buffer = JSON.parse json_buffer
  buffer << "\n"
  buffer
end

def parse(buffer)
  data = Marshal.load(Marshal.dump(buffer)) # array deep copy
  dn = data.first.split(": ").last
  if dn.match /uid=.*,ou=people,dc=unimore,dc=it/
    return {:dn => dn.strip, :data => data}
  else
    logger.error("wrong dn format: #{dn}")
    raise RuntimeError, "wrong dn format: #{dn}"
  end
end

def calc_diff(mode, buffer, other_data = nil)
  new_ldif = nil
  old_ldif = nil
  comparison = Marshal.load(Marshal.dump(other_data))
  case mode
  when :mod
    new = LDAP::LDIF.parse_entry(buffer)
    old = LDAP::LDIF.parse_entry(comparison)
    new_ldif = Ldif.new(new.dn, new.attrs)
    old_ldif = Ldif.new(old.dn, old.attrs)
  when :del
    old = LDAP::LDIF.parse_entry(buffer)
    old_ldif = Ldif.new(old.dn, old.attrs)
    new_ldif = Ldif.new(old.dn, {})
  when :add
    new = LDAP::LDIF.parse_entry(buffer)
    new_ldif = Ldif.new(new.dn, new.attrs)
    old_ldif = Ldif.new(new.dn, {})
  else raise RuntimeError, "mode #{mode} unknown"
  end
  diff = (old_ldif - new_ldif).to_ldif
  {:diff => diff, :dn => new.respond_to?(:dn) ? new.dn : old.dn }
end

# first receive old data
parsed = 0
entries = {}
while true
  
#  parsed += 1

  buffer = ""
  receiver.recv_string(json_buffer = "")
  if json_buffer.strip.eql? "__NEXT_STEP__"
    logger.debug json_buffer
    break
  end
  buffer = read_buffer json_buffer

  entry = parse(buffer)
  raise RuntimeError, "parsed empty entry: #{buffer}" unless entry[:dn]
  entries[entry[:dn]] = entry[:data]
end

# then receive matching new data
while true
  parsed += 1
  buffer = ""
  receiver.recv_string(json_buffer = "")

  if json_buffer.strip.eql? "__ADD_STEP__"
    logger.debug json_buffer
    break
  end
  buffer = read_buffer json_buffer
  dn = buffer.detect{|attr| attr.match /^dn:/}.split(": ").last.chomp
  res = calc_diff(:mod, buffer, entries[dn])
  raise RuntimeError, "strange dn mismatch: #{dn} <> #{res[:dn]}" unless res[:dn].eql? dn
  entries.delete res[:dn]
  forwarder.send_string res[:diff]
end

# now process add entries
while true

  parsed += 1
  buffer = ""
  receiver.recv_string(json_buffer = "")
  if json_buffer.strip.eql? "__SHUTDOWN__"
    logger.debug json_buffer
    break
  end
  buffer = read_buffer json_buffer

  res = calc_diff(:add, buffer)
  forwarder.send_string res[:diff]
end

# and last delete entries
parsed += entries.size
logger.debug("delete phase")
entries.each do |dn, data|
  res = calc_diff(:del, data)
  forwarder.send_string res[:diff]
end
logger.info "parsed: #{parsed} entries"
forwarder.send_string "__END_OF_DATA__"

