#encoding: utf-8

require 'ldap'
require 'ldap/ldif'
require 'ldif'
require 'rubygems'
require 'ffi-rzmq'
require 'json'
require 'logger'
require 'yaml'

config = YAML.load_file("config.yaml")
catalog_socket = config[:catalog_socket]
loader_socket = config[:loader_socket]

context = ZMQ::Context.new(1)

identity = "parser-#{(0..10).to_a.map {(65 + rand(21)).chr}.join}"

$logger = Logger.new("logs/parser_client.log")
$logger.progname = identity
$logger.sev_threshold = Logger::INFO
$logger.debug("hallo")

def error_check(rc)
  if ZMQ::Util.resultcode_ok?(rc)
    false
  else
    $logger.info "Operation failed, errno [#{ZMQ::Util.errno}] description [#{ZMQ::Util.error_string}]"
    caller(1).each { |callstack| @logger.info callstack }
    true
  end
end



receiver = context.socket(ZMQ::DEALER)
receiver.setsockopt(ZMQ::IDENTITY, identity)
receiver.connect loader_socket

forwarder = context.socket(ZMQ::PUSH)
forwarder.setsockopt(ZMQ::IDENTITY, identity)
forwarder.connect catalog_socket

start_time = Time.new

$step = "start"

receiver.send_string "#{identity} says: hallo master"

def read_buffer(json_buffer)
  buffer = JSON.parse json_buffer
  buffer << "\n"
  buffer
end

def get_dn(data)
  tail = data[1] if data[1].match /^ .*/
  head = data.first.split(": ").last
  dn = if tail
         head.strip + tail.strip
       else
         head.strip
       end
  dn
end

def parse(buffer)
  data = Marshal.load(Marshal.dump(buffer)) # array deep copy
  dn = get_dn(data)
  if dn.match /uid=.*,ou=people,dc=unimore,dc=it/
    return {:dn => dn, :data => data}
  else
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
  rc = receiver.recv_string(json_buffer = "")
  ZMQ::Util.resultcode_ok? rc

  if json_buffer.strip.eql? "__NEXT_STEP__"
    $step = "next_step"
    break
  end
  buffer = read_buffer json_buffer

  entry = parse(buffer)
  raise RuntimeError, "parsed empty entry: #{buffer}" unless entry[:dn]
  entries[entry[:dn]] = entry[:data]
end

receiver.send_string "#{identity} says: next step ready"

# then receive matching new data
while true
  parsed += 1
  buffer = ""
  rc = receiver.recv_string(json_buffer = "")
  ZMQ::Util.resultcode_ok? rc

  if json_buffer.strip.eql? "__ADD_STEP__"
    $step = "add_step"
    $logger.debug json_buffer
    break
  end

  buffer = read_buffer json_buffer
  $logger.debug buffer
  dn = get_dn buffer
  $logger.debug("ns: #{dn}")
  res = calc_diff(:mod, buffer, entries[dn])
  raise RuntimeError, "strange dn mismatch: #{dn} <> #{res[:dn]}" unless res[:dn].eql? dn
  $logger.debug("ns: #{res[:diff]}")
  raise RuntimeError, "missing dn #{res[:dn]} in entries" unless entries[res[:dn]]
  entries.delete res[:dn]
  rc = forwarder.send_string res[:diff]
  ZMQ::Util.resultcode_ok? rc
end

$logger.info "add step ready"
receiver.send_string "#{identity} says: add step ready"

# now process add entries
while true

  parsed += 1
  buffer = ""

  rc = receiver.recv_string(json_buffer = "")
  ZMQ::Util.resultcode_ok? rc

  if json_buffer.strip.eql? "__SHUTDOWN__"
    $logger.debug json_buffer
    break
  end
  buffer = read_buffer json_buffer

  res = calc_diff(:add, buffer)
  forwarder.send_string res[:diff]
end

$step = "delete_step"

# and last delete entries
parsed += entries.size
$logger.debug("delete phase")
entries.each do |dn, data|
  res = calc_diff(:del, data)
  forwarder.send_string res[:diff]
end
$logger.info "parsed: #{parsed} entries"
forwarder.send_string "__END_OF_DATA__"

rc = receiver.send_string "#{identity} says: goodbye master"
ZMQ::Util.resultcode_ok? rc

