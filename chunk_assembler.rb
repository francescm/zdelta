#encoding: utf-8

require 'ldap'
require 'ldap/ldif'
require 'parser'
require 'rubygems'
require 'ffi-rzmq'
require 'yaml'

context = ZMQ::Context.new(1)

puts "Ready to assemble ldif ..."
subscriber = context.socket(ZMQ::PULL)
subscriber.connect("ipc://assembler.ipc")

start_time = Time.new

CLIENTS = 8
stop_signal = 0

entries = []
parsed = 0
continue = true

while continue
  parsed += 1

  buffer = ""
  subscriber.recv_string(buffer)
  if buffer.eql? "__END_OF_DATA__"
  then
    stop_signal += 1 
    puts  "received #{stop_signal} stop_signal(s)"
    continue = false if stop_signal >= CLIENTS
  else
    entries = entries + YAML.load(buffer)
    puts  "\nassembled #{entries.size} entries in #{parsed} chunk(s)"
  end
end


puts
puts "assembled #{entries.size} entries in #{Time.new - start_time}"


