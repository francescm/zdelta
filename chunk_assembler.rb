#encoding: utf-8

require 'ldap'
require 'ldap/ldif'
require 'parser'
require 'rubygems'
require 'ffi-rzmq'
require 'yaml'

context = ZMQ::Context.new(1)

receiver = context.socket(ZMQ::ROUTER)
receiver.bind ENV['CATALOG_SOCKET']

start_time = Time.new

CLIENTS = ENV['CLIENTS'].to_i
stop_signals = 0

entries = []
parsed = 0
continue = true

while continue
  parsed += 1

  receiver.recv_string(sender = "")

  receiver.recv_string(buffer = "")

  if buffer.eql? "__END_OF_DATA__"
  then
    stop_signals += 1 
    puts  "received #{stop_signals} stop_signal(s)"
    continue = false if stop_signals >= ( CLIENTS )
  else
    entries = entries << {buffer => sender}
#    puts  "\nassembled #{entries.size} entries in #{parsed} chunk(s)"
  end
end

puts "stop signals received: #{stop_signals}; clients: #{CLIENTS}"
puts "assembled #{entries.size} entries in #{Time.new - start_time}"

