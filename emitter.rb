#encoding: utf-8

require 'parser'
require 'rubygems'
require 'ffi-rzmq'
require 'yaml'

def error_check(rc)
  if ZMQ::Util.resultcode_ok?(rc)
    false
  else
    STDERR.puts "Operation failed, errno [#{ZMQ::Util.errno}] description [#{ZMQ::Util.error_string}]"
    caller(1).each { |callstack| STDERR.puts("pull: #{callstack}") }
    true
  end
end

config = YAML.load_file("config.yaml")
output_file = config[:output_file]
catalog_socket = config[:catalog_socket]
CLIENTS = config[:clients].to_i

context = ZMQ::Context.new(1)

receiver = context.socket(ZMQ::PULL)
rc = receiver.bind catalog_socket
error_check rc

start_time = Time.new


stop_signals = 0

entries = []
parsed = 0
continue = true

while continue
  parsed += 1

  rc = receiver.recv_string(buffer = "")
  break if error_check rc

  if buffer.eql? "__END_OF_DATA__"
  then
    stop_signals += 1 
    puts  "received #{stop_signals} stop_signal(s)" if $DEBUG
    continue = false if stop_signals >= ( CLIENTS )
  else
    entries = entries << buffer
#    puts  "\nassembled #{entries.size} entries in #{parsed} chunk(s)"
  end
end

puts "stop signals received: #{stop_signals}; clients: #{CLIENTS}" if $DEBUG
puts "assembled #{entries.size} entries in #{Time.new - start_time}"
puts

File.open(output_file, "w+") do |f|
  entries.each do |diff|
    f.puts diff
    f.puts ""
  end
end

receiver.close
