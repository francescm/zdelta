#encoding: utf-8
#time ruby -J-Xmx2048m -I . loader.rb

require 'ldap'
require 'ldap/ldif'
require 'parser'
require 'rubygems'
require 'ffi-rzmq'

BULK = "bulk.ldif"

progress = 0
start_time = Time.new
incremental = nil
buffer = []

entries = []

def process_old(entries, buffer)
  entry = LDAP::LDIF.parse_entry(buffer)
  entries << entry
  true
end

def process_parser(entries, buffer)
  buffer << ""
  parsed_entries = Parser.parse(buffer)
  entries << parsed_entries.first
end

def zeromq
  context = ZMQ::Context.new(1)
  publisher = context.socket(ZMQ::PUB)
  publisher.bind("ipc://weather.ipc")
#  publisher.bind("tcp://*:5556")
  publisher
end

def process(publisher, buffer)
   publisher.send_string(buffer.join)
end

publisher = zeromq

File.open(BULK).each_line do |l|
  buffer << l
  if "\n".eql? l
#    process(entries, buffer)
    process(publisher, buffer)
    STDOUT.write "\r#{progress}"
    progress = progress + 1
    if (progress % 1000) == 0
      puts "\r#{progress}"
      new_time = Time.new
      puts "total time : #{new_time - start_time}"
      puts "incremental time : #{new_time - incremental}" if incremental
      incremental = new_time
    end
    buffer.clear
  end
end

puts
puts entries.size
