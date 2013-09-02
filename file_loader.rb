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


def process(publisher, buffer)
  publisher.send_string(buffer.join)
  true
end

def shutdown(publisher)
  publisher.send_string("__SHUTDOWN__")
  true
end


context = ZMQ::Context.new(1)
publisher = context.socket(ZMQ::PUSH)
publisher.bind("ipc://loader.ipc")

File.open(BULK).each_line do |l|
  buffer << l
  if "\n".eql? l
    process(publisher, buffer)
    STDOUT.write "\r#{progress}"
    progress = progress + 1
    if (progress % 10000) == 0
      puts "\r#{progress}"
      new_time = Time.new
      puts "total time : #{new_time - start_time}"
      puts "incremental time : #{new_time - incremental}" if incremental
      incremental = new_time
    end
    buffer.clear
  end
end

1.upto(10) do
  shutdown(publisher)
end

puts
puts entries.size
