#encoding: utf-8
#time ruby -J-Xmx2048m -I . loader.rb

require 'ldap'
require 'ldap/ldif'
require 'parser'
require 'rubygems'
require 'ffi-rzmq'
require 'yaml'

FILE = ENV['DATA_FILE']
CLIENTS = ENV['CLIENTS'].to_i

progress = 0
start_time = Time.new
incremental = nil
buffer = []

entries = []

def process(sender, buffer)
  sender.send_string(buffer.join)
  true
end

def shutdown(sender)
  sender.send_string("__SHUTDOWN__")
  true
end

context = ZMQ::Context.new(1)
sender = context.socket(ZMQ::PUSH)
sender.bind ENV['LOADER_SOCKET']

File.open(FILE).each_line do |l|
  buffer << l
  if "\n".eql? l
    process(sender, buffer)
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

1.upto(CLIENTS) do
  shutdown(sender)
  sleep 0.1
end

