#encoding: utf-8
#time ruby -J-Xmx2048m -I . loader.rb

#require 'ldap'
#require 'ldap/ldif'
#require 'parser'
require 'rubygems'
require 'ffi-rzmq'
#require 'yaml'

OLD = ENV['OLD_FILE']
NEW = ENV['NEW_FILE']
CLIENTS = ENV['CLIENTS'].to_i

progress = 0
start_time = Time.new
incremental = nil
buffer = []

memory = {} #contiene le coppie dn -> client cui sono stati spediti 
            # i dati della entry

def get_dn(buffer)
  dn = buffer.detect{|attr| attr.match /^dn:/}.split(": ").last.chomp
end

def process(socket, buffer, client)
  socket.send_string client, ZMQ::SNDMORE
  socket.send_string(buffer.join)
  dn = get_dn(buffer)
  dn
end

def send_data(socket, buffer, client)
  socket.send_string client, ZMQ::SNDMORE
  socket.send_string(buffer.join)
  true
end

def shutdown(socket, client)
  socket.send_string client, ZMQ::SNDMORE
  socket.send_string("__SHUTDOWN__")
  true
end

def next_step(socket, client)
  socket.send_string client, ZMQ::SNDMORE
  socket.send_string("__NEXT_STEP__")
  true
end

def add_step(socket, client)
  socket.send_string client, ZMQ::SNDMORE
  socket.send_string("__ADD_STEP__")
  true
end

context = ZMQ::Context.new(1)
socket = context.socket(ZMQ::ROUTER)
socket.bind ENV['LOADER_SOCKET']

#aspetto che i client (DEALER) si presentino all'appello

still_missing = ENV['CLIENTS'].to_i
client_addrs = []

while (still_missing != 0)
  socket.recv_string(client = "")
  socket.recv_string(msg = "")
  client_addrs << client
  still_missing = still_missing -1
end

#puts "clients: #{client_addrs.join(", ")}"
#controllo che non ci siano degli indirizzi doppi:
client_addrs.inject [] do |prev, el| 
  raise RuntimeError, "#{el} compare due volte" if prev.include? el
  prev << el
end

File.open(OLD).each_line do |l|
  buffer << l
  if "\n".eql? l
    client = client_addrs[ progress % 8 ]
    dn = process(socket, buffer, client)
    memory[dn] = client
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

client_addrs.each do |client|
  next_step(socket, client)
end

new_entries = {}

File.open(NEW).each_line do |l|
  buffer << l
  if "\n".eql? l
    dn = get_dn(buffer)

    if client = memory[dn] 
      send_data(socket, buffer, client)
      progress = progress + 1
      if (progress % 10000) == 0
        puts "\r#{progress}"
        new_time = Time.new
        puts "total time : #{new_time - start_time}"
        puts "incremental time : #{new_time - incremental}" if incremental
        incremental = new_time
      end
    else
      new_entries[dn] = Marshal.load(Marshal.dump(buffer)) # array deep copy
    end
    buffer.clear
  end
end

client_addrs.each do |client|
  add_step(socket, client)
end

new_entries.each do |dn, data|
  client = client_addrs[ progress % 8 ]
  progress = progress + 1
  send_data(socket, data, client)
end

client_addrs.each do |client|
  shutdown(socket, client)
end

#puts "new_entries: #{new_entries.keys.join(", ")}"
