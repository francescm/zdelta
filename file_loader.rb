#encoding: utf-8

require 'rubygems'
require 'ldap'
require 'ldap/ldif'
require 'ffi-rzmq'
require 'json'
require 'logger'

OLD = ENV['OLD_FILE']
NEW = ENV['NEW_FILE']
CLIENTS = ENV['CLIENTS'].to_i

progress = 0
start_time = Time.new
incremental = nil
buffer = []

memory = {} #holds the map dn -> client whom data has been send

def error_check(rc)
  if ZMQ::Util.resultcode_ok?(rc)
    false
  else
    puts "Operation failed, errno [#{ZMQ::Util.errno}] description [#{ZMQ::Util.error_string}]"
    caller(1).each { |callstack| puts callstack }
    true
  end
end


def get_dn(buffer)
  begin
    dn = buffer.detect{|attr| attr.match /^dn:/}.split(": ").last.chomp
  rescue
    puts "Missing dn in: "
    puts buffer
    exit 0
  end
end

def process(socket, buffer, client)
  send_data(socket, buffer, client)
  dn = get_dn(buffer)
  dn
end

def send_data(socket, buffer, client)
  socket.send_string client, ZMQ::SNDMORE
  socket.send_string(JSON.generate(buffer))
  true
end

def shutdown(socket, client)
  deal_message(socket, client, "__SHUTDOWN__")
end

def next_step(socket, client)
  deal_message(socket, client, "__NEXT_STEP__")
end

def add_step(socket, client)
  deal_message(socket, client, "__ADD_STEP__")
end

def deal_message(socket, client, message)
  socket.send_string client, ZMQ::SNDMORE
  socket.send_string message
  true  
end

client_addrs = []
$loggers = {}


def wait_all(socket)
  still_missing = CLIENTS
  while (still_missing != 0)
    socket.recv_string(client = "")
    socket.recv_string(msg = "")
    yield client
    still_missing = still_missing -1
  end
end


context = ZMQ::Context.new(1)
socket = context.socket(ZMQ::ROUTER)
socket.setsockopt(ZMQ::SNDHWM, 10000)
socket.bind ENV['LOADER_SOCKET']

# wait for the clients (DEALER) to show themself

wait_all(socket) do |client| 
  client_addrs << client
end


# check no two clients share the same address
client_addrs.inject [] do |prev, el| 
  raise RuntimeError, "#{el} shows twice" if prev.include? el
  prev << el
end

File.open(OLD).each_line do |l|
  if "\n".eql? l
    client = client_addrs[ progress % CLIENTS ]
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
  else
    buffer << l
  end
end



client_addrs.each do |client|
  next_step(socket, client)
end

wait_all(socket) do |client| 
  puts "client #{client} ready for next step" if $DEBUG
end

new_entries = {}

File.open(NEW).each_line do |l|

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
  else
    buffer << l
  end

end

client_addrs.each do |client|
  add_step(socket, client)
end

wait_all(socket) do |client| 
  puts "client #{client} ready for add step" if $DEBUG
end

new_entries.each do |dn, data|
  client = client_addrs[ progress % CLIENTS ]
  progress = progress + 1
  send_data(socket, data, client)
end

client_addrs.each do |client|
  shutdown(socket, client)
end

#goobye messages
wait_all(socket) do |client| 
  puts "client #{client} said goodbye" if $DEBUG
end

socket.close
