#encoding: utf-8
#time ruby -J-Xmx2048m -I . loader.rb

require 'rubygems'
require "ldif"


BULK = "bulk.ldif"
bulk = []
progress = 0
start_time = Time.new
incremental = nil
LDAP::LDIF.parse_file(BULK, false) do |ldif|
  bulk << ldif
  STDOUT.write "\r#{progress}"
  progress = progress + 1
  if (progress % 1000) == 0
    puts "\r#{progress}"
    new_time = Time.new
    puts "total time : #{new_time - start_time}"
    puts "incremental time : #{new_time - incremental}" if incremental
    incremental = new_time
  end
end
puts "caricato in memoria!"
io = bulk.detect{|ldif| ldif.dn.eql? "uid=malvezzi,ou=people,dc=unimore,dc=it"}

p io
