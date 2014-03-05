#encoding: utf-8

require 'rubygems'
require 'ldap'
require 'ldap/ldif'
require 'ldif'
require 'ffi-rzmq'
require 'json'
require 'yaml'

config = YAML.load_file("config.yaml")
output_file = config[:output_file]
OLD = config[:old]
NEW = config[:new]


FP = File.open(output_file, "w+")

progress = 0
start_time = Time.new
incremental = nil
buffer = []

memory = {} #holds the map dn -> buffer

def print(data)
  FP.puts data
  FP.puts
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


File.open(OLD).each_line do |l|
  if "\n".eql? l
    dn = get_dn(buffer)
    memory[dn] = Marshal.load(Marshal.dump(buffer))
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

new_entries = {}


File.open(NEW).each_line do |l|

  if "\n".eql? l
    dn = get_dn(buffer)

    if memory[dn] 

      res = calc_diff(:mod, buffer, memory[dn])
      memory.delete res[:dn]
      print res[:diff]

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


new_entries.each do |dn, data|
  progress = progress + 1
  res = calc_diff(:add, data)
  print res[:diff]
end



memory.each do |dn, data|
  res = calc_diff(:del, data)
  print res[:diff]
end


FP.close

#puts "new_entries: #{new_entries.keys.join(", ")}"
