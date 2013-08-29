#encoding: utf-8

require 'base64'

class Parser
  def self.parse(lines)
    memory = []
    entries = []
    entry = {}

    lines.each do |line|
      line.strip!
      if line =~ /^#/ 
          next
      end
      if ( line.match /\w+::? \w+/ or line.eql? "" ) and not memory.empty?
        attr = memory.first.split(":").first
        base64 = memory.first.match(/:: /) ? true : false
        value = memory.first.split(":").last
        memory[1..-1].each do |increment|
          value += increment
        end if memory.size > 1
        value = Base64.decode64(value) if base64
        entry[attr] ||= []
        entry[attr] << value
        memory.clear
        memory << line
      elsif
        memory << line
      end
      if line.eql? ""
#toglie le chiavi a nil
        entry.delete_if {|k, v| ! k}
        entries << entry.clone
        entry.clear
      end
    end
    
    entries
  end

end
