#encoding: utf-8

require 'ldap'
require 'ldap/ldif'

#contiene i diff dal raffronto di un dn solo

class LdifDiff

  IS_LAST = true
  NOT_LAST = false

  attr_reader :mods, :type, :dn
  def initialize(dn, type, mods)
    @dn = dn
    @type = type
    @mods = mods
  end

  def to_ldif
    if @mods.empty? and not @type.eql? "delete"
      return "# #{@dn}"
    end

    res = "dn: #{@dn}\n"
    res += "changetype: #{@type}\n"
    return res if "delete".eql? @type

    last_mod = @mods.last
    mods = @mods[0..-2].map do |mod| 
      [mod, NOT_LAST]
    end
    mods << [last_mod, IS_LAST]

    mods.each do |mod_arr|
      mod = mod_arr.first
      is_last = mod_arr.last
      if mod.mod_op.eql? LDAP::LDAP_MOD_ADD
        res += print_chunk("add", mod, is_last)
      elsif mod.mod_op.eql? LDAP::LDAP_MOD_DELETE
        res += print_chunk("delete", mod, is_last)
      elsif mod.mod_op.eql? LDAP::LDAP_MOD_REPLACE
        res += print_chunk("replace", mod, is_last)
      else raise RuntimeError, "type sconosciuto: #{mod.mod_op}"
      end
    end
    res
  end

  private
  def print_chunk(action, mod, is_last)
    chunk = ""
    chunk += "#{action}: #{mod.mod_type}\n" unless "add".eql? @type
    mod.mod_vals.each do |value|
      chunk += "#{mod.mod_type}: #{value}\n"
    end
    chunk += "-\n" unless is_last or "add".eql? @type
    chunk
  end
end
