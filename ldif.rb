#encoding: utf-8

require 'ldap'
require 'ldap/ldif'
require 'ldif_diff'

class Ldif
  attr_reader :dn, :attrs
  def initialize(dn, attrs)
    @dn = dn
    @attrs = attrs
  end

  def -(other)
    raise RuntimeError, "dn diversi: #{@dn}, #{other.dn}" unless other.dn.eql? @dn
    if @attrs.empty?
      #e' la add di una entry
      res = []
      other.attrs.keys.each do |attr|
        res << LDAP::Mod.new(LDAP::LDAP_MOD_ADD, attr, other.attrs[attr])
      end
      LdifDiff.new(@dn, "add", res)

    elsif other.attrs.empty?
      #e' una delete della entry
      LdifDiff.new(@dn, "delete", {})

    else
      to_add = other.attrs.keys - @attrs.keys
      to_delete = @attrs.keys - other.attrs.keys
      commons = @attrs.keys & other.attrs.keys
    
      res = []

      to_add.each do |attr|
        if attr.match /userpassword/i
          res << LDAP::Mod.new(LDAP::LDAP_MOD_REPLACE, attr, other.attrs[attr])
        else
          res << LDAP::Mod.new(LDAP::LDAP_MOD_ADD, attr, other.attrs[attr])
        end
      end

      to_delete.each do |attr|
        res << LDAP::Mod.new(LDAP::LDAP_MOD_DELETE, attr, @attrs[attr])
      end

      commons.each do |attr|
        values_to_add = other.attrs[attr] - @attrs[attr]
        values_to_delete = @attrs[attr] - other.attrs[attr]
        if attr.match /userpassword/i
          unless values_to_add.empty?
            res << LDAP::Mod.new(LDAP::LDAP_MOD_REPLACE, attr, values_to_add)
          end
        else
          unless values_to_add.empty?
            res << LDAP::Mod.new(LDAP::LDAP_MOD_ADD, attr, values_to_add)
          end
          unless values_to_delete.empty?
            res << LDAP::Mod.new(LDAP::LDAP_MOD_DELETE, attr, values_to_delete)
          end
        end

      end
      LdifDiff.new(@dn, "modify", res)
    end
  end

end

