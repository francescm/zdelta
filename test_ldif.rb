#encoding: utf-8

require 'rubygems'
gem "test-unit"
require 'test/unit'
require "ldif"

OLD = "user.ldif"
NEW = "new.ldif"

class LdifTester < Test::Unit::TestCase
  def setup
    
    new_ldif = LDAP::LDIF.parse_file(NEW, false).first
    @new = Ldif.new(new_ldif.dn, new_ldif.attrs)

    old_ldif = LDAP::LDIF.parse_file(OLD, false).first
    @old = Ldif.new(old_ldif.dn, old_ldif.attrs)
  end
  
  def test_different_dn
    fake = Ldif.new("uid=fake", {"dn" => ["fake"]})
    assert_raise RuntimeError do
      @new - fake
    end
  end

  def test_diff
    diff = @old - @new
    assert diff
    assert_equal 4, diff.size
    assert diff.detect{|mod| ["Super DIREZIONE PIANIFICAZIONE,VALUTAZIONE E FORMAZIONE"].eql? mod.mod_vals and LDAP::LDAP_MOD_ADD.eql? mod.mod_op}
  end

  def test_diff_new_empty    
#se il new e' empty, devo fare un delete
    new = Ldif.new("uid=malvezzi,ou=people,dc=unimore,dc=it", {})
    diff = @old - new
    assert diff
#    puts diff.map{|attr| attr.to_ldif(@old.dn)}.join("-\n")
    
  end
end
