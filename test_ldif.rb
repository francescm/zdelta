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
    assert_equal 4, diff.mods.size
    assert diff.mods.detect{|mod| ["Super DIREZIONE PIANIFICAZIONE,VALUTAZIONE E FORMAZIONE"].eql? mod.mod_vals and LDAP::LDAP_MOD_ADD.eql? mod.mod_op}
  end

  def test_add_entry
#se il old e' empty, e' una add
    old = Ldif.new("uid=malvezzi,ou=people,dc=example,dc=org", {})
    diff = old - @new
    assert diff
    assert_equal "changetype: add", diff.to_ldif.split("\n")[1]
  end

  def test_delete_entry
#se il new e' empty, e' una delete
    new = Ldif.new("uid=malvezzi,ou=people,dc=example,dc=org", {})
    diff = @old - new
    assert diff
    assert_equal 2, diff.to_ldif.split("\n").size
    assert_equal "changetype: delete", diff.to_ldif.split("\n")[1]
#puts diff.to_ldif
  end

  def test_entry_without_mod
    the_same_ldif = LDAP::LDIF.parse_file(NEW, false).first
    old = Ldif.new(the_same_ldif.dn, the_same_ldif.attrs)
    diff = old - @new
    assert diff
    assert_equal 1, diff.to_ldif.split("\n").size
    assert_match /^#/, diff.to_ldif.split("\n").first
  end

  def test_user_password_creates_a_replace
    old = Ldif.new("uid=malvezzi,ou=people,dc=example,dc=org", {"userPassword" => ["{crypt}secret"]})
    new = Ldif.new("uid=malvezzi,ou=people,dc=example,dc=org", {"userPassword" => ["{crypt}password"]})
    diff = old - new
    assert diff
    diff.mods.each do |m|
      assert_equal 2, m.mod_op
    end

  end

end
