#encoding: utf-8

require 'rubygems'
gem "test-unit"
require 'test/unit'
require "ldif_diff"

class LdifDiffTester < Test::Unit::TestCase
  def setup
    @add1 = LDAP::Mod.new(LDAP::LDAP_MOD_ADD, "alpha", %w{uno due})
    @add2 = LDAP::Mod.new(LDAP::LDAP_MOD_ADD, "beta" , %w{tre})
    @delete1 = LDAP::Mod.new(LDAP::LDAP_MOD_DELETE, "gamma", %w{quattro cinque sei})
    @delete2 = LDAP::Mod.new(LDAP::LDAP_MOD_DELETE, "delta", %w{sette})
  end

  def test_initialize_and_does_not_end_with_dash
    ldif_diff = LdifDiff.new("uid=test", "modify", [@add1, @add2])
    assert ldif_diff    
    assert_not_equal "-", ldif_diff.to_ldif.split("\n").last
    assert_match "alpha", ldif_diff.to_ldif
    assert_match "beta", ldif_diff.to_ldif
#    puts ldif_diff.to_ldif
  end

  def test_create_entry
    ldif_diff = LdifDiff.new("uid=test", "add", [@add1, @add2])
    assert ldif_diff    
    assert_not_match /-/, ldif_diff.to_ldif
    assert_not_match /^add/, ldif_diff.to_ldif
    assert_match "alpha", ldif_diff.to_ldif
    assert_match "beta", ldif_diff.to_ldif
#    puts ldif_diff.to_ldif
  end

end
