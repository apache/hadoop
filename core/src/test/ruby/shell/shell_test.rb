require 'hbase'
require 'shell'
require 'shell/formatter'

class ShellTest < Test::Unit::TestCase
  def setup
    @formatter = ::Shell::Formatter::Console.new(:format_width => 110)
    @hbase = ::Hbase::Hbase.new
    @shell = Shell::Shell.new(@hbase, @formatter)
  end

  define_test "Shell::Shell#hbase_admin should return an admin instance" do
    assert_kind_of(Hbase::Admin, @shell.hbase_admin)
  end

  define_test "Shell::Shell#hbase_admin should cache admin instances" do
    assert_same(@shell.hbase_admin, @shell.hbase_admin)
  end

  #-------------------------------------------------------------------------------

  define_test "Shell::Shell#hbase_table should return a table instance" do
    assert_kind_of(Hbase::Table, @shell.hbase_table('.META.'))
  end

  define_test "Shell::Shell#hbase_table should not cache table instances" do
    assert_not_same(@shell.hbase_table('.META.'), @shell.hbase_table('.META.'))
  end

  #-------------------------------------------------------------------------------

  define_test "Shell::Shell#export_commands should export command methods to specified object" do
    module Foo; end
    assert(!Foo.respond_to?(:version))
    @shell.export_commands(Foo)
    assert(Foo.respond_to?(:version))
  end

  #-------------------------------------------------------------------------------

  define_test "Shell::Shell#command_instance should return a command class" do
    assert_kind_of(Shell::Commands::Command, @shell.command_instance('version'))
  end

  #-------------------------------------------------------------------------------

  define_test "Shell::Shell#command should execute a command" do
    @shell.command('version')
  end
end
