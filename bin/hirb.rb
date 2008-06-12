# Command passed to org.jruby.Main.  Pollutes jirb with hbase imports and hbase
# commands and then loads jirb.  Outputs a banner that tells user where to find
# help, shell version, etc.

# TODO: Process command-line arguments: e.g. --master= or -Dhbase.etc and --formatter
# or read hbase shell configurations from irbrc

# TODO: Write a base class for formatters with ascii, xhtml, and json subclasses.

# Run the java magic include and import basic HBase types that will help ease
# hbase hacking.
include Java
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.io.BatchUpdate

# Some goodies for hirb. Should these be left up to the user's discretion?
require 'irb/completion'

# Add the $HBASE_HOME/bin directory, the presumed location of this script,
# to the ruby load path so I can load up my HBase ruby modules
$LOAD_PATH.unshift File.dirname($PROGRAM_NAME)
require 'Formatter'
require 'HBase'

# A HERE document used outputting shell command-line options.
@cmdline_help = <<HERE
HBase Shell command-line options:
 format   Formatter outputting results: console | html.  Default: console.
 master   HBase master shell should connect to: e.g --master=example:60000.
HERE

# See if there are args for us.  If any, read and then strip from ARGV
# so they don't go through to irb.
master = nil
@formatter = Formatter::Console.new(STDOUT)
found = []
for arg in ARGV
  if arg =~ /^--master=(.+)/i
    master = $1
    found.push(arg)
  elsif arg =~ /^--format=(.+)/i
    format = $1
    if format =~ /^html$/i
      @formatter = Formatter::XHTML.new(STDOUT)
    elsif format =~ /^console$/i
      # This is default
    else
      raise ArgumentError.new("Unsupported format " + arg)
    end
  elsif arg == '-h' || arg == '--help'
    puts @cmdline_help
    exit
  end
end
for arg in found
  ARGV.delete(arg)
end

# Setup the HBase module.  Create a configuration.  If a master, set it.
@configuration = HBaseConfiguration.new()
@configuration.set("hbase.master", master) if master
# Do lazy create of admin.  If we are pointed at bad master, will hang
# shell on startup trying to connect.
@admin = nil

# Promote all HBase constants to be constants of this module.
for c in HBase.constants
  if c == c.upcase
    eval("%s = \"%s\"" % [c, c])
  end
end 

# TODO: Add table options here.

# General Shell Commands: help and version
def help
  # Format is command name and then short description
  # TODO: Can't do 'help COMMAND'.  Interpreter runs help and then the command
  commands = {'version' => 'Output HBase version',
    'list' => 'List all tables',
    # The help string in the below is carefully formatted to wrap nicely in
    # our dumb Console formatter
    'create' => "Create table; pass a table name, a dictionary of \
specifications per   column family, and optionally, named parameters of table \
options.     Dictionaries are specified with curly-brackets, uppercase keys, a '=>'\
key/value delimiter and then a value. Named parameters are like dict- \
ionary elements with uppercase names and a '=>' delimiter.  E.g. To   \
create a table named 'table1' with an alternate maximum region size   \
and a single family named 'family1' with an alternate maximum cells:  \
create 'table1' {NAME =>'family1', MAX_NUM_VERSIONS => 5}, REGION_SIZE => 12345",
    'enable' => "Enable named table",
    'disable' => "Disable named table",
    'exists' => "Does named table exist",
    }
  @formatter.header(["HBase Shell Commands:"])
  # TODO: Add general note that all names must be quoted and a general
  # description of dictionary so create doesn't have to be so long.
  for k, v in commands.sort
    @formatter.row([k, v])
  end
  @formatter.footer()
end

def version
  @formatter.header()
  @formatter.row(["Version: #{org.apache.hadoop.hbase.util.VersionInfo.getVersion()},\
 r#{org.apache.hadoop.hbase.util.VersionInfo.getRevision()},\
 #{org.apache.hadoop.hbase.util.VersionInfo.getDate()}"])
  @formatter.footer()
end

# DDL

def create(table_name, *args)
  @admin = HBase::Admin.new(@configuration, @formatter) unless @admin
  @admin.create(table_name, args)
end

def drop(table_name)
  @admin = HBase::Admin.new(@configuration, @formatter) unless @admin
  @admin.drop(table_name)
end

def alter(table_name, *args)
  puts "Not implemented yet"
end

# Administration

def list
  @admin = HBase::Admin.new(@configuration, @formatter) unless @admin
  @admin.list()
end
  
def enable(table_name)
  @admin = HBase::Admin.new(@configuration, @formatter) unless @admin
  @admin.enable(table_name)
end

def disable(table_name)
  @admin = HBase::Admin.new(@configuration, @formatter) unless @admin
  @admin.disable(table_name)
end

def exists(table_name)
  @admin = HBase::Admin.new(@configuration, @formatter) unless @admin
  @admin.exists(table_name)
end
  
# CRUD
  
def get(table_name, row_key, *args)
  puts "Not implemented yet"
end

def put(table_name, row_key, *args)
  puts "Not implemented yet"
end
  
def scan(table_name, start_key, end_key, *args)
  puts "Not implemented yet"
end
  
def delete(table_name, row_key, *args)
  puts "Not implemented yet"
end

# Output a banner message that tells users where to go for help
# TODO: Test that we're in irb context.  For now presume it.
# TODO: Test that we are in shell context.
puts "HBase Shell; type 'hbase<RETURN>' for the list of supported HBase commands"
version

require "irb"

# IRB::ExtendCommandBundle.instance_variable_get("@EXTEND_COMMANDS").delete_if{|x| x.first == :irb_help}

module IRB
  module ExtendCommandBundle
    # These are attempts at blocking the complaint about :irb_help on startup.
    # @EXTEND_COMMANDS.delete_if{|x| x[0] == :irb_help}
    # @EXTEND_COMMANDS.each{|x| x[3][1] = OVERRIDE_ALL if x[0] == :irb_help}
    # @EXTEND_COMMANDS.each{|x| puts x if x[0] == :irb_help}
  end

  class HIRB < Irb
    # Subclass irb so can intercept methods

    def output_value
      # Suppress output if last_value is 'nil'
      # Otherwise, when user types help, get ugly 'nil'
      # after all output.
      if @context.last_value
        super
      end
    end
  end

  def IRB.start(ap_path = nil)
    $0 = File::basename(ap_path, ".rb") if ap_path

    IRB.setup(ap_path)
    @CONF[:IRB_NAME]="hbase"
    
    if @CONF[:SCRIPT]
      hirb = HIRB.new(nil, @CONF[:SCRIPT])
    else
      hirb = HIRB.new
    end

    @CONF[:IRB_RC].call(hirb.context) if @CONF[:IRB_RC]
    @CONF[:MAIN_CONTEXT] = hirb.context

    trap("SIGINT") do
      hirb.signal_handle
    end

    catch(:IRB_EXIT) do
      hirb.eval_input
    end
  end
end

IRB.start
