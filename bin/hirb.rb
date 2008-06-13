# File passed to org.jruby.Main by bin/hbase.  Pollutes jirb with hbase imports
# and hbase  commands and then loads jirb.  Outputs a banner that tells user
# where to find help, shell version, and loads up a custom hirb.

# TODO: Add 'debug' support (client-side logs show in shell).  Add it as
# command-line option and as command.
# TODO: Interrupt a table creation or a connection to a bad master.  Currently
# has to time out.
# TODO: Add support for listing and manipulating catalog tables, etc.
# TODO: Fix 'irb: warn: can't alias help from irb_help.' in banner message

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

# Add the $HBASE_HOME/bin directory, the location of this script, to the ruby
# load path so I can load up my HBase ruby modules
$LOAD_PATH.unshift File.dirname($PROGRAM_NAME)
# Require formatter and hbase
require 'Formatter'
require 'HBase'

# See if there are args for this shell. If any, read and then strip from ARGV
# so they don't go through to irb.  Output shell 'usage' if user types '--help'
cmdline_help = <<HERE # HERE document output as shell usage
HBase Shell command-line options:
 format  Formatter for outputting results: console | html. Default: console
 master  HBase master shell should connect to: e.g --master=example:60000
HERE
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
    puts cmdline_help
    exit
  end
end
for arg in found
  ARGV.delete(arg)
end

# Setup the HBase module.  Create a configuration.  If a master, set it.
@configuration = HBaseConfiguration.new()
@configuration.set("hbase.master", master) if master
# Do lazy create of admin because if we are pointed at bad master, it will hang
# shell on startup trying to connect.
@admin = nil

# Promote hbase constants to be constants of this module so can
# be used bare as keys in 'create', 'alter', etc. To see constants
# in IRB, type 'Object.constants'. Don't promote defaults because
# flattens all types to String.  Can be confusing.
def promoteConstants(constants)
  # The constants to import are all in uppercase
  for c in constants
    if c == c.upcase
      eval("%s = \"%s\"" % [c, c]) unless c =~ /DEFAULT_.*/
    end
  end
end
promoteConstants(HColumnDescriptor.constants)
promoteConstants(HTableDescriptor.constants)

# Start of the hbase shell commands.

# General shell methods

def help
  # Output help.  Help used to be a dictionary of name to short and long
  # descriptions emitted using Formatters but awkward getting it to show
  # nicely on console; instead use a HERE document.  Means we can't
  # output help other than on console but not an issue at the moment.
  # TODO: Add help to the commands themselves rather than keep it distinct
  h  = <<HERE
HBASE SHELL COMMANDS:
 alter     Alter column family schema in a table.  Pass table name and a
           dictionary specifying the new column family schema. Dictionaries
           are described below in the GENERAL NOTES section.  Dictionary must
           include name of column family to alter.  For example, to change
           the 'f1' column family in table 't1' to have a MAX_VERSIONS of 5,
           do:

           hbase> alter 't1', {NAME => 'f1', MAX_VERSIONS => 5}

 create    Create table; pass a table name, a dictionary of specifications per
           column family, and optionally, named parameters of table options.
           Dictionaries are described below in the GENERAL NOTES section. Named
           parameters are like dictionary elements with uppercase names
           (constants) as keys and a '=>' key/value delimiter.  Parameters are
           comma-delimited.  For example, to create a table named 't1' with an
           alternate maximum region size and a single family named 'f1' with an
           alternate maximum number of cells and 'record' compression, type:

           hbase> create 't1' {NAME => 'f1', MAX_VERSIONS => 5, \
               COMPRESSION => 'RECORD'}, REGION_SIZE => 1024

           For compression types, pass one of 'NONE', 'RECORD', or 'BLOCK'

 describe  Describe the named table. Outputs the table and family descriptors
 drop      Drop the named table.  Table must first be disabled
 disable   Disable the named table: e.g. "disable 't1'<RETURN>"
 enable    Enable the named table
 exists    Does the named table exist? e.g. "exists 't1'<RETURN>"
 exit      Exit the shell
 list      List all tables
 version   Output this HBase version

GENERAL NOTES:
Quote all names in the hbase shell such as table and column names.  Don't
forget commas delimiting command parameters. Dictionaries of configuration used
in the creation and alteration of tables are ruby-style Hashes. They look like
this: { 'key1' => 'value1', 'key2' => 'value2', ...}.  They are opened and
closed with curley-braces.  Key/values are delimited by the '=>' character
combination.  Usually keys are predefined constants such as NAME, MAX_VERSIONS,
COMPRESSION, MAX_LENGTH, TTL, etc.  Constants do not need to be quoted.  Type
'Object.constants' to see a (messy) list of all constants in the environment.
See http://wiki.apache.org/hadoop/Hbase/Shell for more on the HBase Shell.
HERE
  puts h
end

def version
  # Output version.
  puts "Version: #{org.apache.hadoop.hbase.util.VersionInfo.getVersion()},\
 r#{org.apache.hadoop.hbase.util.VersionInfo.getRevision()},\
 #{org.apache.hadoop.hbase.util.VersionInfo.getDate()}"
end

# DDL

def admin()
  @admin = HBase::Admin.new(@configuration, @formatter) unless @admin
  @admin
end

def create(table_name, *args)
  admin().create(table_name, args)
end

def drop(table_name)
  admin().drop(table_name)
end

def alter(table_name, args)
  admin().alter(table_name, args) 
end

# Administration

def list
  admin().list()
end

def describe(table_name)
  admin().describe(table_name)
end
  
def enable(table_name)
  admin().enable(table_name)
end

def disable(table_name)
  admin().disable(table_name)
end

def exists(table_name)
  admin().exists(table_name)
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
puts <<HERE
HBase Shell; enter 'help<RETURN>' for list of supported commands.
HERE
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
