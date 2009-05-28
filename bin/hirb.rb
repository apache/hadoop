# File passed to org.jruby.Main by bin/hbase.  Pollutes jirb with hbase imports
# and hbase  commands and then loads jirb.  Outputs a banner that tells user
# where to find help, shell version, and loads up a custom hirb.

# TODO: Add 'debug' support (client-side logs show in shell).  Add it as
# command-line option and as command.
# TODO: Interrupt a table creation or a connection to a bad master.  Currently
# has to time out.  Below we've set down the retries for rpc and hbase but
# still can be annoying (And there seem to be times when we'll retry for
# ever regardless)
# TODO: Add support for listing and manipulating catalog tables, etc.
# TODO: Encoding; need to know how to go from ruby String to UTF-8 bytes

# Run the java magic include and import basic HBase types that will help ease
# hbase hacking.
include Java

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
 format        Formatter for outputting results: console | html. Default: console
 format-width  Width of table outputs. Default: 110 characters.
 master        HBase master shell should connect to: e.g --master=example:60000
HERE
master = nil
found = []
format = 'console'
format_width = 110
for arg in ARGV
  if arg =~ /^--master=(.+)/i
    master = $1
    found.push(arg)
  elsif arg =~ /^--format=(.+)/i
    format = $1
    if format =~ /^html$/i
      raise NoMethodError.new("Not yet implemented")
    elsif format =~ /^console$/i
      # This is default
    else
      raise ArgumentError.new("Unsupported format " + arg)
    end
    found.push(arg)
  elsif arg =~ /^--format-width=(.+)/i
    format_width = $1.to_i
    found.push(arg)
  elsif arg == '-h' || arg == '--help'
    puts cmdline_help
    exit
  else
    # Presume it a script and try running it.  Will go on to run the shell unless
    # script calls 'exit' or 'exit 0' or 'exit errcode'.
    load(arg)
  end
end
for arg in found
  ARGV.delete(arg)
end
# Presume console format.
@formatter = Formatter::Console.new(STDOUT, format_width)
# TODO, etc.  @formatter = Formatter::XHTML.new(STDOUT)

# Setup the HBase module.  Create a configuration.  If a master, set it.
# Turn off retries in hbase and ipc.  Human doesn't want to wait on N retries.
@configuration = org.apache.hadoop.hbase.HBaseConfiguration.new()
@configuration.set("hbase.master", master) if master
@configuration.setInt("hbase.client.retries.number", 5)
@configuration.setInt("ipc.client.connect.max.retries", 3)

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
promoteConstants(org.apache.hadoop.hbase.HColumnDescriptor.constants)
promoteConstants(org.apache.hadoop.hbase.HTableDescriptor.constants)
promoteConstants(HBase.constants)

# Start of the hbase shell commands.

# General shell methods

def tools
  # Help for hbase shell surgery tools
  h  = <<HERE
HBASE SURGERY TOOLS:
 close_region    Close a single region. Optionally specify regionserver.
                 Examples:
                 
                 hbase> close_region 'REGIONNAME'
                 hbase> close_region 'REGIONNAME', 'REGIONSERVER_IP:PORT'

 compact         Compact all regions in passed table or pass a region row
                 to compact an individual region

 disable_region  Disable a single region

 enable_region   Enable a single region. For example:
  
                 hbase> enable_region 'REGIONNAME'
 
 flush           Flush all regions in passed table or pass a region row to
                 flush an individual region.  For example:

                 hbase> flush 'TABLENAME'
                 hbase> flush 'REGIONNAME'

 major_compact   Run major compaction on passed table or pass a region row
                 to major compact an individual region

 split           Split table or pass a region row to split individual region

Above commands are for 'experts'-only as misuse can damage an install
HERE
  puts h
end

def help
  # Output help.  Help used to be a dictionary of name to short and long
  # descriptions emitted using Formatters but awkward getting it to show
  # nicely on console; instead use a HERE document.  Means we can't
  # output help other than on console but not an issue at the moment.
  # TODO: Add help to the commands themselves rather than keep it distinct
  h  = <<HERE
HBASE SHELL COMMANDS:
 alter     Alter column family schema;  pass table name and a dictionary
           specifying new column family schema. Dictionaries are described
           below in the GENERAL NOTES section.  Dictionary must include name
           of column family to alter.  For example, 
           
           To change or add the 'f1' column family in table 't1' from defaults
           to instead keep a maximum of 5 cell VERSIONS, do:
           hbase> alter 't1', {NAME => 'f1', VERSIONS => 5}
           
           To delete the 'f1' column family in table 't1', do:
           hbase> alter 't1', {NAME => 'f1', METHOD => 'delete'}

           You can also change table-scope attributes like MAX_FILESIZE
           MEMCACHE_FLUSHSIZE and READONLY.

           For example, to change the max size of a family to 128MB, do:
           hbase> alter 't1', {METHOD => 'table_att', MAX_FILESIZE => '134217728'}
           
 count     Count the number of rows in a table. This operation may take a LONG
           time (Run '$HADOOP_HOME/bin/hadoop jar hbase.jar rowcount' to run a
           counting mapreduce job). Current count is shown every 1000 rows by
           default. Count interval may be optionally specified. Examples:
           
           hbase> count 't1'
           hbase> count 't1', 100000

 create    Create table; pass table name, a dictionary of specifications per
           column family, and optionally a dictionary of table configuration.
           Dictionaries are described below in the GENERAL NOTES section.
           Examples:

           hbase> create 't1', {NAME => 'f1', VERSIONS => 5}
           hbase> create 't1', {NAME => 'f1'}, {NAME => 'f2'}, {NAME => 'f3'}
           hbase> # The above in shorthand would be the following:
           hbase> create 't1', 'f1', 'f2', 'f3'
           hbase> create 't1', {NAME => 'f1', VERSIONS => 1, TTL => 2592000, \\
             BLOCKCACHE => true}

 describe  Describe the named table: e.g. "hbase> describe 't1'"

 delete    Put a delete cell value at specified table/row/column and optionally
           timestamp coordinates.  Deletes must match the deleted cell's
           coordinates exactly.  When scanning, a delete cell suppresses older
           versions. Takes arguments like the 'put' command described below
 
 deleteall Delete all cells in a given row; pass a table name, row, and optionally 
           a column and timestamp

 disable   Disable the named table: e.g. "hbase> disable 't1'"
 
 drop      Drop the named table. Table must first be disabled. If table has
           more than one region, run a major compaction on .META.:

           hbase> major_compact ".META."

 enable    Enable the named table

 exists    Does the named table exist? e.g. "hbase> exists 't1'"

 exit      Type "hbase> exit" to leave the HBase Shell

 get       Get row or cell contents; pass table name, row, and optionally
           a dictionary of column(s), timestamp and versions.  Examples:

           hbase> get 't1', 'r1'
           hbase> get 't1', 'r1', {COLUMN => 'c1'}
           hbase> get 't1', 'r1', {COLUMN => ['c1', 'c2', 'c3']}
           hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1}
           hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1, \\
             VERSIONS => 4}

 list      List all tables in hbase

 put       Put a cell 'value' at specified table/row/column and optionally
           timestamp coordinates.  To put a cell value into table 't1' at
           row 'r1' under column 'c1' marked with the time 'ts1', do:

           hbase> put 't1', 'r1', 'c1', 'value', ts1

 tools     Listing of hbase surgery tools

 scan      Scan a table; pass table name and optionally a dictionary of scanner 
           specifications.  Scanner specifications may include one or more of 
           the following: LIMIT, STARTROW, STOPROW, TIMESTAMP, or COLUMNS.  If 
           no columns are specified, all columns will be scanned.  To scan all 
           members of a column family, leave the qualifier empty as in 
           'col_family:'.  Examples:
           
           hbase> scan '.META.'
           hbase> scan '.META.', {COLUMNS => 'info:regioninfo'}
           hbase> scan 't1', {COLUMNS => ['c1', 'c2'], LIMIT => 10, \\
             STARTROW => 'xyz'}

 status    Show cluster status. Can be 'summary', 'simple', or 'detailed'. The
           default is 'summary'. Examples:
           
           hbase> status
           hbase> status 'simple'
           hbase> status 'summary'
           hbase> status 'detailed'

 shutdown  Shut down the cluster.

 truncate  Disables, drops and recreates the specified table.
           
 version   Output this HBase version

GENERAL NOTES:
Quote all names in the hbase shell such as table and column names.  Don't
forget commas delimit command parameters.  Type <RETURN> after entering a
command to run it.  Dictionaries of configuration used in the creation and
alteration of tables are ruby Hashes. They look like this:

  {'key1' => 'value1', 'key2' => 'value2', ...}

They are opened and closed with curley-braces.  Key/values are delimited by
the '=>' character combination.  Usually keys are predefined constants such as
NAME, VERSIONS, COMPRESSION, etc.  Constants do not need to be quoted.  Type
'Object.constants' to see a (messy) list of all constants in the environment.

This HBase shell is the JRuby IRB with the above HBase-specific commands added.
For more on the HBase Shell, see http://wiki.apache.org/hadoop/Hbase/Shell
HERE
  puts h
end

def version
  # Output version.
  puts "Version: #{org.apache.hadoop.hbase.util.VersionInfo.getVersion()},\
 r#{org.apache.hadoop.hbase.util.VersionInfo.getRevision()},\
 #{org.apache.hadoop.hbase.util.VersionInfo.getDate()}"
end

def shutdown
  admin().shutdown()
end 

# DDL

def admin()
  @admin = HBase::Admin.new(@configuration, @formatter) unless @admin
  @admin
end

def table(table)
  # Create new one each time
  HBase::Table.new(@configuration, table, @formatter)
end

def create(table, *args)
  admin().create(table, args)
end

def drop(table)
  admin().drop(table)
end

def alter(table, args)
  admin().alter(table, args) 
end

# Administration

def list
  admin().list()
end

def describe(table)
  admin().describe(table)
end
  
def enable(table)
  admin().enable(table)
end

def disable(table)
  admin().disable(table)
end

def enable_region(regionName)
  admin().enable_region(regionName)
end

def disable_region(regionName)
  admin().disable_region(regionName)
end

def exists(table)
  admin().exists(table)
end

def truncate(table)
  admin().truncate(table)
end

def close_region(regionName, server = nil)
  admin().close_region(regionName, server)
end

def status(format = 'summary')
  admin().status(format)
end

# CRUD
  
def get(table, row, args = {})
  table(table).get(row, args)
end

def put(table, row, column, value, timestamp = nil)
  table(table).put(row, column, value, timestamp)
end
  
def scan(table, args = {})
  table(table).scan(args)
end
  
def delete(table, row, column,
    timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP)
  table(table).delete(row, column, timestamp)
end

def deleteall(table, row, column = nil,
    timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP)
  table(table).deleteall(row, column, timestamp)
end

def count(table, interval = 1000)
  table(table).count(interval)
end

def flush(tableNameOrRegionName)
  admin().flush(tableNameOrRegionName)
end

def compact(tableNameOrRegionName)
  admin().compact(tableNameOrRegionName)
end

def major_compact(tableNameOrRegionName)
  admin().major_compact(tableNameOrRegionName)
end

def split(tableNameOrRegionName)
  admin().split(tableNameOrRegionName)
end

# Output a banner message that tells users where to go for help
puts <<HERE
HBase Shell; enter 'help<RETURN>' for list of supported commands.
HERE
version

require "irb"

module IRB
  # Subclass of IRB so can intercept methods
  class HIRB < Irb
    def initialize
      # This is ugly.  Our 'help' method above provokes the following message
      # on irb construction: 'irb: warn: can't alias help from irb_help.'
      # Below, we reset the output so its pointed at /dev/null during irb
      # construction just so this message does not come out after we emit
      # the banner.  Other attempts at playing with the hash of methods
      # down in IRB didn't seem to work. I think the worst thing that can
      # happen is the shell exiting because of failed IRB construction with
      # no error (though we're not blanking STDERR)
      begin
        f = File.open("/dev/null", "w")
        $stdout = f
        super
      ensure
        f.close()
        $stdout = STDOUT
      end
    end 
    
    def output_value
      # Suppress output if last_value is 'nil'
      # Otherwise, when user types help, get ugly 'nil'
      # after all output.
      if @context.last_value != nil
        super
      end
    end
  end

  def IRB.start(ap_path = nil)
    $0 = File::basename(ap_path, ".rb") if ap_path

    IRB.setup(ap_path)
    @CONF[:IRB_NAME] = 'hbase'
    @CONF[:AP_NAME] = 'hbase'
    
    if @CONF[:SCRIPT]
      hirb = HIRB.new(nil, @CONF[:SCRIPT])
    else
      hirb = HIRB.new
    end

    @CONF[:IRB_RC].call(hirb.context) if @CONF[:IRB_RC]
    @CONF[:MAIN_CONTEXT] = hirb.context

    catch(:IRB_EXIT) do
      hirb.eval_input
    end
  end
end

IRB.start
