# Command passed to org.jruby.Main.  Pollutes jirb with hbase imports and hbase
# commands and then loads jirb.  Outputs a banner that tells user where to find
# help, shell version, etc.

# TODO: Process command-line arguments: e.g. --master= or -Dhbase.etc and --formatter
# or read hbase shell configurations from irbrc
# TODO: Read from environment which outputter to use (outputter should
# be able to output to a passed Stream as well as STDIN and STDOUT)
# TODO: Write a base class for formatters with ascii, xhtml, and json subclasses.

# Run the java magic include and import basic HBase types.
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

def help
  puts 'HBase Shell Commands:'
  puts ' version   Output HBase version'
  puts ARGV.inspect
end

def version
  "Version: #{org.apache.hadoop.hbase.util.VersionInfo.getVersion()},\
 r#{org.apache.hadoop.hbase.util.VersionInfo.getRevision()},\
 #{org.apache.hadoop.hbase.util.VersionInfo.getDate()}"
end

# general

def list
  puts "Not implemented yet"
end

# DDL

def create(table_name, *args)
  puts "Not impemented yet"
end

def drop(table_name)
  puts "Not implemented yet"
end

def alter(table_name, *args)
  puts "Not implemented yet"
end

# admin
  
def enable(table_name)
  puts "Not implemented yet"
end

def disable(table_name)
  puts "Not implemented yet"
end
  
def truncate(table_name)
  puts "Not implemented yet"
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
puts version

require "irb"

IRB::ExtendCommandBundle.instance_variable_get("@EXTEND_COMMANDS").delete_if{|x| x.first == :irb_help}

module IRB
  module ExtendCommandBundle
    
  end
  
  def IRB.start(ap_path = nil)
    $0 = File::basename(ap_path, ".rb") if ap_path

    IRB.setup(ap_path)
    
    @CONF[:PROMPT][:HBASE] = {
      :PROMPT_I => "hbase> ",
    	:PROMPT_N => "hbase> ",
    	:PROMPT_S => nil,
    	:PROMPT_C => "?> ",
    	:RETURN => "%s\n"
    }
    @CONF[:PROMPT_MODE] = :HBASE
    
    
    if @CONF[:SCRIPT]
      irb = Irb.new(nil, @CONF[:SCRIPT])
    else
      irb = Irb.new
    end

    @CONF[:IRB_RC].call(irb.context) if @CONF[:IRB_RC]
    @CONF[:MAIN_CONTEXT] = irb.context

    trap("SIGINT") do
      irb.signal_handle
    end

    catch(:IRB_EXIT) do
      irb.eval_input
    end
  end
end


# .delete_if{|x| x.first == :irb_help}.inspect

IRB.start
