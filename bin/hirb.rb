#
# Copyright 2009 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
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

# Add the $HBASE_HOME/lib/ruby OR $HBASE_HOME/src/main/ruby/lib directory
# to the ruby load path so I can load up my HBase ruby modules
if File.exists?(File.join(File.dirname(__FILE__), "..", "lib", "ruby", "hbase.rb"))
  $LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib", "ruby")
else
  $LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "src", "main", "ruby")
end

#
# FIXME: Switch args processing to getopt
#
# See if there are args for this shell. If any, read and then strip from ARGV
# so they don't go through to irb.  Output shell 'usage' if user types '--help'
cmdline_help = <<HERE # HERE document output as shell usage
HBase Shell command-line options:
 format        Formatter for outputting results: console | html. Default: console
 -d | --debug  Set DEBUG log levels.
HERE
found = []
format = 'console'
script2run = nil
log_level = org.apache.log4j.Level::ERROR
for arg in ARGV
  if arg =~ /^--format=(.+)/i
    format = $1
    if format =~ /^html$/i
      raise NoMethodError.new("Not yet implemented")
    elsif format =~ /^console$/i
      # This is default
    else
      raise ArgumentError.new("Unsupported format " + arg)
    end
    found.push(arg)
  elsif arg == '-h' || arg == '--help'
    puts cmdline_help
    exit
  elsif arg == '-d' || arg == '--debug'
    log_level = org.apache.log4j.Level::DEBUG
    $fullBackTrace = true
    puts "Setting DEBUG log level..."
  else
    # Presume it a script. Save it off for running later below
    # after we've set up some environment.
    script2run = arg
    found.push(arg)
    # Presume that any other args are meant for the script.
    break
  end
end

# Delete all processed args
found.each { |arg| ARGV.delete(arg) }

# Set logging level to avoid verboseness
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level)

# Require HBase now after setting log levels
require 'hbase'

# Load hbase shell
require 'shell'

# Require formatter
require 'shell/formatter'

# Presume console format.
# Formatter takes an :output_stream parameter, if you don't want STDOUT.
@formatter = Shell::Formatter::Console.new

# Setup the HBase module.  Create a configuration.
@hbase = Hbase::Hbase.new

# Setup console
@shell = Shell::Shell.new(@hbase, @formatter)

# Add commands to this namespace
@shell.export_commands(self)

# Add help command
def help(command = nil)
  @shell.help(command)
end

# Backwards compatibility method
def tools
  @shell.help_group('tools')
end

# Debugging method
def debug
  if @shell.debug
    @shell.debug = false
    conf.back_trace_limit = 0
  else
    @shell.debug = true
    conf.back_trace_limit = 100
  end
  debug?
end

def debug?
  puts "Debug mode is #{@shell.debug ? 'ON' : 'OFF'}\n\n"
  nil
end

# Include hbase constants
include HBaseConstants

# If script2run, try running it.  Will go on to run the shell unless
# script calls 'exit' or 'exit 0' or 'exit errcode'.
load(script2run) if script2run

# Output a banner message that tells users where to go for help
@shell.print_banner

require "irb"
require 'irb/hirb'

module IRB
  def self.start(ap_path = nil)
    $0 = File::basename(ap_path, ".rb") if ap_path

    IRB.setup(ap_path)
    @CONF[:IRB_NAME] = 'hbase'
    @CONF[:AP_NAME] = 'hbase'
    @CONF[:BACK_TRACE_LIMIT] = 0 unless $fullBackTrace

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
