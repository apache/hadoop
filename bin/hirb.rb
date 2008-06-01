# Module passed to jirb using the '-r' flag when bin/hbase shell is invoked.
# Pollutes jirb with hbase imports and hbase commands.  Outputs a banner
# that tells user where to find help, shell version, etc.

# TODO: Process command-line arguments: e.g. --master= or -Dhbase.etc and --formatter
# or read hbase shell configurations from irbrc
# TODO: Read from environment which outputter to use (outputter should
# be able to output to a passed Stream as well as STDIN and STDOUT)
# TODO: Write a base class for formatters with ascii, xhtml, and json subclasses.
# TODO: Intercept 'help'

# Run the java magic include and import basic HBase types.
include Java
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.io.BatchUpdate

# Some goodies for hirb. Should these be left up to the user's discretion?
require 'irb/completion'

# Set the irb shell name to be hbase.
IRB.conf[:IRB_NAME] = "hbase"

def hbase
  puts 'HBase Shell Commands:'
  puts ' version   Output HBase version'
end

def versionstr
  "Version: #{org.apache.hadoop.hbase.util.VersionInfo.getVersion()},\
 r#{org.apache.hadoop.hbase.util.VersionInfo.getRevision()},\
 #{org.apache.hadoop.hbase.util.VersionInfo.getDate()}"
end 

def version
  puts versionstr()
end

# Output a banner message that tells users where to go for help
# TODO: Test that we're in irb context.  For now presume it.
puts "HBase Shell; " + versionstr()
puts "+ Type 'hbase<RETURN>' for list of HBase commands"
