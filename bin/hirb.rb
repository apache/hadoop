# Some goodies for hirb
require 'irb/completion'

# Run the java magic include and import basic HBase types.
include Java
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.io.BatchUpdate

# Set the irb shell name to be hbase.
IRB.conf[:IRB_NAME] = "hbase"

# TODO: Read from environment which outputter to use (outputter should
# be able to output to a passed Stream as well as STDIN and STDOUT)


def help
    puts 'HBase Commands:'
    puts ' version   Output the hbase version'
end

def version
    puts "HBase Version: #{org.apache.hadoop.hbase.util.VersionInfo.getVersion()}"
    puts "SVN Revision: #{org.apache.hadoop.hbase.util.VersionInfo.getRevision()}"
    puts "HBase Compiled: #{org.apache.hadoop.hbase.util.VersionInfo.getDate()}"
end
