# HBase ruby classes.
# Has wrapper classes for org.apache.hadoop.hbase.client.HBaseAdmin
# and for org.apache.hadoop.hbase.client.HTable.  Classes take
# Formatters on construction and outputs any results using
# Formatter methods.  These classes are only really for use by
# the hirb.rb HBase Shell script; they don't make much sense elsewhere.
# For example, the exists method on Admin class prints to the formatter
# whether the table exists and returns nil regardless.
include Java

java_import org.apache.hadoop.hbase.HConstants
java_import org.apache.hadoop.hbase.HColumnDescriptor
java_import org.apache.hadoop.hbase.HTableDescriptor

include_class('java.lang.Integer') {|package,name| "J#{name}" }
include_class('java.lang.Long') {|package,name| "J#{name}" }
include_class('java.lang.Boolean') {|package,name| "J#{name}" }

module HBaseConstants
  COLUMN = "COLUMN"
  COLUMNS = "COLUMNS"
  TIMESTAMP = "TIMESTAMP"
  NAME = HConstants::NAME
  VERSIONS = HConstants::VERSIONS
  IN_MEMORY = HConstants::IN_MEMORY
  STOPROW = "STOPROW"
  STARTROW = "STARTROW"
  ENDROW = STOPROW
  LIMIT = "LIMIT"
  METHOD = "METHOD"
  MAXLENGTH = "MAXLENGTH"
  CACHE_BLOCKS = "CACHE_BLOCKS"
  REPLICATION_SCOPE = "REPLICATION_SCOPE"

  # Load constants from hbase java API
  def self.promote_constants(constants)
    # The constants to import are all in uppercase
    constants.each do |c|
      next if c =~ /DEFAULT_.*/ || c != c.upcase
      next if eval("defined?(#{c})")
      eval("#{c} = '#{c}'")
    end
  end

  promote_constants(HColumnDescriptor.constants)
  promote_constants(HTableDescriptor.constants)
end

# Include classes definition
require 'hbase/hbase'
require 'hbase/admin'
require 'hbase/table'
