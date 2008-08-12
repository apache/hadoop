import sys, os
from skytools.config import *

PSQL_SCHEMA = "psql_schema"
PSQL_TABLENAME = "psql_table_name"
PSQL_KEYCOL = "psql_key_column"
PSQL_COLUMNS = "psql_columns"
HBASE_TABLENAME = "hbase_table_name"
HBASE_COLUMNDESCS = "hbase_column_descriptors"
HBASE_ROWPREFIX = "hbase_row_prefix"

def load_table_mappings(config_file, table_names):
  table_mappings = {}
  for table_name in table_names:
    conf = Config(table_name, config_file)
    table_mappings[table_name] = PSqlHBaseTableMapping(conf)
  return table_mappings
  
class PSqlHBaseTableMapping:
  # conf can be anything with a get function eg, a dictionary
  def __init__(self, conf):
    self.psql_schema = conf.get(PSQL_SCHEMA)
    self.psql_table_name = conf.get(PSQL_TABLENAME)
    self.psql_key_column = conf.get(PSQL_KEYCOL)
    self.psql_columns = conf.get(PSQL_COLUMNS).split()
    self.hbase_table_name = conf.get(HBASE_TABLENAME)
    self.hbase_column_descriptors = conf.get(HBASE_COLUMNDESCS).split()
    self.hbase_row_prefix = conf.get(HBASE_ROWPREFIX)
    
    if len(self.psql_columns) != len(self.hbase_column_descriptors):
      raise Exception("psql_columns and hbase_column_descriptors must have same length")
    
  
