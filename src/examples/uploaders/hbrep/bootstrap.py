import sys, os

import pgq, pgq.producer
import skytools

from HBaseConnection import *
import tablemapping

class HBaseBootstrap(skytools.DBScript):
  """Bootstrapping script for loading columns from a table in postgresql to hbase."""
  
  def __init__(self, service_name, args):
    # This will process any options eg -k -v -d
    skytools.DBScript.__init__(self, service_name, args)
    
    config_file = self.args[0]
    if len(self.args) < 2:
      print "need table names"
      sys.exit(1)
    else:
      self.table_names = self.args[1:]
    
    #just to check this option exists
    self.cf.get("postgresql_db")
    
    self.max_batch_size = int(self.cf.get("max_batch_size", "10000"))
    self.hbase_hostname = self.cf.get("hbase_hostname", "localhost")
    self.hbase_port = int(self.cf.get("hbase_port", "9090"))
    self.table_mappings = tablemapping.load_table_mappings(config_file, self.table_names)
  
  def startup(self):
    # make sure the script loops only once.
    self.set_single_loop(1)
    self.log.info("Starting " + self.job_name)
    
  def work(self):
    for t in self.table_names:
      self.bootstrap_table(t)
      
  def bootstrap_table(self, table_name):
    try:
      self.log.info("Bootstrapping table %s" % table_name)
      hbase = HBaseConnection(self.hbase_hostname, self.hbase_port)
      try:
        table_mapping = self.table_mappings[table_name]
      
        self.log.debug("Connecting to HBase")
        hbase.connect()
        
        # Fetch postgresql cursor
        self.log.debug("Getting postgresql cursor")
        db = self.get_database("postgresql_db")
        curs = db.cursor()
        
        hbase.validate_table_name(table_mapping.hbase_table_name)
        hbase.validate_column_descriptors(table_mapping.hbase_table_name, table_mapping.hbase_column_descriptors)
        
        try:
          dump_file = self.cf.get("bootstrap_tmpfile")
        except:
          dump_file = None
        
        if dump_file != None:
          row_source = CopiedRows(self.log, curs, dump_file)
        else:
          row_source = SelectedRows(self.log, curs)
        
        table_name = table_mapping.psql_schema+"."+table_mapping.psql_table_name
        # we are careful to make sure that the first column will be the key.
        column_list = [table_mapping.psql_key_column] + table_mapping.psql_columns
        
        # Load the rows either via a select or via a table copy to file. 
        # Either way, it does not load it all into memory. 
        # copy is faster, but may incorrectly handle data with tabs in it.
        row_source.load_rows(table_name, column_list)
        
        # max number of rows to fetch at once
        batch_size = self.max_batch_size
        total_rows = 0L
          
        self.log.debug("Starting puts to hbase")
        rows = row_source.get_rows(batch_size)
        while rows != []:
          batches = []
          for row in rows:
            batches.append(self.createRowBatch(table_mapping, row))
          
          hbase.client.mutateRows(table_mapping.hbase_table_name, batches)
          total_rows = total_rows + len(batches)
          self.log.debug("total rows put = %d" % (total_rows))
          # get next batch of rows
          rows = row_source.get_rows(batch_size)
          
        self.log.info("total rows put = %d" % (total_rows))
        self.log.info("Bootstrapping table %s complete" % table_name)
        
        
      except Exception, e:
        #self.log.info(e)
        sys.exit(e)
      
    finally:
      hbase.disconnect()
  
  def createRowBatch(self, table_mapping, row):
    batch = BatchMutation()
    batch.row = table_mapping.hbase_row_prefix + str(row[0])
    batch.mutations = []
    for column, value in zip(table_mapping.hbase_column_descriptors, row[1:]):
      if value != 'NULL' and  value != None:
        m = Mutation()
        m.column = column
        m.value = str(value)
        batch.mutations.append(m)
    return batch
  
  
## Helper classes to fetch rows from a select, or from a table dumped by copy
  
class RowSource:
  """ Base class for fetching rows from somewhere. """
  
  def __init__(self, log):
    self.log = log
    
  def make_column_str(self, column_list):
    i = 0
    while i < len(column_list):
      column_list[i] = '"%s"' % column_list[i]
      i += 1
    return ",".join(column_list)
  
   
class CopiedRows(RowSource):
  """ 
  Class for fetching rows from a postgresql database,
  rows are dumped to a copied to a file first
  """
  def __init__(self, log, curs, dump_file):
    RowSource.__init__(self, log)
    self.dump_file = dump_file
    # Set DBAPI-2.0 cursor
    self.curs = curs
    
  def load_rows(self, table_name, column_list):
    columns = self.make_column_str(column_list)
    self.log.debug("starting dump to file:%s. table:%s. columns:%s" % (self.dump_file, table_name, columns))
    dump_out = open(self.dump_file, 'w')
    self.curs.copy_to(dump_out, table_name + "(%s)" % columns, '\t', 'NULL')
    dump_out.close()
    self.log.debug("table %s dump complete" % table_name)
    
    self.dump_in = open(self.dump_file, 'r')
    
  def get_rows(self, no_of_rows):
    rows = []
    if not self.dump_in.closed:
      for line in self.dump_in:
        rows.append(line.split())      
        if len(rows) >= no_of_rows:
          break
      if rows == []:
        self.dump_in.close()
    return rows


class SelectedRows(RowSource):
  """ 
  Class for fetching rows from a postgresql database,
  rows are fetched via a select on the entire table.
  """
  def __init__(self, log, curs):
    RowSource.__init__(self, log)
    # Set DBAPI-2.0 cursor
    self.curs = curs
    
  def load_rows(self, table_name, column_list):
    columns = self.make_column_str(column_list)
    q = "SELECT %s FROM %s" % (columns,table_name)
    self.log.debug("Executing query %s" % q)
    self.curs.execute(q)
    self.log.debug("query finished")
    
  def get_rows(self, no_of_rows):
    return self.curs.fetchmany(no_of_rows)
    

if __name__ == '__main__':
  bootstrap = HBaseBootstrap("HBaseReplic",sys.argv[1:])
  bootstrap.start()
