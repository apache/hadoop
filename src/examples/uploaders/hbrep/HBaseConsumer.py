import sys, os, pgq, skytools, ConfigParser

from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

from HBaseConnection import *
import tablemapping

INSERT = 'I'
UPDATE = 'U'
DELETE = 'D'

class HBaseConsumer(pgq.Consumer):
  """HBaseConsumer is a pgq.Consumer that sends processed events to hbase as mutations."""
  
  def __init__(self, service_name, args):
    pgq.Consumer.__init__(self, service_name, "postgresql_db", args)
    
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
    self.row_limit = int(self.cf.get("bootstrap_row_limit", 0))
    self.table_mappings = tablemapping.load_table_mappings(config_file, self.table_names)
  
  def process_batch(self, source_db, batch_id, event_list):
    try:
      self.log.debug("processing batch %s" % (batch_id))
      hbase = HBaseConnection(self.hbase_hostname, self.hbase_port)
      try:
        self.log.debug("Connecting to HBase")
        hbase.connect()
              
        i = 0L
        for event in event_list:
          i = i+1
          self.process_event(event, hbase)
        print "%i events processed" % (i)
        
      except Exception, e:
        #self.log.info(e)
        sys.exit(e)
      
    finally:
      hbase.disconnect()
  
  def process_event(self, event, hbase):
    if event.ev_extra1 in self.table_mappings:
      table_mapping = self.table_mappings[event.ev_extra1]
    else:
      self.log.info("table name not found in config, skipping event")
      return
    #hbase.validate_table_name(table_mapping.hbase_table_name)
    #hbase.validate_column_descriptors(table_mapping.hbase_table_name, table_mapping.hbase_column_descriptors)
    event_data = skytools.db_urldecode(event.data)
    event_type = event.type.split(':')[0]
    
    batch = BatchMutation()
    batch.row = table_mapping.hbase_row_prefix + str(event_data[table_mapping.psql_key_column])
        
    batch.mutations = []
    for psql_column, hbase_column in zip(table_mapping.psql_columns, table_mapping.hbase_column_descriptors):
      if event_type == INSERT or event_type == UPDATE:
        m = Mutation()
        m.column = hbase_column
        m.value = str(event_data[psql_column])
      elif event_type == DELETE:
        # delete this column entry
        m = Mutation()
        m.isDelete = True
        m.column = hbase_column
      else:
        raise Exception("Invalid event type: %s, event data was: %s" % (event_type, str(event_data)))
      batch.mutations.append(m)
    hbase.client.mutateRow(table_mapping.hbase_table_name, batch.row, batch.mutations)
    event.tag_done()
    
if __name__ == '__main__':
  script = HBaseConsumer("HBaseReplic",sys.argv[1:])
  script.start()
