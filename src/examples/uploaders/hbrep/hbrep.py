#!/usr/bin/env python
import sys, os

import pgq, pgq.producer
import skytools, skytools._pyquoting

from bootstrap import HBaseBootstrap
from HBaseConsumer import HBaseConsumer

command_usage = """
%prog [options] inifile command [tablenames]

commands:
  play          Run event consumer to update specified tables with hbase.
  bootstrap     Bootstrap specified tables args into hbase.
  install       Setup the pgq queue, and install trigger on each table.
  uninstall     Remove the triggers from each specified table.
"""

class HBaseReplic(skytools.DBScript):
  def __init__(self, service_name, args):
    try:
      self.run_script = 0
      
      # This will process any options eg -k -v -d
      skytools.DBScript.__init__(self, service_name, args)
      
      self.config_file = self.args[0]
      
      if len(self.args) < 2:
        self.print_usage()
        print "need command"
        sys.exit(0)
      cmd = self.args[1]
        
      if not cmd in ["play","bootstrap","install", "uninstall"]:
        self.print_usage()
        print "unknown command"
        sys.exit(0)
        
      if len(self.args) < 3:
        self.print_usage()
        print "need table names"
        sys.exit(0)
      else:
        self.table_names = self.args[2:]
      
      if cmd == "play":
        self.run_script = HBaseConsumer(service_name, [self.config_file] + self.table_names)
      elif cmd == "bootstrap":
        self.run_script = HBaseBootstrap(service_name, [self.config_file] + self.table_names)
      elif cmd == "install":
        self.work = self.do_install
      elif cmd == "uninstall":
        self.work = self.do_uninstall
        
    except Exception, e:
      sys.exit(e)
  
  def print_usage(self):
    print "Usage: " + command_usage
    
  def init_optparse(self, parser=None):
    p = skytools.DBScript.init_optparse(self, parser)
    p.set_usage(command_usage.strip())
    return p

  def start(self):
    if self.run_script:
      self.run_script.start()
    else:
      skytools.DBScript.start(self)
      
  def startup(self):
    # make sure the script loops only once.
    self.set_single_loop(1)
    
  def do_install(self):
    try:
      queue_name = self.cf.get("pgq_queue_name")
      consumer = self.job_name
      
      self.log.info('Creating queue: %s' % queue_name)
      self.exec_sql("select pgq.create_queue(%s)", [queue_name])
  
      self.log.info('Registering consumer %s on queue %s' % (consumer, queue_name))
      self.exec_sql("select pgq.register_consumer(%s, %s)", [queue_name, consumer])
  
      for table_name in self.table_names:
        self.log.info('Creating trigger hbase_replic on table %s' % (table_name))
        q = """
        CREATE TRIGGER hbase_replic
          AFTER INSERT OR UPDATE OR DELETE
          ON %s
          FOR EACH ROW
          EXECUTE PROCEDURE pgq.logutriga('%s')"""
        self.exec_sql(q % (table_name, queue_name), [])
    except Exception, e:
      sys.exit(e)
      
  def do_uninstall(self):
    try:
      queue_name = self.cf.get("pgq_queue_name")
      consumer = "HBaseReplic"
      
      #self.log.info('Unregistering consumer %s on queue %s' % (consumer, queue_name))
      #self.exec_sql("select pgq.unregister_consumer(%s, %s)", [queue_name, consumer])
  
      for table_name in self.table_names:
        self.log.info('Dropping trigger hbase_replic on table %s' % (table_name))
        q = "DROP TRIGGER hbase_replic ON %s" % table_name
        self.exec_sql(q, [])
        
    except Exception, e:
      sys.exit(e)
    
  def exec_sql(self, q, args):
    self.log.debug(q)
    db = self.get_database('postgresql_db')
    curs = db.cursor()
    curs.execute(q, args)
    db.commit()
    
if __name__ == '__main__':
  script = HBaseReplic("HBaseReplic",sys.argv[1:])
  script.start()
