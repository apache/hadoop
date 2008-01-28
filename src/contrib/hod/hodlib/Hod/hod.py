#Licensed to the Apache Software Foundation (ASF) under one
#or more contributor license agreements.  See the NOTICE file
#distributed with this work for additional information
#regarding copyright ownership.  The ASF licenses this file
#to you under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance
#with the License.  You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
# -*- python -*-

import sys, os, getpass, pprint, re, cPickle, random, shutil, time

import hodlib.Common.logger

from hodlib.ServiceRegistry.serviceRegistry import svcrgy
from hodlib.Common.xmlrpc import hodXRClient
from hodlib.Common.util import to_http_url, get_exception_string
from hodlib.Common.util import get_exception_error_string
from hodlib.Common.util import hodInterrupt, HodInterruptException
from hodlib.Common.util import HOD_INTERRUPTED_CODE

from hodlib.Common.nodepoolutil import NodePoolUtil
from hodlib.Hod.hadoop import hadoopCluster, hadoopScript

CLUSTER_DATA_FILE = 'clusters'

class hodState:
  def __init__(self, store):
    self.__store = store
    self.__stateFile = None
    self.__init_store()
    self.__STORE_EXT = ".state"
   
  def __init_store(self):
    if not os.path.exists(self.__store):
      os.mkdir(self.__store)
  
  def __set_state_file(self, id=None):
    if id:
      self.__stateFile = os.path.join(self.__store, "%s%s" % (id, 
                                      self.__STORE_EXT))
    else:
      for item in os.listdir(self.__store):
        if item.endswith(self.__STORE_EXT):  
          self.__stateFile = os.path.join(self.__store, item)          
          
  def read(self, id=None):
    info = {}
    
    self.__set_state_file(id)
  
    if self.__stateFile:
      if os.path.isfile(self.__stateFile):
        stateFile = open(self.__stateFile, 'r')
        try:
          info = cPickle.load(stateFile)
        except EOFError:
          pass
        
        stateFile.close()
    
    return info
           
  def write(self, id, info):
    self.__set_state_file(id)
    if not os.path.exists(self.__stateFile):
      self.clear(id)
      
    stateFile = open(self.__stateFile, 'w')
    cPickle.dump(info, stateFile)
    stateFile.close()
  
  def clear(self, id=None):
    self.__set_state_file(id)
    if self.__stateFile and os.path.exists(self.__stateFile):
      os.remove(self.__stateFile)
    else:
      for item in os.listdir(self.__store):
        if item.endswith(self.__STORE_EXT):
          os.remove(item)
        
class hodRunner:
  def __init__(self, cfg):
    self.__ops = [ 'prepare', 'allocate', 'deallocate', 
                   'list', 'info', 'help' ]           
    self.__cfg = cfg  
    self.__npd = self.__cfg['nodepooldesc']
    self.__opCode = 0
    self.__user = getpass.getuser()
    self.__registry = None
    self.__baseLogger = None
    self.__setup_logger()
    
    self.__userState = hodState(self.__cfg['hod']['user_state']) 
    
    self.__clusterState = None
    self.__clusterStateInfo = { 'env' : None, 'hdfs' : None, 'mapred' : None }
    
    self.__cluster = hadoopCluster(self.__cfg, self.__log)
  
  def __setup_logger(self):
    self.__baseLogger = hodlib.Common.logger.hodLog('hod')
    self.__log = self.__baseLogger.add_logger(self.__user )
 
    if self.__cfg['hod']['stream']:
      self.__baseLogger.add_stream(level=self.__cfg['hod']['debug'], 
                            addToLoggerNames=(self.__user ,))
  
    if self.__cfg['hod'].has_key('syslog-address'):
      self.__baseLogger.add_syslog(self.__cfg['hod']['syslog-address'], 
                                   level=self.__cfg['hod']['debug'], 
                                   addToLoggerNames=(self.__user ,))

  def get_logger(self):
    return self.__log

  def __setup_cluster_logger(self, directory):
    self.__baseLogger.add_file(logDirectory=directory, level=4, 
                               addToLoggerNames=(self.__user ,))

  def __setup_cluster_state(self, directory):
    self.__clusterState = hodState(directory)

  def __norm_cluster_dir(self, directory):
    directory = os.path.expanduser(directory)
    if not os.path.isabs(directory):
      directory = os.path.join(self.__cfg['hod']['original-dir'], directory)
    directory = os.path.abspath(directory)
    
    return directory
  
  def __setup_service_registry(self):
    cfg = self.__cfg['hod'].copy()
    cfg['debug'] = 0
    self.__registry = svcrgy(cfg)
    self.__registry.start()
    self.__log.debug(self.__registry.getXMLRPCAddr())
    self.__cfg['hod']['xrs-address'] = self.__registry.getXMLRPCAddr()
    self.__cfg['ringmaster']['svcrgy-addr'] = self.__cfg['hod']['xrs-address']

  def __set_cluster_state_info(self, env, hdfs, mapred, ring, jobid, min, max):
    self.__clusterStateInfo['env'] = env
    self.__clusterStateInfo['hdfs'] = "http://%s" % hdfs
    self.__clusterStateInfo['mapred'] = "http://%s" % mapred
    self.__clusterStateInfo['ring'] = ring
    self.__clusterStateInfo['jobid'] = jobid
    self.__clusterStateInfo['min'] = min
    self.__clusterStateInfo['max'] = max
    
  def __set_user_state_info(self, info):
    userState = self.__userState.read(CLUSTER_DATA_FILE)
    for key in info.keys():
      userState[key] = info[key]
      
    self.__userState.write(CLUSTER_DATA_FILE, userState)  

  def __remove_cluster(self, clusterDir):
    clusterInfo = self.__userState.read(CLUSTER_DATA_FILE)
    if clusterDir in clusterInfo:
      del(clusterInfo[clusterDir])
      self.__userState.write(CLUSTER_DATA_FILE, clusterInfo)
      
  def __cleanup(self):
    if self.__registry: self.__registry.stop()
    
  def __check_operation(self, operation):    
    opList = operation.split()
    
    if not opList[0] in self.__ops:
      self.__log.critical("Invalid hod operation specified: %s" % operation)
      self._op_help(None)
      self.__opCode = 2
         
    return opList 
  
  def _op_allocate(self, args):
    operation = "allocate"
    argLength = len(args)
    min = 0
    max = 0
    if argLength == 3:
      nodes = args[2]
      clusterDir = self.__norm_cluster_dir(args[1])
      if os.path.isdir(clusterDir):
        self.__setup_cluster_logger(clusterDir)
        if re.match('\d+-\d+', nodes):
          (min, max) = nodes.split("-")
          min = int(min)
          max = int(max)
        else:
          try:
            nodes = int(nodes)
            min = nodes
            max = nodes
          except ValueError:
            self.__log.critical(
            "%s operation requires a single argument. n nodes, or n-m nodes." % 
            operation)
            self.__opCode = 3
          else:
            self.__setup_cluster_state(clusterDir)
            clusterInfo = self.__clusterState.read()
            self.__opCode = self.__cluster.check_cluster(clusterInfo)
            if self.__opCode == 0 or self.__opCode == 15:
              self.__setup_service_registry()   
              if hodInterrupt.isSet(): 
                self.__cleanup()
                raise HodInterruptException()
              self.__log.info("Service Registry Started.")
              try:
                allocateStatus = self.__cluster.allocate(clusterDir, min, max)    
              except HodInterruptException, h:
                self.__cleanup()
                raise h
              # Allocation has gone through.
              # Don't care about interrupts any more

              if allocateStatus == 0:
                self.__set_cluster_state_info(os.environ, 
                                              self.__cluster.hdfsInfo, 
                                              self.__cluster.mapredInfo, 
                                              self.__cluster.ringmasterXRS,
                                              self.__cluster.jobId,
                                              min, max)
                self.__setup_cluster_state(clusterDir)
                self.__clusterState.write(self.__cluster.jobId, 
                                          self.__clusterStateInfo)
                #  Do we need to check for interrupts here ??

                self.__set_user_state_info( 
                  { clusterDir : self.__cluster.jobId, } )
              self.__opCode = allocateStatus
            elif self.__opCode == 12:
              self.__log.critical("Cluster %s already allocated." % clusterDir)
            elif self.__opCode == 10:
              self.__log.critical("dead\t%s\t%s" % (clusterInfo['jobid'], 
                                                    clusterDir))
            elif self.__opCode == 13:
              self.__log.warn("hdfs dead\t%s\t%s" % (clusterInfo['jobid'], 
                                                         clusterDir))
            elif self.__opCode == 14:
              self.__log.warn("mapred dead\t%s\t%s" % (clusterInfo['jobid'], 
                                                       clusterDir))   
            
            if self.__opCode > 0 and self.__opCode != 15:
              self.__log.critical("Cannot allocate cluster %s" % clusterDir)
            
      else:
        self.__log.critical("Invalid cluster directory '%s' specified." % 
                          clusterDir)
        self.__opCode = 3
    else:
      self.__log.critical("%s operation requires two arguments. "  % operation
                        + "A cluster path and n nodes, or min-max nodes.")
      self.__opCode = 3
 
  def _is_cluster_allocated(self, clusterDir):
    if os.path.isdir(clusterDir):
      self.__setup_cluster_state(clusterDir)
      clusterInfo = self.__clusterState.read()
      if clusterInfo != {}:
        return True
    return False

  def _op_deallocate(self, args):
    operation = "deallocate"
    argLength = len(args)
    if argLength == 2:
      clusterDir = self.__norm_cluster_dir(args[1])
      if os.path.isdir(clusterDir):
        self.__setup_cluster_state(clusterDir)
        clusterInfo = self.__clusterState.read()
        if clusterInfo == {}:
          self.__opCode = 15
          self.__log.critical("Cluster %s not allocated." % clusterDir)
        else:
          self.__opCode = \
            self.__cluster.deallocate(clusterDir, clusterInfo)
          # irrespective of whether deallocate failed or not\
          # remove the cluster state.
          self.__clusterState.clear()
          self.__remove_cluster(clusterDir)
      else:
        self.__log.critical("Invalid cluster directory '%s' specified." % 
                            clusterDir)
        self.__opCode = 3        
    else:
      self.__log.critical("%s operation requires one argument. "  % operation
                        + "A cluster path.")
      self.__opCode = 3
            
  def _op_list(self, args):
    clusterList = self.__userState.read(CLUSTER_DATA_FILE)
    for path in clusterList.keys():
      self.__setup_cluster_state(path)
      clusterInfo = self.__clusterState.read()
      clusterStatus = self.__cluster.check_cluster(clusterInfo)
      if clusterStatus == 12:
        self.__log.info("alive\t%s\t%s" % (clusterList[path], path))
      elif clusterStatus == 10:
        self.__log.info("dead\t%s\t%s" % (clusterList[path], path))
      elif clusterStatus == 13:
        self.__log.info("hdfs dead\t%s\t%s" % (clusterList[path], path))
      elif clusterStatus == 14:
        self.__log.info("mapred dead\t%s\t%s" % (clusterList[path], path))    
         
  def _op_info(self, args):
    operation = 'info'
    argLength = len(args)  
    if argLength == 2:
      clusterDir = self.__norm_cluster_dir(args[1])
      if os.path.isdir(clusterDir):
        self.__setup_cluster_state(clusterDir)
        clusterInfo = self.__clusterState.read()
        clusterStatus = self.__cluster.check_cluster(clusterInfo)
        if clusterStatus == 12:
          self.__log.info(clusterDir)
          self.__print_cluster_info(clusterInfo)
        elif clusterStatus == 10:
          self.__log.critical("%s cluster is dead" % clusterDir)
        elif clusterStatus == 13:
          self.__log.warn("%s cluster hdfs is dead" % clusterDir)
        elif clusterStatus == 14:
          self.__log.warn("%s cluster mapred is dead" % clusterDir)

        if clusterStatus != 12:
          if clusterStatus == 15:
            self.__log.critical("Cluster %s not allocated." % clusterDir)
          else:
            self.__print_cluster_info(clusterInfo)
            
          self.__opCode = clusterStatus
      else:
        self.__log.critical("'%s' does not exist." % clusterDir)
        self.__opCode = 3 
    else:
      self.__log.critical("%s operation requires one argument. "  % operation
                        + "A cluster path.")
      self.__opCode = 3      
 
  def __print_cluster_info(self, clusterInfo):
    keys = clusterInfo.keys()
    keys.sort()
    for key in keys:
      if key != 'env':
        self.__log.info("%s\t%s" % (key, clusterInfo[key]))  
            
    if self.__cfg['hod']['debug'] == 4:
      for var in clusterInfo['env'].keys():
        self.__log.debug("%s = %s" % (var, clusterInfo['env'][var]))

 
  def _op_help(self, args):  
    print "hod operations:\n"
    print " allocate <directory> <nodes> - Allocates a cluster of n nodes using the specified cluster"
    print "                                directory to store cluster state information.  The Hadoop site XML" 
    print "                                is also stored in this location."
    print ""
    print " deallocate <directory>       - Deallocates a cluster using the pecified cluster directory.  This"
    print "                                operation is also required to clean up a dead cluster."      
    print ""
    print " list                         - List all clusters currently allocated by a user, along with" 
    print "                                limited status information and the cluster's job ID."
    print ""
    print " info <directory>             - Provide detailed information on an allocated cluster."
  
  def operation(self):  
    operation = self.__cfg['hod']['operation']
    try:
      opList = self.__check_operation(operation)
      if self.__opCode == 0:
        getattr(self, "_op_%s" % opList[0])(opList)
    except HodInterruptException, h:
      self.__log.critical("op: %s failed because of an process interrupt." \
                                                                % operation)
      self.__opCode = HOD_INTERRUPTED_CODE
    except:
      self.__log.critical("op: %s failed: %s" % (operation,
                          get_exception_error_string()))
      self.__log.debug(get_exception_string())
    
    self.__cleanup()
    
    self.__log.debug("return code: %s" % self.__opCode)
    
    return self.__opCode
  
  def script(self):
    script = self.__cfg['hod']['script']
    nodes = self.__cfg['hod']['min-nodes']
    isExecutable = os.access(script, os.X_OK)
    if not isExecutable:
      self.__log.critical('Script %s is not an executable.' % script)
      return 1

    clusterDir = "/tmp/%s.%s" % (self.__cfg['hod']['userid'], 
                                 random.randint(0, 20000))
    os.mkdir(clusterDir)
    ret = 0
    try:
      self._op_allocate(('allocate', clusterDir, str(nodes)))
      if self.__opCode == 0:
        if self.__cfg['hod'].has_key('script-wait-time'):
          time.sleep(self.__cfg['hod']['script-wait-time'])
          self.__log.debug('Slept for %d time. Now going to run the script' % self.__cfg['hod']['script-wait-time'])
        if hodInterrupt.isSet():
          self.__log.debug('Interrupt set - not executing script')
        else:
          scriptRunner = hadoopScript(clusterDir, 
                                  self.__cfg['hod']['original-dir'])
          self.__opCode = scriptRunner.run(script)
          ret = self.__opCode
          self.__log.debug("Exit code from running the script: %d" % self.__opCode)
      else:
        self.__log.critical("Error %d in allocating the cluster. Cannot run the script." % self.__opCode)

      if hodInterrupt.isSet():
        # Got interrupt while executing script. Unsetting it for deallocating
        hodInterrupt.setFlag(False)
      if self._is_cluster_allocated(clusterDir):
        self._op_deallocate(('deallocate', clusterDir))
      shutil.rmtree(clusterDir, True)
    except HodInterruptException, h:
      self.__log.critical("Script failed because of an process interrupt.")
      self.__opCode = HOD_INTERRUPTED_CODE
    except:
      self.__log.critical("script: %s failed: %s" % (script,
                          get_exception_error_string()))
      self.__log.debug(get_exception_string())
    
    self.__cleanup()      
    
    # We want to give importance to a failed script's exit code.
    if ret != 0:
      self.__opCode = ret

    return self.__opCode
