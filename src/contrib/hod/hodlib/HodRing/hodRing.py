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
#!/usr/bin/env python
"""hodring launches hadoop commands on work node and 
 cleans up all the work dirs afterward
"""
# -*- python -*-
import os, sys, time, shutil, getpass, xml.dom.minidom, xml.dom.pulldom
import socket, sets, urllib, csv, signal, pprint, random, re

from xml.dom import getDOMImplementation
from pprint import pformat
from optparse import OptionParser
from urlparse import urlparse
from hodlib.Common.util import local_fqdn

binfile = sys.path[0]
libdir = os.path.dirname(binfile)
sys.path.append(libdir)

import hodlib.Common.logger

from hodlib.GridServices.service import *
from hodlib.Common.util import *
from hodlib.Common.socketServers import threadedHTTPServer
from hodlib.Common.hodsvc import hodBaseService
from hodlib.Common.threads import simpleCommand
from hodlib.Common.xmlrpc import hodXRClient

mswindows = (sys.platform == "win32")
originalcwd = os.getcwd()

reHdfsURI = re.compile("hdfs://(.*?:\d+)(.*)")

class CommandDesc:
  """A class that represents the commands that
  are run by hodring"""
  def __init__(self, dict, log):
    self.log = log
    self.log.debug("In command desc")
    self.log.debug("Done in command desc")
    dict.setdefault('argv', [])
    dict.setdefault('envs', {})
    dict.setdefault('java-opts', [])
    dict.setdefault('workdirs', [])
    dict.setdefault('attrs', {})
    dict.setdefault('final-attrs', {})
    dict.setdefault('fg', False)
    dict.setdefault('ignorefailures', False)
    dict.setdefault('stdin', None)

    self.log.debug("Printing dict")
    self._checkRequired(dict)
    self.dict = dict

  def _checkRequired(self, dict):
    if 'name' not in dict:
      raise ValueError, "Command description lacks 'name'"
    if 'program' not in dict:
      raise ValueError, "Command description lacks 'program'"
    if 'pkgdirs' not in dict:
      raise ValueError, "Command description lacks 'pkgdirs'"

  def getName(self):
    return self.dict['name']

  def getProgram(self):
    return self.dict['program']

  def getArgv(self):
    return self.dict['argv']

  def getEnvs(self):
    return self.dict['envs']

  def getJavaOpts(self):
    return self.dict['java-opts']

  def getPkgDirs(self):
    return self.dict['pkgdirs']

  def getWorkDirs(self):
    return self.dict['workdirs']

  def getAttrs(self):
    return self.dict['attrs']

  def getfinalAttrs(self):
    return self.dict['final-attrs']
  
  def isForeground(self):
    return self.dict['fg']

  def isIgnoreFailures(self):
    return self.dict['ignorefailures']

  def getStdin(self):
    return self.dict['stdin']

  def parseDesc(str):

    dict = CommandDesc._parseMap(str)

    dict['argv'] = CommandDesc._parseList(dict['argv'])
    dict['envs'] = CommandDesc._parseMap(dict['envs'])
    dict['pkgdirs'] = CommandDesc._parseList(dict['pkgdirs'], ':')
    dict['workdirs'] = CommandDesc._parseList(dict['workdirs'], ':')
    dict['attrs'] = CommandDesc._parseMap(dict['attrs'])
    dict['final-attrs'] = CommandDesc._parseMap(dict['final-attrs'])
						
    return CommandDesc(dict)

  parseDesc = staticmethod(parseDesc)

  def _parseList(str, delim = ','):
    list = []
    for row in csv.reader([str], delimiter=delim, escapechar='\\', 
                          quoting=csv.QUOTE_NONE, doublequote=False):
      list.extend(row)
    return list

  _parseList = staticmethod(_parseList)

  def _parseMap(str):
    """Parses key value pairs"""
    dict = {}
    for row in csv.reader([str], escapechar='\\', quoting=csv.QUOTE_NONE, doublequote=False):
      for f in row:
        [k, v] = f.split('=', 1)
        dict[k] = v
    return dict

  _parseMap = staticmethod(_parseMap)

      
class HadoopCommand:
  """Runs a single hadoop command"""
    
  def __init__(self, id, desc, tempdir, tardir, log, javahome, restart=False):
    self.desc = desc
    self.log = log
    self.javahome = javahome
    self.program = desc.getProgram()
    self.name = desc.getName()
    self.workdirs = desc.getWorkDirs()
    self.hadoopdir = tempdir
    self.confdir = os.path.join(self.hadoopdir, '%d-%s' % (id, self.name), 
                                "confdir")
    self.logdir = os.path.join(self.hadoopdir, '%d-%s' % (id, self.name), 
                               "logdir")
    self.child = None
    self.restart = restart
    self.filledInKeyVals = []
    self._createWorkDirs()
    self._createHadoopSiteXml()
    self._createHadoopLogDir()
    self.__hadoopThread = None

  def _createWorkDirs(self):
    for dir in self.workdirs:
      if os.path.exists(dir):
        if not os.access(dir, os.F_OK | os.R_OK | os.W_OK | os.X_OK):
          raise ValueError, "Workdir %s does not allow rwx permission." % (dir)
        continue
      try:
        os.makedirs(dir)
      except:
        pass

  def getFilledInKeyValues(self):
    return self.filledInKeyVals

  def createXML(self, doc, attr, topElement, final):
    for k,v in attr.iteritems():
      self.log.debug('_createHadoopSiteXml: ' + str(k) + " " + str(v))
      if ( v == "fillinport" ):
        v = "%d" % (ServiceUtil.getUniqRandomPort(low=50000))

      keyvalpair = ''
      if isinstance(v, (tuple, list)):
        for item in v:
          keyvalpair = "%s%s=%s," % (keyvalpair, k, item)
        keyvalpair = keyvalpair[:-1]
      else:
        keyvalpair = k + '=' + v

      self.filledInKeyVals.append(keyvalpair)
      if(k == "mapred.job.tracker"): # total hack for time's sake
        keyvalpair = k + "=" + v
        self.filledInKeyVals.append(keyvalpair)
	
      if ( v == "fillinhostport"):
        port = "%d" % (ServiceUtil.getUniqRandomPort(low=50000))
        self.log.debug('Setting hostname to: %s' % local_fqdn())
        v = local_fqdn() + ':' + port
      
      keyvalpair = ''
      if isinstance(v, (tuple, list)):
        for item in v:
          keyvalpair = "%s%s=%s," % (keyvalpair, k, item)
        keyvalpair = keyvalpair[:-1]
      else:
        keyvalpair = k + '=' + v
      
      self.filledInKeyVals.append(keyvalpair)
      if ( v == "fillindir"):
        v = os.path.join('/mapredsystem', local_fqdn())
        pass
      
      prop = None
      if isinstance(v, (tuple, list)):
        for item in v:
          prop = self._createXmlElement(doc, k, item, "No description", final)
          topElement.appendChild(prop)
      else:
        prop = self._createXmlElement(doc, k, v, "No description", final)
        topElement.appendChild(prop)
	
  def _createHadoopSiteXml(self):
    if self.restart:
      if not os.path.exists(self.confdir):
        os.makedirs(self.confdir)
    else:
      assert os.path.exists(self.confdir) == False
      os.makedirs(self.confdir)

    implementation = getDOMImplementation()
    doc = implementation.createDocument('', 'configuration', None)
    comment = doc.createComment("This is an auto generated hadoop-site.xml, do not modify")
    topElement = doc.documentElement
    topElement.appendChild(comment)
    
    attr = self.desc.getfinalAttrs()
    self.createXML(doc, attr, topElement, True)
    attr = self.desc.getAttrs()
    self.createXML(doc, attr, topElement, False)
              
    
    siteName = os.path.join(self.confdir, "hadoop-site.xml")
    sitefile = file(siteName, 'w')
    print >> sitefile, topElement.toxml()
    sitefile.close()
    self.log.debug('created %s' % (siteName))

  def _createHadoopLogDir(self):
    if self.restart:
      if not os.path.exists(self.logdir):
        os.makedirs(self.logdir)
    else:
      assert os.path.exists(self.logdir) == False
      os.makedirs(self.logdir)

  def _createXmlElement(self, doc, name, value, description, final):
    prop = doc.createElement("property")
    nameP = doc.createElement("name")
    string = doc.createTextNode(name)
    nameP.appendChild(string)
    valueP = doc.createElement("value")
    string = doc.createTextNode(value)
    valueP.appendChild(string)
    desc = doc.createElement("description")
    string = doc.createTextNode(description)
    desc.appendChild(string)
    prop.appendChild(nameP)
    prop.appendChild(valueP)
    prop.appendChild(desc)
    if (final):
      felement = doc.createElement("final")
      string = doc.createTextNode("true")
      felement.appendChild(string)
      prop.appendChild(felement)
      pass
    
    return prop

  def run(self, dir):
    status = True
    args = []
    desc = self.desc
    
    self.log.debug(pprint.pformat(desc.dict))
    
    
    self.log.debug("Got package dir of %s" % dir)
    
    self.path = os.path.join(dir, self.program)
    
    self.log.debug("path: %s" % self.path)
    args.append(self.path)
    args.extend(desc.getArgv())
    envs = desc.getEnvs()
    javaOpts = desc.getJavaOpts()
    fenvs = os.environ
    
    for k, v in envs.iteritems():
      fenvs[k] = v[0]
    
    self.log.debug(javaOpts)
    fenvs['HADOOP_OPTS'] = ''
    for option in javaOpts:
      fenvs['HADOOP_OPTS'] = "%s%s " % (fenvs['HADOOP_OPTS'], option)
    
    fenvs['JAVA_HOME'] = self.javahome
    fenvs['HADOOP_CONF_DIR'] = self.confdir
    fenvs['HADOOP_LOG_DIR'] = self.logdir

    self.log.info(pprint.pformat(fenvs))

    hadoopCommand = ''
    for item in args:
        hadoopCommand = "%s%s " % (hadoopCommand, item)
        
    self.log.debug('running command: %s' % (hadoopCommand)) 
    self.log.debug('hadoop env: %s' % fenvs)

    self.__hadoopThread = simpleCommand('hadoop', hadoopCommand, env=fenvs)
    self.__hadoopThread.start()
    
    while self.__hadoopThread.stdin == None:
      time.sleep(.2)
      self.log.debug("hadoopThread still == None ...")
    
    input = desc.getStdin()
    self.log.debug("hadoop input: %s" % input)
    if input:
      if self.__hadoopThread.is_running():
        print >>self.__hadoopThread.stdin, input
      else:
        self.log.error("hadoop command failed to start")
    
    self.__hadoopThread.stdin.close()  
    
    self.log.debug("isForground: %s" % desc.isForeground())
    if desc.isForeground():
      self.log.debug("Waiting on hadoop to finish...")
      self.__hadoopThread.wait()
      
      self.log.debug("Joining hadoop thread...")
      self.__hadoopThread.join()
      if self.__hadoopThread.exit_code() != 0:
        status = False
    else:
      code = self.__hadoopThread.exit_code()
      if code != 0 and code != None:
        status = False
        
    self.log.debug("hadoop run status: %s" % status)    
    
    if status == False:
      for item in self.__hadoopThread.output():
        self.log.error(item)
      self.log.error('hadoop error: %s' % (
                       self.__hadoopThread.exit_status_string()))
   
    if (status == True) or (not desc.isIgnoreFailures()):
      return status
    else:
      self.log.error("Ignoring Failure")
      return True

  def kill(self):
    self.__hadoopThread.kill()
    if self.__hadoopThread:
      self.__hadoopThread.join()

  def addCleanup(self, list):
    list.extend(self.workdirs)
    list.append(self.confdir)

class HodRing(hodBaseService):
  """The main class for hodring that
  polls the commands it runs"""
  def __init__(self, config):
    hodBaseService.__init__(self, 'hodring', config['hodring'])
    self.log = self.logs['main']
    self._http = None
    self.__pkg = None
    self.__pkgDir = None 
    self.__tempDir = None
    self.__running = {}
    self.__hadoopLogDirs = []
    self.__init_temp_dir()

  def __init_temp_dir(self):
    self.__tempDir = os.path.join(self._cfg['temp-dir'], 
                                  "%s.%s.hodring" % (self._cfg['userid'], 
                                                      self._cfg['service-id']))
    if not os.path.exists(self.__tempDir):
      os.makedirs(self.__tempDir)
    os.chdir(self.__tempDir)  

  def __fetch(self, url, spath):
    retry = 3
    success = False
    while (retry != 0 and success != True):
      try:
        input = urllib.urlopen(url)
        bufsz = 81920
        buf = input.read(bufsz)
        out = open(spath, 'w')
        while len(buf) > 0:
          out.write(buf)
          buf = input.read(bufsz)
        input.close()
        out.close()
        success = True
      except:
        self.log.debug("Failed to copy file")
        retry = retry - 1
    if (retry == 0 and success != True):
      raise IOError, "Failed to copy the files"

      
  def __get_name(self, addr):
    parsedUrl = urlparse(addr)
    path = parsedUrl[2]
    split = path.split('/', 1)
    return split[1]

  def __get_dir(self, name):
    """Return the root directory inside the tarball
    specified by name. Assumes that the tarball begins
    with a root directory."""
    import tarfile
    myTarFile = tarfile.open(name)
    hadoopPackage = myTarFile.getnames()[0]
    self.log.debug("tarball name : %s hadoop package name : %s" %(name,hadoopPackage))
    return hadoopPackage

  def __download_package(self, ringClient):
    self.log.debug("Found download address: %s" % 
                   self._cfg['download-addr'])
    try:
      addr = 'none'
      downloadTime = self._cfg['tarball-retry-initial-time']           # download time depends on tarball size and network bandwidth
      
      increment = 0
      
      addr = ringClient.getTarList(self.hostname)

      while(addr == 'none'):
        rand = self._cfg['tarball-retry-initial-time'] + increment + \
                        random.uniform(0,self._cfg['tarball-retry-interval'])
        increment = increment + 1
        self.log.debug("got no tarball. Retrying again in %s seconds." % rand)
        time.sleep(rand)
        addr = ringClient.getTarList(self.hostname)

    
      self.log.debug("got this address %s" % addr)
      
      tarName = self.__get_name(addr)
      self.log.debug("tar package name: %s" % tarName)
      
      fetchPath = os.path.join(os.getcwd(), tarName) 
      self.log.debug("fetch path: %s" % fetchPath)
      
      self.__fetch(addr, fetchPath)
      self.log.debug("done fetching")
    
      tarUrl = "http://%s:%d/%s" % (self._http.server_address[0], 
                                    self._http.server_address[1], 
                                    tarName)
      try: 
        ringClient.registerTarSource(self.hostname, tarUrl,addr)
        #ringClient.tarDone(addr)
      except KeyError, e:
        self.log.error("registerTarSource and tarDone failed: ", e)
        raise KeyError(e)
      
      check = untar(fetchPath, os.getcwd())
      
      if (check == False):
        raise IOError, "Untarring failed."
      
      self.__pkg = self.__get_dir(tarName)
      self.__pkgDir = os.path.join(os.getcwd(), self.__pkg)      
    except Exception, e:
      self.log.error("Failed download tar package: %s" % 
                     get_exception_error_string())
      raise Exception(e)
      
  def __run_hadoop_commands(self, restart=True):
    id = 0
    for desc in self._cfg['commanddesc']:
      self.log.debug(pprint.pformat(desc.dict))
      cmd = HadoopCommand(id, desc, self.__tempDir, self.__pkgDir, self.log, 
                          self._cfg['java-home'], restart)
    
      self.__hadoopLogDirs.append(cmd.logdir)
      self.log.debug("hadoop log directory: %s" % self.__hadoopLogDirs)
      
      try:
        # if the tarball isn't there, we use the pkgs dir given.
        if self.__pkgDir == None:
          pkgdir = desc.getPkgDirs()
        else:
          pkgdir = self.__pkgDir

        self.log.debug('This is the packcage dir %s ' % (pkgdir))
        if not cmd.run(pkgdir):
          raise ValueError, "Can't launch command: %s" % pkgdir
      except Exception, e:
        print get_exception_string()
        self.__running[id] = cmd
        raise Exception(e)

      id += 1
      if desc.isForeground():
        continue
      self.__running[id-1] = cmd

  def stop(self):
    self.log.debug("Entered hodring stop.")
    if self._http: 
      self.log.debug("stopping http server...")
      self._http.stop()
    
    self.log.debug("call hodsvcrgy stop...")
    hodBaseService.stop(self)
    
    self.clean_up()
    
  def clean_up(self):
    os.chdir(originalcwd)
    if not mswindows:
      # do the UNIX double-fork magic, see Stevens' "Advanced 
      # Programming in the UNIX Environment" for details (ISBN 0201563177)
      try: 
        pid = os.fork() 
        if pid > 0:
          # exit first parent
          sys.exit(0) 
      except OSError, e: 
        self.log.error("fork #1 failed: %d (%s)" % (e.errno, e.strerror)) 
        sys.exit(1)

      # decouple from parent environment
      os.chdir("/") 
      os.setsid() 
      os.umask(0) 

      # do second fork
      try: 
        pid = os.fork() 
        if pid > 0:
          # exit from second parent, print eventual PID before
          sys.exit(0) 
      except OSError, e: 
        self.log.error("fork #2 failed: %d (%s)" % (e.errno, e.strerror))
        sys.exit(1) 

    try:
#      for cmd in self.__running.values():
#        self.log.debug("killing %s..." % cmd)
#        cmd.kill()
  
      list = []
      
      for cmd in self.__running.values():
        self.log.debug("addding %s to cleanup list..." % cmd)
        cmd.addCleanup(list)
      
      list.append(self.__tempDir)
         
      self.__archive_logs()   
          
      for dir in list:
        if os.path.exists(dir):
          self.log.debug('removing %s' % (dir))
          shutil.rmtree(dir, True)
    except:
      self.log.error(get_exception_string())
    sys.exit(0)

  def _xr_method_clusterStart(self, initialize=True):
    return self.clusterStart(initialize)

  def _xr_method_clusterStop(self):
    return self.clusterStop()
 
  def __copy_archive_to_dfs(self, archiveFile):        
    hdfsURIMatch = reHdfsURI.match(self._cfg['log-destination-uri'])
    
    # FIXME this is a complete and utter hack. Currently hadoop is broken
    # and does not understand hdfs:// syntax on the command line :(
    
    pid = os.getpid()
    tempConfDir = '/tmp/%s' % pid
    os.mkdir(tempConfDir)
    tempConfFileName = '%s/hadoop-site.xml' % tempConfDir
    tempHadoopConfig = open(tempConfFileName, 'w')
    print >>tempHadoopConfig, "<configuration>"
    print >>tempHadoopConfig, "  <property>"
    print >>tempHadoopConfig, "    <name>fs.default.name</name>"
    print >>tempHadoopConfig, "    <value>%s</value>" % hdfsURIMatch.group(1)
    print >>tempHadoopConfig, "    <description>No description</description>"
    print >>tempHadoopConfig, "  </property>"
    print >>tempHadoopConfig, "</configuration>"
    tempHadoopConfig.close()
    
    # END LAME HACK
    
    (head, tail) = os.path.split(archiveFile)
    destFile = os.path.join(hdfsURIMatch.group(2), self._cfg['userid'], 
                            self._cfg['service-id'], tail)
    
    self.log.info("copying archive %s to DFS %s ..." % (archiveFile, destFile))
    
    runningHadoops = self.__running.values()
    if (len(runningHadoops) == 0):
      self.log.info("len(runningHadoops) == 0, No running cluster?")
      self.log.info("Skipping __copy_archive_to_dfs")
      return
 
    run = runningHadoops[0]
    hadoopCmd = run.path
    if self._cfg.has_key('pkgs'):
      hadoopCmd = os.path.join(self._cfg['pkgs'], 'bin', 'hadoop')

    # LAME HACK AGAIN, using config generated above :( 
    copyCommand = "%s --config %s dfs -copyFromLocal %s %s" % (hadoopCmd, 
      tempConfDir, archiveFile, destFile)
    
    self.log.debug(copyCommand)
    
    copyThread = simpleCommand('hadoop', copyCommand)
    copyThread.start()
    copyThread.wait()
    copyThread.join()
    self.log.debug(pprint.pformat(copyThread.output()))
    
    # LAME HACK AGAIN, deleting config generated above :( 
    os.unlink(tempConfFileName)
    os.rmdir(tempConfDir)
    os.unlink(archiveFile)
  
  def __archive_logs(self):
    status = True
    if self._cfg.has_key("log-destination-uri"):
      try:
        if self.__hadoopLogDirs:
          date = time.localtime()
          for logDir in self.__hadoopLogDirs:
            (head, tail) = os.path.split(logDir)
            (head, logType) = os.path.split(head)
            tarBallFile = "%s-%s-%04d%02d%02d%02d%02d%02d-%s.tar.gz" % (
              logType, local_fqdn(), date[0], date[1], date[2], date[3], 
              date[4], date[5], random.randint(0,1000))
            
            if self._cfg["log-destination-uri"].startswith('file://'):
              tarBallFile = os.path.join(self._cfg["log-destination-uri"][7:], 
                                         tarBallFile)
            else:
              tarBallFile = os.path.join(self._cfg['temp-dir'], tarBallFile)
            
            self.log.info('archiving log files to: %s' % tarBallFile)
            status = tar(tarBallFile, logDir, ['*',])
            self.log.info('archive %s status: %s' % (tarBallFile, status))
            if status and \
              self._cfg["log-destination-uri"].startswith('hdfs://'):
              self.__copy_archive_to_dfs(tarBallFile)
          dict = {} 
      except:
        self.log.error(get_exception_string())
      
    return status
      
  def start(self):
    """Run and maintain hodring commands"""
    
    try:
      if self._cfg.has_key('download-addr'):
        self._http = threadedHTTPServer('', self._cfg['http-port-range'])
        self.log.info("Starting http server...")
        self._http.serve_forever()
        self.log.debug("http://%s:%d" % (self._http.server_address[0],
                     self._http.server_address[1]))
      
      hodBaseService.start(self)
      
      ringXRAddress = None
      if self._cfg.has_key('ringmaster-xrs-addr'):
        ringXRAddress = "http://%s:%s/" % (self._cfg['ringmaster-xrs-addr'][0],
                          self._cfg['ringmaster-xrs-addr'][1])
        self.log.debug("Ringmaster at %s" % ringXRAddress)

      self.log.debug("Creating service registry XML-RPC client.")
      serviceClient = hodXRClient(to_http_url(
                                  self._cfg['svcrgy-addr']))
      if ringXRAddress == None:
        self.log.info("Did not get ringmaster XML-RPC address. Fetching information from service registry.")
        ringList = serviceClient.getServiceInfo(self._cfg['userid'], 
            self._cfg['service-id'], 'ringmaster', 'hod')
      
        self.log.debug(pprint.pformat(ringList))
      
        if len(ringList):
          if isinstance(ringList, list):
            ringXRAddress = ringList[0]['xrs']
      
        count = 0
        while (ringXRAddress == None and count < 3000):
          ringList = serviceClient.getServiceInfo(self._cfg['userid'], 
            self._cfg['service-id'], 'ringmaster', 'hod')
        
          if len(ringList):
            if isinstance(ringList, list):
              ringXRAddress = ringList[0]['xrs']
        
          count = count + 1
          time.sleep(.2)
      
      if ringXRAddress == None:
        raise Exception("Could not get ringmaster XML-RPC server address.")
        
      self.log.debug("Creating ringmaster XML-RPC client.")
      ringClient = hodXRClient(ringXRAddress)    
      
      id = self.hostname + "_" + str(os.getpid())
      
      if 'download-addr' in self._cfg:
        self.__download_package(ringClient)
      else:
        self.log.debug("Did not find a download address.")
          
      cmdlist = []
      firstTime = True
      increment = 0
      hadoopStartupTime = 2
       
      cmdlist = ringClient.getCommand(id)

      while (cmdlist == []):
        if firstTime:
          sleepTime = increment + self._cfg['cmd-retry-initial-time'] + hadoopStartupTime\
                        + random.uniform(0,self._cfg['cmd-retry-interval'])
          firstTime = False
        else:
          sleepTime = increment + self._cfg['cmd-retry-initial-time'] + \
                        + random.uniform(0,self._cfg['cmd-retry-interval'])
        self.log.debug("Did not get command list. Waiting for %s seconds." % (sleepTime))
        time.sleep(sleepTime)
        increment = increment + 1
        cmdlist = ringClient.getCommand(id)

      self.log.debug(pformat(cmdlist)) 
      cmdDescs = []
      for cmds in cmdlist:
        cmdDescs.append(CommandDesc(cmds['dict'], self.log))
  
      self._cfg['commanddesc'] = cmdDescs
      
      self.log.info("Running hadoop commands...")

      self.__run_hadoop_commands(False)
        
      masterParams = []
      for k, cmd in self.__running.iteritems():
        masterParams.extend(cmd.filledInKeyVals)
  
      self.log.debug("printing getparams")
      self.log.debug(pformat(id))
      self.log.debug(pformat(masterParams))
      # when this is on a required host, the ringMaster already has our masterParams
      if(len(masterParams) > 0):
        ringClient.addMasterParams(id, masterParams)
    except Exception, e:
      raise Exception(e)

  def clusterStart(self, initialize=True):
    """Start a stopped mapreduce/dfs cluster"""
    if initialize:
      self.log.debug('clusterStart Method Invoked - Initialize')
    else:
      self.log.debug('clusterStart Method Invoked - No Initialize')
    try:
      self.log.debug("Creating service registry XML-RPC client.")
      serviceClient = hodXRClient(to_http_url(self._cfg['svcrgy-addr']),
                                  None, None, 0, 0, 0)

      self.log.info("Fetching ringmaster information from service registry.")
      count = 0
      ringXRAddress = None
      while (ringXRAddress == None and count < 3000):
        ringList = serviceClient.getServiceInfo(self._cfg['userid'],
          self._cfg['service-id'], 'ringmaster', 'hod')
        if len(ringList):
          if isinstance(ringList, list):
            ringXRAddress = ringList[0]['xrs']
        count = count + 1

      if ringXRAddress == None:
        raise Exception("Could not get ringmaster XML-RPC server address.")

      self.log.debug("Creating ringmaster XML-RPC client.")
      ringClient = hodXRClient(ringXRAddress, None, None, 0, 0, 0)

      id = self.hostname + "_" + str(os.getpid())

      cmdlist = []
      if initialize:
        if 'download-addr' in self._cfg:
          self.__download_package(ringClient)
        else:
          self.log.debug("Did not find a download address.")
        while (cmdlist == []):
          cmdlist = ringClient.getCommand(id)
      else:
        while (cmdlist == []):
          cmdlist = ringClient.getAdminCommand(id)

      self.log.debug(pformat(cmdlist))
      cmdDescs = []
      for cmds in cmdlist:
        cmdDescs.append(CommandDesc(cmds['dict'], self.log))

      self._cfg['commanddesc'] = cmdDescs

      if initialize:
        self.log.info("Running hadoop commands again... - Initialize")
        self.__run_hadoop_commands()
        masterParams = []
        for k, cmd in self.__running.iteritems():
          self.log.debug(cmd)
          masterParams.extend(cmd.filledInKeyVals)

        self.log.debug("printing getparams")
        self.log.debug(pformat(id))
        self.log.debug(pformat(masterParams))
        # when this is on a required host, the ringMaster already has our masterParams
        if(len(masterParams) > 0):
          ringClient.addMasterParams(id, masterParams)
      else:
        self.log.info("Running hadoop commands again... - No Initialize")
        self.__run_hadoop_commands()

    except:
      self.log.error(get_exception_string())

    return True

  def clusterStop(self):
    """Stop a running mapreduce/dfs cluster without stopping the hodring"""
    self.log.debug('clusterStop Method Invoked')
    try:
      for cmd in self.__running.values():
        cmd.kill()
      self.__running = {}
    except:
      self.log.error(get_exception_string())

    return True
