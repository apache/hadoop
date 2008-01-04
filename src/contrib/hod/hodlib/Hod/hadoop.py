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
"""define WorkLoad as abstract interface for user job"""
# -*- python -*-

import os, time, sys, shutil, exceptions, re, threading, signal, urllib, pprint, math

from HTMLParser import HTMLParser

import xml.dom.minidom
import xml.dom.pulldom
from xml.dom import getDOMImplementation

from hodlib.Common.util import *
from hodlib.Common.xmlrpc import hodXRClient
from hodlib.Common.miniHTMLParser import miniHTMLParser
from hodlib.Common.nodepoolutil import NodePoolUtil
from hodlib.Common.tcp import tcpError, tcpSocket

reCommandDelimeterString = r"(?<!\\);"
reCommandDelimeter = re.compile(reCommandDelimeterString)

class hadoopConfig:
  def __create_xml_element(self, doc, name, value, description, final = False):
    prop = doc.createElement("property")
    nameP = doc.createElement("name")
    string = doc.createTextNode(name)
    nameP.appendChild(string)
    valueP = doc.createElement("value")
    string = doc.createTextNode(value)
    valueP.appendChild(string)
    if final:
      finalP = doc.createElement("final")
      string = doc.createTextNode("true")
      finalP.appendChild(string)
    desc = doc.createElement("description")
    string = doc.createTextNode(description)
    desc.appendChild(string)
    prop.appendChild(nameP)
    prop.appendChild(valueP)
    if final:
      prop.appendChild(finalP)
    prop.appendChild(desc)
    
    return prop

  def gen_site_conf(self, confDir, numNodes, hdfsAddr, mapredAddr=None,\
             clientParams=None, serverParams=None,\
             finalServerParams=None, clusterFactor=None):
    if not mapredAddr:
      mapredAddr = "dummy:8181"
    
    implementation = getDOMImplementation()
    doc = implementation.createDocument('', 'configuration', None)
    comment = doc.createComment(
      "This is an auto generated hadoop-site.xml, do not modify")
    topElement = doc.documentElement
    topElement.appendChild(comment)
    prop = self.__create_xml_element(doc, 'mapred.job.tracker', 
                                     mapredAddr, "description")
    topElement.appendChild(prop)
    prop = self.__create_xml_element(doc, 'fs.default.name', hdfsAddr, 
                                   "description")
    topElement.appendChild(prop)
    mapredAddrSplit = mapredAddr.split(":")
    mapredsystem = os.path.join('/mapredsystem', mapredAddrSplit[0])
    prop = self.__create_xml_element(doc, 'mapred.system.dir', mapredsystem, 
                                   "description", True )
    topElement.appendChild(prop)
    prop = self.__create_xml_element(doc, 'hadoop.tmp.dir', confDir, 
                                   "description")
    topElement.appendChild(prop)
    prop = self.__create_xml_element(doc, 'dfs.client.buffer.dir', 
                                     confDir, "description")
    topElement.appendChild(prop)

    # clientParams aer enabled now
    if clientParams:
      for k, v in clientParams.iteritems():
        prop = self.__create_xml_element(doc, k, v[0], "client param")
        topElement.appendChild(prop)

    # end

    # servelParams
    if serverParams:
      for k, v in serverParams.iteritems():
        prop = self.__create_xml_element(doc, k, v[0], "server param")
        topElement.appendChild(prop)

    # finalservelParams
    if finalServerParams:
      for k, v in finalServerParams.iteritems():
        prop = self.__create_xml_element(doc, k, v[0], "server param", True)
        topElement.appendChild(prop)

   
    # mapred-default.xml is no longer used now.
    numred = int(math.floor(clusterFactor * (int(numNodes) - 1)))
    prop = self.__create_xml_element(doc, "mapred.reduce.tasks", str(numred), 
                                 "description")
    topElement.appendChild(prop)
    # end

    siteName = os.path.join(confDir, "hadoop-site.xml")
    sitefile = file(siteName, 'w')
    print >> sitefile, topElement.toxml()
    sitefile.close()

class hadoopCluster:
  def __init__(self, cfg, log):
    self.__cfg = cfg
    self.__log = log
    self.__changedClusterParams = []
    
    self.__hostname = local_fqdn()    
    self.__svcrgyClient = None
    self.__nodePool = NodePoolUtil.getNodePool(self.__cfg['nodepooldesc'], 
                                               self.__cfg, self.__log)        
    self.__hadoopCfg = hadoopConfig()
    self.jobId = None
    self.mapredInfo = None
    self.hdfsInfo = None
    self.ringmasterXRS = None

  def __get_svcrgy_client(self):
    svcrgyUrl = to_http_url(self.__cfg['hod']['xrs-address'])
    return hodXRClient(svcrgyUrl)

  def __get_service_status(self):
    serviceData = self.__get_service_data()
    
    status = True
    hdfs = False
    mapred = False
    
    for host in serviceData.keys():
      for item in serviceData[host]:
        service = item.keys()
        if service[0] == 'hdfs.grid' and \
          self.__cfg['gridservice-hdfs']['external'] == False:
          hdfs = True
        elif service[0] == 'mapred.grid':
          mapred = True
    
    if not mapred:
      status = "mapred"
    
    if not hdfs and self.__cfg['gridservice-hdfs']['external'] == False:
      if status != True:
        status = "mapred and hdfs"
      else:
        status = "hdfs"
      
    return status
  
  def __get_service_data(self):
    registry = to_http_url(self.__cfg['hod']['xrs-address'])
    serviceData = self.__svcrgyClient.getServiceInfo(
      self.__cfg['hod']['userid'], self.__setup.np.getNodePoolId())
    
    return serviceData
  
  def __check_allocation_manager(self):
    userValid = True
    try:
      self.serviceProxyClient = hodXRClient(
        to_http_url(self.__cfg['hod']['proxy-xrs-address']), None, None, 0,
        0, 1, False, 15)
      
      userValid = self.serviceProxyClient.isProjectUserValid(
        self.__setup.cfg['hod']['userid'], 
        self.__setup.cfg['resource_manager']['pbs-account'],True)
      
      if userValid:
        self.__log.debug("Validated that user %s is part of project %s." %
          (self.__cfg['hod']['userid'], 
           self.__cfg['resource_manager']['pbs-account']))
      else:
        self.__log.debug("User %s is not part of project: %s." % (
          self.__cfg['hod']['userid'], 
          self.__cfg['resource_manager']['pbs-account']))
        self.__log.error("Please specify a valid project in "
                      + "resource_manager.pbs-account. If you still have "
                      + "issues, please contact operations")
        userValidd = False
        # ignore invalid project for now - TODO
    except Exception:
      # ignore failures - non critical for now
      self.__log.debug(
        "Unable to contact Allocation Manager Proxy - ignoring...")
      #userValid = False
        
    return userValid

  def __check_job_status(self):
    initWaitCount = 20
    count = 0
    status = False
    state = 'Q'
    while state == 'Q':
      state = self.__nodePool.getJobState()
      if (state==False) or (state!='Q'):
        break
      count = count + 1
      if count < initWaitCount:
        time.sleep(0.5)
      else:
        time.sleep(10)
    
    if state and state != 'C':
      status = True
    
    return status
  
  def __get_ringmaster_client(self):
    ringmasterXRS = None
   
    ringList = self.__svcrgyClient.getServiceInfo(
      self.__cfg['ringmaster']['userid'], self.__nodePool.getServiceId(), 
      'ringmaster', 'hod')

    if ringList and len(ringList):
      if isinstance(ringList, list):
        ringmasterXRS = ringList[0]['xrs']
    else:    
      count = 0
      waitTime = self.__cfg['hod']['allocate-wait-time']
  
      while count < waitTime:
        ringList = self.__svcrgyClient.getServiceInfo(
          self.__cfg['ringmaster']['userid'], self.__nodePool.getServiceId(), 
          'ringmaster', 
          'hod')
        
        if ringList and len(ringList):
          if isinstance(ringList, list):        
            ringmasterXRS = ringList[0]['xrs']
        
        if ringmasterXRS is not None:
          break
        else:
          time.sleep(1)
          count = count + 1
          # check to see if the job exited by any chance in that time:
          if (count % 10 == 0):
            if not self.__check_job_status():
              break

    return ringmasterXRS
 
  def __init_hadoop_service(self, serviceName, xmlrpcClient):
    status = True
    serviceAddress = None
    serviceInfo = None
 
    for i in range(0, 250):
      try:
        serviceAddress = xmlrpcClient.getServiceAddr(serviceName)
        if serviceAddress:
          if serviceAddress == 'not found':
            time.sleep(.5)
          # check to see if the job exited by any chance in that time:
            if (i % 10 == 0):
              if not self.__check_job_status():
                break
          else:
            serviceInfo = xmlrpcClient.getURLs(serviceName)           
            break 
      except:
        self.__log.critical("'%s': ringmaster xmlrpc error." % serviceName)
        self.__log.debug(get_exception_string())
        status = False
        break
    
    if serviceAddress == 'not found' or not serviceAddress:
      self.__log.critical("Failed to retrieve '%s' service address." % 
                          serviceName)
      status = False
    else:
      try:
        self.__svcrgyClient.registerService(self.__cfg['hodring']['userid'], 
                                            self.jobId, self.__hostname, 
                                            serviceName, 'grid', serviceInfo)
        
      except:
        self.__log.critical("'%s': registry xmlrpc error." % serviceName)    
        self.__log.debug(get_exception_string())
        status = False
        
    return status, serviceAddress, serviceInfo

  def __collect_jobtracker_ui(self, dir):

     link = self.mapredInfo + "/jobtracker.jsp"
     parser = miniHTMLParser()
     parser.setBaseUrl(self.mapredInfo)
     node_cache = {}

     self.__log.debug("collect_jobtracker_ui seeded with " + link)

     def alarm_handler(number, stack):
         raise AlarmException("timeout")
       
     signal.signal(signal.SIGALRM, alarm_handler)

     input = None
     while link:
       self.__log.debug("link: %s" % link)
       # taskstats.jsp,taskdetails.jsp not included since too many to collect
       if re.search(
         "jobfailures\.jsp|jobtracker\.jsp|jobdetails\.jsp|jobtasks\.jsp", 
         link):

         for i in range(1,5):
           try:
             input = urllib.urlopen(link)
             break
           except:
             self.__log.debug(get_exception_string())
             time.sleep(1)
  
         if input:
           out = None
    
           self.__log.debug("collecting " + link + "...")
           filename = re.sub(self.mapredInfo, "", link)
           filename = dir + "/"  + filename
           filename = re.sub("http://","", filename)
           filename = re.sub("[\?\&=:]","_",filename)
           filename = filename + ".html"
    
           try:
             tempdir, tail = os.path.split(filename)
             if not os.path.exists(tempdir):
               os.makedirs(tempdir)
           except:
             self.__log.debug(get_exception_string())
    
           out = open(filename, 'w')
           
           bufSz = 8192
           
           signal.alarm(10)
           
           try:
             self.__log.debug("Starting to grab: %s" % link)
             buf = input.read(bufSz)
      
             while len(buf) > 0:
               # Feed the file into the HTML parser
               parser.feed(buf)
        
         # Re-write the hrefs in the file
               p = re.compile("\?(.+?)=(.+?)")
               buf = p.sub(r"_\1_\2",buf)
               p= re.compile("&(.+?)=(.+?)")
               buf = p.sub(r"_\1_\2",buf)
               p = re.compile("http://(.+?):(\d+)?")
               buf = p.sub(r"\1_\2/",buf)
               buf = re.sub("href=\"/","href=\"",buf)
               p = re.compile("href=\"(.+?)\"")
               buf = p.sub(r"href=\1.html",buf)
 
               out.write(buf)
               buf = input.read(bufSz)
      
             signal.alarm(0)
             input.close()
             if out:
               out.close()
               
             self.__log.debug("Finished grabbing: %s" % link)
           except AlarmException:
             if out: out.close()
             if input: input.close()
             
             self.__log.debug("Failed to retrieve: %s" % link)
         else:
           self.__log.debug("Failed to retrieve: %s" % link)
         
       # Get the next link in level traversal order
       link = parser.getNextLink()

     parser.close()
     
  def check_cluster(self, clusterInfo):
    status = 0

    if 'mapred' in clusterInfo:
      mapredAddress = clusterInfo['mapred'][7:]
      hdfsAddress = clusterInfo['hdfs'][7:]
  
      mapredSocket = tcpSocket(mapredAddress)
        
      try:
        mapredSocket.open()
        mapredSocket.close()
      except tcpError:
        status = 14
  
      hdfsSocket = tcpSocket(hdfsAddress)
        
      try:
        hdfsSocket.open()
        hdfsSocket.close()
      except tcpError:
        if status > 0:
          status = 10
        else:
          status = 13
      
      if status == 0:
        status = 12
    else:
      status = 15
      
    return status
  
  def cleanup(self):
    if self.__nodePool: self.__nodePool.finalize()     

  def get_job_id(self):
    return self.jobId

  def delete_job(self, jobId):
    '''Delete a job given it's ID'''
    ret = 0
    if self.__nodePool: 
      ret = self.__nodePool.deleteJob(jobId)
    else:
      raise Exception("Invalid state: Node pool is not initialized to delete the given job.")
    return ret
         
  def allocate(self, clusterDir, min, max=None):
    status = 0  
    self.__svcrgyClient = self.__get_svcrgy_client()
        
    self.__log.debug("allocate %s %s %s" % (clusterDir, min, max))
    
    if min < 3:
      self.__log.critical("Minimum nodes must be greater than 2.")
      status = 2
    else:
      if self.__check_allocation_manager():
        nodeSet = self.__nodePool.newNodeSet(min)
        self.jobId, exitCode = self.__nodePool.submitNodeSet(nodeSet)
        if self.jobId:                 
          if self.__check_job_status():
            self.ringmasterXRS = self.__get_ringmaster_client()
            if self.ringmasterXRS:
              ringClient =  hodXRClient(self.ringmasterXRS)
              
              hdfsStatus, hdfsAddr, self.hdfsInfo = \
                self.__init_hadoop_service('hdfs', ringClient)
              
              if hdfsStatus:
                mapredStatus, mapredAddr, self.mapredInfo = \
                  self.__init_hadoop_service('mapred', ringClient)
                  
                if mapredStatus:
                  self.__log.info("HDFS UI on http://%s" % self.hdfsInfo)
                  self.__log.info("Mapred UI on http://%s" % self.mapredInfo)
 
                  # Go generate the client side hadoop-site.xml now
                  # adding final-params as well, just so that conf on 
                  # client-side and server-side are (almost) the same
                  clientParams = None
                  serverParams = {}
                  finalServerParams = {}

                  # client-params
                  if self.__cfg['hod'].has_key('client-params'):
                    clientParams = self.__cfg['hod']['client-params']

                  # server-params
                  if self.__cfg['gridservice-mapred'].has_key('server-params'):
                    serverParams.update(\
                      self.__cfg['gridservice-mapred']['server-params'])
                  if self.__cfg['gridservice-hdfs'].has_key('server-params'):
                    # note that if there are params in both mapred and hdfs
                    # sections, the ones in hdfs overwirte the ones in mapred
                    serverParams.update(\
                        self.__cfg['gridservice-mapred']['server-params'])
                  
                  # final-server-params
                  if self.__cfg['gridservice-mapred'].has_key(\
                                                    'final-server-params'):
                    finalServerParams.update(\
                      self.__cfg['gridservice-mapred']['final-server-params'])
                  if self.__cfg['gridservice-hdfs'].has_key(
                                                    'final-server-params'):
                    finalServerParams.update(\
                        self.__cfg['gridservice-hdfs']['final-server-params'])

                  clusterFactor = self.__cfg['hod']['cluster-factor']
                  self.__hadoopCfg.gen_site_conf(clusterDir, min,
                            hdfsAddr, mapredAddr, clientParams,\
                            serverParams, finalServerParams,\
                            clusterFactor)
                  # end of hadoop-site.xml generation
                else:
                  status = 8
              else:
                status = 7  
            else:
              status = 6
            if status != 0:
              self.__log.info("Cleaning up job id %s, as cluster could not be allocated." % self.jobId)
              self.delete_job(self.jobId)
          else:
            self.__log.critical("No job found, ringmaster failed to run.")
            status = 5 
 
        elif self.jobId == False:
          if exitCode == 188:
            self.__log.critical("Request execeeded maximum resource allocation.")
          else:
            self.__log.critical("Insufficient resources available.")
          status = 4
        else:    
          self.__log.critical("Scheduler failure, allocation failed.\n\n")        
          status = 4
      else:
        status = 9
    
    return status

  def deallocate(self, clusterDir, clusterInfo):
    status = 0 
    
    nodeSet = self.__nodePool.newNodeSet(clusterInfo['min'], 
                                         id=clusterInfo['jobid'])
    self.mapredInfo = clusterInfo['mapred']
    self.hdfsInfo = clusterInfo['hdfs']
    try:
      if self.__cfg['hod'].has_key('hadoop-ui-log-dir'):
        clusterStatus = self.check_cluster(clusterInfo)
        if clusterStatus != 14 and clusterStatus != 10:   
          # If JT is still alive
          self.__collect_jobtracker_ui(self.__cfg['hod']['hadoop-ui-log-dir'])
      else:
        self.__log.debug('hadoop-ui-log-dir not specified. Skipping Hadoop UI log collection.')
    except:
      self.__log.info("Exception in collecting Job tracker logs. Ignoring.")
    status = self.__nodePool.finalize()
   
    return status
  
class hadoopScript:
  def __init__(self, conf, execDir):
    self.__environ = os.environ.copy()
    self.__environ['HADOOP_CONF_DIR'] = conf
    self.__execDir = execDir
    
  def run(self, script):
    scriptThread = simpleCommand(script, script, self.__environ, 4, False, 
                                 False, self.__execDir)
    scriptThread.start()
    scriptThread.wait()
    scriptThread.join()
    
    return scriptThread.exit_code()
