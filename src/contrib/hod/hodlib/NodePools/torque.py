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
"""Maui/Torque implementation of NodePool"""
# -*- python -*-

import os, sys, csv, socket, time, re, pprint

from hodlib.Hod.nodePool import *
from hodlib.Schedulers.torque import torqueInterface
from hodlib.Common.threads import simpleCommand
from hodlib.Common.util import get_exception_string, args_to_string, local_fqdn

class TorqueNodeSet(NodeSet):
  def __init__(self, id, numNodes, preferredList, isPreemptee):
    NodeSet.__init__(self, id, numNodes, preferredList, isPreemptee)
    self.qsubId = None
    self.addrList = []

  def _setQsubId(self, qsubId):
    self.qsubId = qsubId

  def _setAddrList(self, addrList):
    self.addrList = addrList

  def getAddrList(self):
    return self.addrList

class TorquePool(NodePool):
  def __init__(self, nodePoolDesc, cfg, log):
    NodePool.__init__(self, nodePoolDesc, cfg, log)

    environ = os.environ.copy()
    
    if self._cfg['resource_manager'].has_key('pbs-server'):
      environ['PBS_DEFAULT'] = self._cfg['resource_manager']['pbs-server']

    self.__torque = torqueInterface(
      self._cfg['resource_manager']['batch-home'], environ, self._log)

  def __gen_submit_params(self, nodeSet, walltime = None, qosLevel = None, 
                          account = None):
    argList = []
    stdinList = []
    
    npd = self.nodePoolDesc
    
    def gen_stdin_list():
      # Here we are basically generating the standard input for qsub.
      #  Specifically a script to exec ringmaster.
      stdinList.append('#!/bin/sh')
      
      ringBin = os.path.join(self._cfg['hod']['base-dir'], 'bin', 
                             'ringmaster')
      ringArgs = [ringBin,]
      ringArgs.extend(self._cfg.get_args(exclude=('hod')))
      
      ringMasterCommand = args_to_string(ringArgs)
      
      self._log.debug("ringmaster cmd: %s" % ringMasterCommand)
      
      stdinList.append(ringMasterCommand)
      
    def gen_arg_list():      
      def process_qsub_attributes():
        rawAttributes = self.nodePoolDesc.getAttrs()
    
        # 'W:x' is used to specify torque management extentensions ie -W x= ...
        resourceManagementExtensions = ''
        if 'W:x' in rawAttributes:
          resourceManagementExtensions = rawAttributes['W:x']
    
        if qosLevel:
          if len(resourceManagementExtensions) > 0:
            resourceManagementExtensions += ';'
          resourceManagementExtensions += 'QOS:%s' % (qosLevel)
    
        rawAttributes['W:x'] = resourceManagementExtensions
        
        hostname = local_fqdn()
    
        rawAttributes['l:nodes'] = nodeSet._getNumNodes()
        
        if walltime:
          rawAttributes['l:walltime'] = walltime
        
        #create a dict of dictionaries for 
        # various arguments of torque
        cmds = {}
        for key in rawAttributes:
          value = rawAttributes[key]
    
          if key.find(':') == -1:
            raise ValueError, 'Syntax error: missing colon after %s in %s=%s' % (
              key, key, value)
    
          [option, subOption] = key.split(':', 1)
          if not option in cmds:
            cmds[option] = {}
          cmds[option][subOption] = value
        
        opts = []
        #create a string from this
        #dictionary of dictionaries createde above
        for k in cmds:
          csv = []
          nv = cmds[k]
          for n in nv:
            v = nv[n]
            if len(n) == 0:
              csv.append(v)
            else:
              csv.append('%s=%s' % (n, v))
          opts.append('-%s' % (k))
          opts.append(','.join(csv))
    
        for option in cmds:
          commandList = []
          for subOption in cmds[option]:
            value = cmds[option][subOption]
            if len(subOption) == 0:
                commandList.append(value)
            else:
                commandList.append("%s=%s" % (subOption, value))
          opts.append('-%s' % option)
          opts.append(','.join(commandList))
          
        return opts
      
      pkgdir = npd.getPkgDir()
  
      qsub = os.path.join(pkgdir, 'bin', 'qsub')
      sdd = self._cfg['servicedesc']
      
      gsvc = None
      for key in sdd:
        gsvc = sdd[key]
        break
      
      argList.extend(process_qsub_attributes())
      argList.extend(('-N', 'HOD'))
      argList.extend(('-r','n'))

      if 'pbs-user' in self._cfg['resource_manager']:
        argList.extend(('-u', self._cfg['resource_manager']['pbs-user']))
  
      argList.extend(('-d','/tmp/'))
      if 'queue' in self._cfg['resource_manager']:
        queue = self._cfg['resource_manager']['queue']
        argList.extend(('-q',queue))
  
      # accounting should recognize userid:pbs-account as being "charged"
      argList.extend(('-A', (self._cfg['hod']['userid'] + ':' + 
                   self._cfg['resource_manager']['pbs-account'])))
    
      if 'env-vars' in self._cfg['resource_manager']:
        qsub_envs = self._cfg['resource_manager']['env-vars']
        argList.extend(('-v', self.__keyValToString(qsub_envs)))

    gen_arg_list()
    gen_stdin_list()
    
    return argList, stdinList
    
  def __keyValToString(self, keyValList):
    ret = ""
    for key in keyValList:
      ret = "%s%s=%s," % (ret, key, keyValList[key][0])
    return ret[:-1]
  
  def newNodeSet(self, numNodes, preferred=[], isPreemptee=True, id=None):
    if not id:
      id = self.getNextNodeSetId()
    
    nodeSet = TorqueNodeSet(id, numNodes, preferred, isPreemptee)

    self.nodeSetDict[nodeSet.getId()] = nodeSet
    
    return nodeSet
      
  def submitNodeSet(self, nodeSet, walltime = None, qosLevel = None, 
                    account = None):

    argList, stdinList = self.__gen_submit_params(nodeSet, walltime, qosLevel, 
                                                  account)
    
    jobId, exitCode = self.__torque.qsub(argList, stdinList)
    
    nodeSet.qsubId = jobId

    return jobId, exitCode

  def freeNodeSet(self, nodeSet):
    
    exitCode = self.deleteJob(nodeSet.getId())
    
    del self.nodeSetDict[nodeSet.getId()]
  
    return exitCode
  
  def finalize(self):
    status = 0
    exitCode = 0
    for nodeSet in self.nodeSetDict.values():
      exitCode = self.freeNodeSet(nodeSet)
      
    if exitCode > 0 and exitCode != 153:
      status = 4
      
    return status
    
  def getWorkers(self):
    hosts = []
    
    qstatInfo = self.__torque(self.getServiceId())
    if qstatInfo:
      hosts = qstatInfop['exec_host']
    
    return hosts
 
  def pollNodeSet(self, nodeSet):
    status = NodeSet.COMPLETE  
    nodeSet = self.nodeSetDict[0] 

    qstatInfo = self.__torque(self.getServiceId())

    if qstatMap:    
      jobstate = qstatMap['job_state']
      exechost = qstatMap['exec_host']

    if jobstate == 'Q':
      status = NodeSet.PENDING
    elif exechost == None:
      status = NodeSet.COMMITTED
    else:
      nodeSet._setAddrList(exec_host)

    return status
        
  def getServiceId(self):
    id = None
    
    nodeSets = self.nodeSetDict.values()
    if len(nodeSets):
      id = nodeSets[0].qsubId
      
    if id == None:
      id = os.getenv('PBS_JOBID')
      
    return id

  def getJobState(self):
    #torque error code when credentials fail, a temporary condition sometimes.
    credFailureErrorCode = 171 
    credFailureRetries = 10
    i = 0
    jobState = False
    
    while i < credFailureRetries:
      qstatInfo, exitCode = self.__torque.qstat(self.getServiceId())
      if exitCode == 0:
        jobState = qstatInfo['job_state'] 
        break
      else:
        if exitCode == credFailureErrorCode:
          time.sleep(1)
          i = i+1
        else:
          break
    return jobState

  def deleteJob(self, jobId):
    exitCode = self.__torque.qdel(jobId)
    return exitCode
    
  def runWorkers(self, args):
    return self.__torque.pbsdsh(args)



