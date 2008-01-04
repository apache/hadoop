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
"""define Hdfs as subclass of Service"""

# -*- python -*-

import os

from service import *
from hodlib.Hod.nodePool import *
from hodlib.Common.desc import CommandDesc
from hodlib.Common.util import get_exception_string

class HdfsExternal(MasterSlave):
  """dummy proxy to external HDFS instance"""

  def __init__(self, serviceDesc, workDirs):
    MasterSlave.__init__(self, serviceDesc, workDirs,None)
    self.launchedMaster = True
    self.masterInitialized = True
    
  def getMasterRequest(self):
    return None

  def getMasterCommands(self, serviceDict):
    return []

  def getAdminCommands(self, serviceDict):
    return []

  def getWorkerCommands(self, serviceDict):
    return []

  def getMasterAddrs(self):
    attrs = self.serviceDesc.getfinalAttrs()
    addr = attrs['fs.default.name']
    return [addr]
  
  def setMasterParams(self, list):
    raise NotImplementedError

  def getInfoAddrs(self):
    attrs = self.serviceDesc.getfinalAttrs()
    addr = attrs['fs.default.name']
    k,v = addr.split( ":")
    # infoaddr = k + ':' + attrs['dfs.info.port']
    # After Hadoop-2185
    infoaddr = attrs['dfs.http.bindAddress']
    return [infoaddr]

class Hdfs(MasterSlave):

  def __init__(self, serviceDesc, nodePool, required_node, format=True, upgrade=False):
    MasterSlave.__init__(self, serviceDesc, nodePool, required_node)
    self.masterNode = None
    self.masterAddr = None
    self.runAdminCommands = True
    self.infoAddr = None
    self._isLost = False
    self.format = format
    self.upgrade = upgrade
    self.workers = []

  def getMasterRequest(self):
    req = NodeRequest(1, [], False)
    return req

  def getMasterCommands(self, serviceDict):

    masterCommands = []
    if self.format:
      masterCommands.append(self._getNameNodeCommand(True))

    if self.upgrade:
      masterCommands.append(self._getNameNodeCommand(False, True))
    else:
      masterCommands.append(self._getNameNodeCommand(False))

    return masterCommands

  def getAdminCommands(self, serviceDict):

    adminCommands = []
    if self.upgrade and self.runAdminCommands:
      adminCommands.append(self._getNameNodeAdminCommand('-safemode wait'))
      adminCommands.append(self._getNameNodeAdminCommand('-finalizeUpgrade',
                                                          True, True))

    self.runAdminCommands = False
    return adminCommands

  def getWorkerCommands(self, serviceDict):
    cmdDesc = self._getDataNodeCommand()
    return [cmdDesc]

  def setMasterNodes(self, list):
    node = list[0]
    self.masterNode = node
    
  def getMasterAddrs(self):
    return [self.masterAddr]

  def getInfoAddrs(self):
    return [self.infoAddr]

  def getWorkers(self):
    return self.workers

  def setMasterParams(self, list):
    dict = self._parseEquals(list)
    self.masterAddr = dict['fs.default.name']
    k,v = self.masterAddr.split( ":")
    self.masterNode = k
    # self.infoAddr = self.masterNode + ':' + dict['dfs.info.port']
    # After Hadoop-2185
    self.infoAddr = dict['dfs.http.bindAddress']
   
  def _parseEquals(self, list):
    dict = {}
    for elems in list:
      splits = elems.split('=')
      dict[splits[0]] = splits[1]
    return dict
  
  def _getNameNodePort(self):
    sd = self.serviceDesc
    attrs = sd.getfinalAttrs()
    if not 'fs.default.name' in attrs:
      return ServiceUtil.getUniqPort()

    v = attrs['fs.default.name']
    try:
      [n, p] = v.split(':', 1)
      return int(p)
    except:
      print get_exception_string()
      raise ValueError, "Can't find port from attr fs.default.name: %s" % (v)

  def _getNameNodeInfoPort(self):
    sd = self.serviceDesc
    attrs = sd.getfinalAttrs()
    if 'dfs.http.bindAddress' not in attrs:
      return ServiceUtil.getUniqPort()

    # p = attrs['dfs.info.port'] 
    p = attrs['dfs.http.bindAddress'].split(':')[1]
    try:
      return int(p)
    except:
      print get_exception_string()
      raise ValueError, "Can't find port from attr dfs.info.port: %s" % (p)

  def _setWorkDirs(self, workDirs, envs, attrs, parentDirs, subDir):
    namedir = None
    datadir = []

    for p in parentDirs:
      workDirs.append(p)
      workDirs.append(os.path.join(p, subDir))
      dir = os.path.join(p, subDir, 'dfs-data')
      datadir.append(dir)

      if not namedir:
        namedir = os.path.join(p, subDir, 'dfs-name')

    workDirs.append(namedir)
    workDirs.extend(datadir)

    # FIXME!! use csv
    attrs['dfs.name.dir'] = namedir
    attrs['dfs.data.dir'] = ','.join(datadir)
    # FIXME -- change dfs.client.buffer.dir
    envs['HADOOP_ROOT_LOGGER'] = ["INFO,DRFA",]


  def _getNameNodeCommand(self, format=False, upgrade=False):
    sd = self.serviceDesc

    parentDirs = self.workDirs
    workDirs = []
    attrs = sd.getfinalAttrs().copy()
    envs = sd.getEnvs().copy()
    #self.masterPort = port = self._getNameNodePort()
    
    if 'fs.default.name' not in attrs:
      attrs['fs.default.name'] = 'fillinhostport'
    #self.infoPort = port = self._getNameNodeInfoPort()
 
    # if 'dfs.info.port' not in attrs:
    #  attrs['dfs.info.port'] = 'fillinport'
   
    # Addressing Hadoop-2815, added the following. Earlier version don't
    # care about this
    if 'dfs.http.bindAddress' not in attrs:
      attrs['dfs.http.bindAddress'] = 'fillinhostport'

    self._setWorkDirs(workDirs, envs, attrs, parentDirs, 'hdfs-nn')

    dict = { 'name' : 'namenode' }
    dict['program'] = os.path.join('bin', 'hadoop')
    argv = ['namenode']
    if format:
      argv.append('-format')
    elif upgrade:
      argv.append('-upgrade')
    dict['argv'] = argv
    dict['envs'] = envs
    dict['pkgdirs'] = sd.getPkgDirs()
    dict['workdirs'] = workDirs
    dict['final-attrs'] = attrs
    dict['attrs'] = sd.getAttrs()
    if format:
      dict['fg'] = 'true'
      dict['stdin'] = 'Y'
    cmd = CommandDesc(dict)
    return cmd

  def _getNameNodeAdminCommand(self, adminCommand, wait=True, ignoreFailures=False):
    sd = self.serviceDesc

    parentDirs = self.workDirs
    workDirs = []
    attrs = sd.getfinalAttrs().copy()
    envs = sd.getEnvs().copy()
    nn = self.masterAddr

    if nn == None:
      raise ValueError, "Can't get namenode address"

    attrs['fs.default.name'] = nn

    self._setWorkDirs(workDirs, envs, attrs, parentDirs, 'hdfs-nn')

    dict = { 'name' : 'dfsadmin' }
    dict['program'] = os.path.join('bin', 'hadoop')
    argv = ['dfsadmin']
    argv.append(adminCommand)
    dict['argv'] = argv
    dict['envs'] = envs
    dict['pkgdirs'] = sd.getPkgDirs()
    dict['workdirs'] = workDirs
    dict['final-attrs'] = attrs
    dict['attrs'] = sd.getAttrs()
    if wait:
      dict['fg'] = 'true'
      dict['stdin'] = 'Y'
    if ignoreFailures:
      dict['ignorefailures'] = 'Y'
    cmd = CommandDesc(dict)
    return cmd
 
  def _getDataNodeCommand(self):

    sd = self.serviceDesc

    parentDirs = self.workDirs
    workDirs = []
    attrs = sd.getfinalAttrs().copy()
    envs = sd.getEnvs().copy()
    nn = self.masterAddr

    if nn == None:
      raise ValueError, "Can't get namenode address"

    attrs['fs.default.name'] = nn

    # Adding the following. Hadoop-2815
    if 'dfs.datanode.bindAddress' not in attrs:
      attrs['dfs.datanode.bindAddress'] = 'fillinhostport'
    if 'dfs.datanode.http.bindAddress' not in attrs:
      attrs['dfs.datanode.http.bindAddress'] = 'fillinhostport'
    self._setWorkDirs(workDirs, envs, attrs, parentDirs, 'hdfs-dn')

    dict = { 'name' : 'datanode' }
    dict['program'] = os.path.join('bin', 'hadoop')
    dict['argv'] = ['datanode']
    dict['envs'] = envs
    dict['pkgdirs'] = sd.getPkgDirs()
    dict['workdirs'] = workDirs
    dict['final-attrs'] = attrs
    dict['attrs'] = sd.getAttrs()

    cmd = CommandDesc(dict)
    return cmd

