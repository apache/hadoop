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
import sys, os, traceback, stat, socket, re, warnings

from hodlib.Common.tcp import tcpSocket, tcpError 
from hodlib.Common.threads import simpleCommand

setUGV   = { 'S_ISUID' : 2, 'S_ISGID' : 1, 'S_ISVTX' : 0 }

class AlarmException(Exception):
    def __init__(self, msg=''):
        self.message = msg
        Exception.__init__(self, msg)

    def __repr__(self):
        return self.message

def untar(file, targetDir):
    status = False
    command = 'tar -C %s -zxf %s' % (targetDir, file)
    commandObj = simpleCommand('untar', command)
    commandObj.start()
    commandObj.wait()
    commandObj.join()
    if commandObj.exit_code() == 0:
        status = True
        
    return status

def tar(tarFile, tarDirectory, tarList):
    currentDir = os.getcwd()
    os.chdir(tarDirectory)
    status = False
    command = 'tar -czf %s ' % (tarFile)

    for file in tarList:
        command = "%s%s " % (command, file)
    
    commandObj = simpleCommand('tar', command)
    commandObj.start()
    commandObj.wait()
    commandObj.join()
    if commandObj.exit_code() == 0:
        status = True
    else:
        status = commandObj.exit_status_string()
    
    os.chdir(currentDir)
        
    return status
  
def to_http_url(list):
    """convert [hostname, port]  to a http url""" 
    str = ''
    str = "http://%s:%s" % (list[0], list[1])
    
    return str

def get_exception_string():
    (type, value, tb) = sys.exc_info()
    exceptList = traceback.format_exception(type, value, tb)
    exceptString = ''
    for line in exceptList:
        exceptString = "%s%s" % (exceptString, line)
    
    return exceptString
  
def get_exception_error_string():
  (type, value, tb) = sys.exc_info()
  if value:
    exceptString = "%s %s" % (type, value)
  else:
    exceptString = type
    
  return exceptString

def check_timestamp(timeStamp):
    """ Checks the validity of a timeStamp.

        timeStamp - (YYYY-MM-DD HH:MM:SS in UTC)

        returns True or False
    """
    isValid = True

    try:
        timeStruct = time.strptime(timeStamp, "%Y-%m-%d %H:%M:%S")
    except:
        isValid = False

    return isValid

def sig_wrapper(sigNum, handler, *args):
  if args:
      handler(args)
  else:
      handler()
      
def get_perms(filename):
    mode = stat.S_IMODE(os.stat(filename)[stat.ST_MODE])
    permsString = ''
    permSet = 0
    place = 2
    for who in "USR", "GRP", "OTH":
        for what in "R", "W", "X":
            if mode & getattr(stat,"S_I"+what+who):
                permSet = permSet + 2**place
            place = place - 1

        permsString = "%s%s" % (permsString, permSet)
        permSet = 0
        place = 2

    permSet = 0
    for permFlag in setUGV.keys():
        if mode & getattr(stat, permFlag):
            permSet = permSet + 2**setUGV[permFlag]

    permsString = "%s%s" % (permSet, permsString)

    return permsString

def local_fqdn():
    """Return a system's true FQDN rather than any aliases, which are
       occasionally returned by socket.gethostname."""

    fqdn = None
    me = os.uname()[1]
    nameInfo=socket.gethostbyname_ex(me)
    nameInfo[1].append(nameInfo[0])
    for name in nameInfo[1]:
        if name.count(".") and name.startswith(me):
            fqdn = name

    return(fqdn)
  
def need_to_allocate(allocated, config, command):
    status = True
    
    if allocated.isSet():
        status = False
    elif re.search("\s*dfs.*$", command) and \
        config['gridservice-hdfs']['external']:    
        status = False
    elif config['gridservice-mapred']['external']:    
        status = False
        
    return status
  
def filter_warnings():
    warnings.filterwarnings('ignore',
        message=".*?'with' will become a reserved keyword.*")
    
def args_to_string(list):
  """return a string argument space seperated"""
  arg = ''
  for item in list:
    arg = "%s%s " % (arg, item)
  return arg[:-1]
