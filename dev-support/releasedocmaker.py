#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from glob import glob
from optparse import OptionParser
from time import gmtime, strftime
import os
import re
import sys
import urllib
try:
  import json
except ImportError:
  import simplejson as json

releaseVersion={}
namePattern = re.compile(r' \([0-9]+\)')

asflicense='''
<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->
'''

def clean(str):
  return tableclean(re.sub(namePattern, "", str))

def formatComponents(str):
  str = re.sub(namePattern, '', str).replace("'", "")
  if str != "":
    ret = str
  else:
    # some markdown parsers don't like empty tables
    ret = "."
  return clean(ret)

# convert to utf-8
# protect some known md metachars
# or chars that screw up doxia
def tableclean(str):
  str=str.encode('utf-8')
  str=str.replace("_","\_")
  str=str.replace("\r","")
  str=str.rstrip()
  return str

# same thing as tableclean,
# except table metachars are also
# escaped as well as more
# things we don't want doxia to
# screw up
def notableclean(str):
  str=tableclean(str)
  str=str.replace("|","\|")
  str=str.replace("<","\<")
  str=str.replace(">","\>")
  str=str.replace("*","\*")
  str=str.rstrip()
  return str

def mstr(obj):
  if (obj == None):
    return ""
  return unicode(obj)

def buildindex(master):
  versions=reversed(sorted(glob("[0-9]*.[0-9]*.[0-9]*")))
  with open("index.md","w") as indexfile:
    for v in versions:
      indexfile.write("* Apache Hadoop v%s\n" % (v))
      for k in ("Changes","Release Notes"):
        indexfile.write("    *  %s\n" %(k))
        indexfile.write("        * [Combined %s](%s/%s.%s.html)\n" \
          % (k,v,k.upper().replace(" ",""),v))
        if not master:
          indexfile.write("        * [Hadoop Common %s](%s/%s.HADOOP.%s.html)\n" \
            % (k,v,k.upper().replace(" ",""),v))
          for p in ("HDFS","MapReduce","YARN"):
            indexfile.write("        * [%s %s](%s/%s.%s.%s.html)\n" \
              % (p,k,v,k.upper().replace(" ",""),p.upper(),v))
  indexfile.close()

class Version:
  """Represents a version number"""
  def __init__(self, data):
    self.mod = False
    self.data = data
    found = re.match('^((\d+)(\.\d+)*).*$', data)
    if (found):
      self.parts = [ int(p) for p in found.group(1).split('.') ]
    else:
      self.parts = []
    # backfill version with zeroes if missing parts
    self.parts.extend((0,) * (3 - len(self.parts)))

  def __str__(self):
    if (self.mod):
      return '.'.join([ str(p) for p in self.parts ])
    return self.data

  def __cmp__(self, other):
    return cmp(self.parts, other.parts)

class Jira:
  """A single JIRA"""

  def __init__(self, data, parent):
    self.key = data['key']
    self.fields = data['fields']
    self.parent = parent
    self.notes = None
    self.incompat = None
    self.reviewed = None

  def getId(self):
    return mstr(self.key)

  def getDescription(self):
    return mstr(self.fields['description'])

  def getReleaseNote(self):
    if (self.notes == None):
      field = self.parent.fieldIdMap['Release Note']
      if (self.fields.has_key(field)):
        self.notes=mstr(self.fields[field])
      else:
        self.notes=self.getDescription()
    return self.notes

  def getPriority(self):
    ret = ""
    pri = self.fields['priority']
    if(pri != None):
      ret = pri['name']
    return mstr(ret)

  def getAssignee(self):
    ret = ""
    mid = self.fields['assignee']
    if(mid != None):
      ret = mid['displayName']
    return mstr(ret)

  def getComponents(self):
    if (len(self.fields['components'])>0):
      return ", ".join([ comp['name'] for comp in self.fields['components'] ])
    else:
      return ""

  def getSummary(self):
    return self.fields['summary']

  def getType(self):
    ret = ""
    mid = self.fields['issuetype']
    if(mid != None):
      ret = mid['name']
    return mstr(ret)

  def getReporter(self):
    ret = ""
    mid = self.fields['reporter']
    if(mid != None):
      ret = mid['displayName']
    return mstr(ret)

  def getProject(self):
    ret = ""
    mid = self.fields['project']
    if(mid != None):
      ret = mid['key']
    return mstr(ret)

  def __cmp__(self,other):
    selfsplit=self.getId().split('-')
    othersplit=other.getId().split('-')
    v1=cmp(selfsplit[0],othersplit[0])
    if (v1!=0):
      return v1
    else:
      if selfsplit[1] < othersplit[1]:
        return True
      elif selfsplit[1] > othersplit[1]:
        return False
    return False

  def getIncompatibleChange(self):
    if (self.incompat == None):
      field = self.parent.fieldIdMap['Hadoop Flags']
      self.reviewed=False
      self.incompat=False
      if (self.fields.has_key(field)):
        if self.fields[field]:
          for hf in self.fields[field]:
            if hf['value'] == "Incompatible change":
              self.incompat=True
            if hf['value'] == "Reviewed":
              self.reviewed=True
    return self.incompat

  def getReleaseDate(self,version):
    for j in range(len(self.fields['fixVersions'])):
      if self.fields['fixVersions'][j]==version:
        return(self.fields['fixVersions'][j]['releaseDate'])
    return None

class JiraIter:
  """An Iterator of JIRAs"""

  def __init__(self, versions):
    self.versions = versions

    resp = urllib.urlopen("https://issues.apache.org/jira/rest/api/2/field")
    data = json.loads(resp.read())

    self.fieldIdMap = {}
    for part in data:
      self.fieldIdMap[part['name']] = part['id']

    self.jiras = []
    at=0
    end=1
    count=100
    while (at < end):
      params = urllib.urlencode({'jql': "project in (HADOOP,HDFS,MAPREDUCE,YARN) and fixVersion in ('"+"' , '".join([str(v).replace("-SNAPSHOT","") for v in versions])+"') and resolution = Fixed", 'startAt':at, 'maxResults':count})
      resp = urllib.urlopen("https://issues.apache.org/jira/rest/api/2/search?%s"%params)
      data = json.loads(resp.read())
      if (data.has_key('errorMessages')):
        raise Exception(data['errorMessages'])
      at = data['startAt'] + data['maxResults']
      end = data['total']
      self.jiras.extend(data['issues'])

      needaversion=False
      for j in versions:
        v=str(j).replace("-SNAPSHOT","")
        if v not in releaseVersion:
          needaversion=True

      if needaversion is True:
        for i in range(len(data['issues'])):
          for j in range(len(data['issues'][i]['fields']['fixVersions'])):
            if 'releaseDate' in data['issues'][i]['fields']['fixVersions'][j]:
              releaseVersion[data['issues'][i]['fields']['fixVersions'][j]['name']]=\
                             data['issues'][i]['fields']['fixVersions'][j]['releaseDate']

    self.iter = self.jiras.__iter__()

  def __iter__(self):
    return self

  def next(self):
    data = self.iter.next()
    j = Jira(data, self)
    return j

class Outputs:
  """Several different files to output to at the same time"""

  def __init__(self, base_file_name, file_name_pattern, keys, params={}):
    self.params = params
    self.base = open(base_file_name%params, 'w')
    self.others = {}
    for key in keys:
      both = dict(params)
      both['key'] = key
      self.others[key] = open(file_name_pattern%both, 'w')

  def writeAll(self, pattern):
    both = dict(self.params)
    both['key'] = ''
    self.base.write(pattern%both)
    for key in self.others.keys():
      both = dict(self.params)
      both['key'] = key
      self.others[key].write(pattern%both)

  def writeKeyRaw(self, key, str):
    self.base.write(str)
    if (self.others.has_key(key)):
      self.others[key].write(str)

  def close(self):
    self.base.close()
    for fd in self.others.values():
      fd.close()

  def writeList(self, mylist):
    for jira in sorted(mylist):
      line = '| [%s](https://issues.apache.org/jira/browse/%s) | %s |  %s | %s | %s | %s |\n' \
        % (notableclean(jira.getId()), notableclean(jira.getId()),
           notableclean(jira.getSummary()),
           notableclean(jira.getPriority()),
           formatComponents(jira.getComponents()),
           notableclean(jira.getReporter()),
           notableclean(jira.getAssignee()))
      self.writeKeyRaw(jira.getProject(), line)

def main():
  parser = OptionParser(usage="usage: %prog --version VERSION [--version VERSION2 ...]",
		epilog=
               "Markdown-formatted CHANGES and RELEASENOTES files will be stored in a directory"
               " named after the highest version provided.")
  parser.add_option("-v", "--version", dest="versions",
             action="append", type="string",
             help="versions in JIRA to include in releasenotes", metavar="VERSION")
  parser.add_option("-m","--master", dest="master", action="store_true",
             help="only create the master, merged project files")
  parser.add_option("-i","--index", dest="index", action="store_true",
             help="build an index file")
  parser.add_option("-u","--usetoday", dest="usetoday", action="store_true",
             help="use current date for unreleased versions")
  (options, args) = parser.parse_args()

  if (options.versions == None):
    options.versions = []

  if (len(args) > 2):
    options.versions.append(args[2])

  if (len(options.versions) <= 0):
    parser.error("At least one version needs to be supplied")

  versions = [ Version(v) for v in options.versions ];
  versions.sort();

  maxVersion = str(versions[-1])

  jlist = JiraIter(versions)
  version = maxVersion

  if version in releaseVersion:
    reldate=releaseVersion[version]
  elif options.usetoday:
    reldate=strftime("%Y-%m-%d", gmtime())
  else:
    reldate="Unreleased"

  if not os.path.exists(version):
    os.mkdir(version)

  if options.master:
    reloutputs = Outputs("%(ver)s/RELEASENOTES.%(ver)s.md",
      "%(ver)s/RELEASENOTES.%(key)s.%(ver)s.md",
      [], {"ver":maxVersion, "date":reldate})
    choutputs = Outputs("%(ver)s/CHANGES.%(ver)s.md",
      "%(ver)s/CHANGES.%(key)s.%(ver)s.md",
      [], {"ver":maxVersion, "date":reldate})
  else:
    reloutputs = Outputs("%(ver)s/RELEASENOTES.%(ver)s.md",
      "%(ver)s/RELEASENOTES.%(key)s.%(ver)s.md",
      ["HADOOP","HDFS","MAPREDUCE","YARN"], {"ver":maxVersion, "date":reldate})
    choutputs = Outputs("%(ver)s/CHANGES.%(ver)s.md",
      "%(ver)s/CHANGES.%(key)s.%(ver)s.md",
      ["HADOOP","HDFS","MAPREDUCE","YARN"], {"ver":maxVersion, "date":reldate})

  reloutputs.writeAll(asflicense)
  choutputs.writeAll(asflicense)

  relhead = '# Hadoop %(key)s %(ver)s Release Notes\n\n' \
    'These release notes cover new developer and user-facing incompatibilities, features, and major improvements.\n\n'

  chhead = '# Hadoop Changelog\n\n' \
    '## Release %(ver)s - %(date)s\n'\
    '\n'

  reloutputs.writeAll(relhead)
  choutputs.writeAll(chhead)

  incompatlist=[]
  buglist=[]
  improvementlist=[]
  newfeaturelist=[]
  subtasklist=[]
  tasklist=[]
  testlist=[]
  otherlist=[]

  for jira in sorted(jlist):
    if jira.getIncompatibleChange():
      incompatlist.append(jira)
    elif jira.getType() == "Bug":
      buglist.append(jira)
    elif jira.getType() == "Improvement":
      improvementlist.append(jira)
    elif jira.getType() == "New Feature":
      newfeaturelist.append(jira)
    elif jira.getType() == "Sub-task":
      subtasklist.append(jira)
    elif jira.getType() == "Task":
     tasklist.append(jira)
    elif jira.getType() == "Test":
      testlist.append(jira)
    else:
       otherlist.append(jira)

    line = '* [%s](https://issues.apache.org/jira/browse/%s) | *%s* | **%s**\n' \
        % (notableclean(jira.getId()), notableclean(jira.getId()), notableclean(jira.getPriority()),
           notableclean(jira.getSummary()))

    if (jira.getIncompatibleChange()) and (len(jira.getReleaseNote())==0):
      reloutputs.writeKeyRaw(jira.getProject(),"\n---\n\n")
      reloutputs.writeKeyRaw(jira.getProject(), line)
      line ='\n**WARNING: No release note provided for this incompatible change.**\n\n'
      print 'WARNING: incompatible change %s lacks release notes.' % (notableclean(jira.getId()))
      reloutputs.writeKeyRaw(jira.getProject(), line)

    if (len(jira.getReleaseNote())>0):
      reloutputs.writeKeyRaw(jira.getProject(),"\n---\n\n")
      reloutputs.writeKeyRaw(jira.getProject(), line)
      line ='\n%s\n\n' % (tableclean(jira.getReleaseNote()))
      reloutputs.writeKeyRaw(jira.getProject(), line)

  reloutputs.writeAll("\n\n")
  reloutputs.close()

  choutputs.writeAll("### INCOMPATIBLE CHANGES:\n\n")
  choutputs.writeAll("| JIRA | Summary | Priority | Component | Reporter | Contributor |\n")
  choutputs.writeAll("|:---- |:---- | :--- |:---- |:---- |:---- |\n")
  choutputs.writeList(incompatlist)

  choutputs.writeAll("\n\n### NEW FEATURES:\n\n")
  choutputs.writeAll("| JIRA | Summary | Priority | Component | Reporter | Contributor |\n")
  choutputs.writeAll("|:---- |:---- | :--- |:---- |:---- |:---- |\n")
  choutputs.writeList(newfeaturelist)

  choutputs.writeAll("\n\n### IMPROVEMENTS:\n\n")
  choutputs.writeAll("| JIRA | Summary | Priority | Component | Reporter | Contributor |\n")
  choutputs.writeAll("|:---- |:---- | :--- |:---- |:---- |:---- |\n")
  choutputs.writeList(improvementlist)

  choutputs.writeAll("\n\n### BUG FIXES:\n\n")
  choutputs.writeAll("| JIRA | Summary | Priority | Component | Reporter | Contributor |\n")
  choutputs.writeAll("|:---- |:---- | :--- |:---- |:---- |:---- |\n")
  choutputs.writeList(buglist)

  choutputs.writeAll("\n\n### TESTS:\n\n")
  choutputs.writeAll("| JIRA | Summary | Priority | Component | Reporter | Contributor |\n")
  choutputs.writeAll("|:---- |:---- | :--- |:---- |:---- |:---- |\n")
  choutputs.writeList(testlist)

  choutputs.writeAll("\n\n### SUB-TASKS:\n\n")
  choutputs.writeAll("| JIRA | Summary | Priority | Component | Reporter | Contributor |\n")
  choutputs.writeAll("|:---- |:---- | :--- |:---- |:---- |:---- |\n")
  choutputs.writeList(subtasklist)

  choutputs.writeAll("\n\n### OTHER:\n\n")
  choutputs.writeAll("| JIRA | Summary | Priority | Component | Reporter | Contributor |\n")
  choutputs.writeAll("|:---- |:---- | :--- |:---- |:---- |:---- |\n")
  choutputs.writeList(otherlist)
  choutputs.writeList(tasklist)

  choutputs.writeAll("\n\n")
  choutputs.close()

  if options.index:
    buildindex(options.master)

if __name__ == "__main__":
  main()
