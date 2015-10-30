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
import pprint
import os
import re
import sys
import urllib
import urllib2
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

# clean output dir
def cleanOutputDir(dir):
    files = os.listdir(dir)
    for name in files:
        os.remove(os.path.join(dir,name))
    os.rmdir(dir)

def mstr(obj):
  if (obj is None):
    return ""
  return unicode(obj)

def buildindex(title,license):
  versions=reversed(sorted(glob("[0-9]*.[0-9]*.[0-9]*")))
  with open("index.md","w") as indexfile:
    if license is True:
      indexfile.write(asflicense)
    for v in versions:
      indexfile.write("* %s v%s\n" % (title,v))
      for k in ("Changes","Release Notes"):
        indexfile.write("    * %s (%s/%s.%s.html)\n" \
          % (k,v,k.upper().replace(" ",""),v))
  indexfile.close()

class GetVersions:
  """ yo """
  def __init__(self,versions, projects):
    versions = versions
    projects = projects
    self.newversions = []
    pp = pprint.PrettyPrinter(indent=4)
    at=0
    end=1
    count=100
    versions.sort()
    print "Looking for %s through %s"%(versions[0],versions[-1])
    for p in projects:
      resp = urllib2.urlopen("https://issues.apache.org/jira/rest/api/2/project/%s/versions"%p)
      data = json.loads(resp.read())
      for d in data:
        if d['name'][0].isdigit and versions[0] <= d['name'] and d['name'] <= versions[-1]:
          print "Adding %s to the list" % d['name']
          self.newversions.append(d['name'])
    newlist=list(set(self.newversions))
    self.newversions=newlist

  def getlist(self):
      pp = pprint.PrettyPrinter(indent=4)
      return(self.newversions)

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
    if (self.notes is None):
      field = self.parent.fieldIdMap['Release Note']
      if (self.fields.has_key(field)):
        self.notes=mstr(self.fields[field])
      else:
        self.notes=self.getDescription()
    return self.notes

  def getPriority(self):
    ret = ""
    pri = self.fields['priority']
    if(pri is not None):
      ret = pri['name']
    return mstr(ret)

  def getAssignee(self):
    ret = ""
    mid = self.fields['assignee']
    if(mid is not None):
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
    if(mid is not None):
      ret = mid['name']
    return mstr(ret)

  def getReporter(self):
    ret = ""
    mid = self.fields['reporter']
    if(mid is not None):
      ret = mid['displayName']
    return mstr(ret)

  def getProject(self):
    ret = ""
    mid = self.fields['project']
    if(mid is not None):
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
    if (self.incompat is None):
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

  def checkMissingComponent(self):
      if (len(self.fields['components'])>0):
          return False
      return True

  def checkMissingAssignee(self):
      if (self.fields['assignee'] is not None):
          return False
      return True

  def checkVersionString(self):
      field = self.parent.fieldIdMap['Fix Version/s']
      for h in self.fields[field]:
          found = re.match('^((\d+)(\.\d+)*).*$|^(\w+\-\d+)$', h['name'])
          if not found:
              return True
      return False

  def getReleaseDate(self,version):
    for j in range(len(self.fields['fixVersions'])):
      if self.fields['fixVersions'][j]==version:
        return(self.fields['fixVersions'][j]['releaseDate'])
    return None

class JiraIter:
  """An Iterator of JIRAs"""

  def __init__(self, version, projects):
    self.version = version
    self.projects = projects
    v=str(version).replace("-SNAPSHOT","")

    resp = urllib2.urlopen("https://issues.apache.org/jira/rest/api/2/field")
    data = json.loads(resp.read())

    self.fieldIdMap = {}
    for part in data:
      self.fieldIdMap[part['name']] = part['id']

    self.jiras = []
    at=0
    end=1
    count=100
    while (at < end):
      params = urllib.urlencode({'jql': "project in ('"+"' , '".join(projects)+"') and fixVersion in ('"+v+"') and resolution = Fixed", 'startAt':at, 'maxResults':count})
      resp = urllib2.urlopen("https://issues.apache.org/jira/rest/api/2/search?%s"%params)
      data = json.loads(resp.read())
      if (data.has_key('errorMessages')):
        raise Exception(data['errorMessages'])
      at = data['startAt'] + data['maxResults']
      end = data['total']
      self.jiras.extend(data['issues'])

      needaversion=False
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
  parser = OptionParser(usage="usage: %prog --project PROJECT [--project PROJECT] --version VERSION [--version VERSION2 ...]",
		epilog=
               "Markdown-formatted CHANGES and RELEASENOTES files will be stored in a directory"
               " named after the highest version provided.")
  parser.add_option("-i","--index", dest="index", action="store_true",
             default=False, help="build an index file")
  parser.add_option("-l","--license", dest="license", action="store_false",
             default=True, help="Add an ASF license")
  parser.add_option("-n","--lint", dest="lint", action="store_true",
             help="use lint flag to exit on failures")
  parser.add_option("-p", "--project", dest="projects",
             action="append", type="string",
             help="projects in JIRA to include in releasenotes", metavar="PROJECT")
  parser.add_option("-r", "--range", dest="range", action="store_true",
             default=False, help="Given versions are a range")
  parser.add_option("-t", "--projecttitle", dest="title",
             type="string",
             help="Title to use for the project (default is Apache PROJECT)")
  parser.add_option("-u","--usetoday", dest="usetoday", action="store_true",
             default=False, help="use current date for unreleased versions")
  parser.add_option("-v", "--version", dest="versions",
             action="append", type="string",
             help="versions in JIRA to include in releasenotes", metavar="VERSION")
  (options, args) = parser.parse_args()

  if (options.versions is None):
    options.versions = []

  if (len(args) > 2):
    options.versions.append(args[2])

  if (len(options.versions) <= 0):
    parser.error("At least one version needs to be supplied")

  proxy = urllib2.ProxyHandler()
  opener = urllib2.build_opener(proxy)
  urllib2.install_opener(opener)

  projects = options.projects

  if (options.range is True):
    versions = [ Version(v) for v in GetVersions(options.versions, projects).getlist() ]
  else:
    versions = [ Version(v) for v in options.versions ]
  versions.sort();

  if (options.title is None):
    title=projects[0]
  else:
    title=options.title

  haderrors=False

  for v in versions:
    vstr=str(v)
    jlist = JiraIter(vstr,projects)

    if vstr in releaseVersion:
      reldate=releaseVersion[vstr]
    elif options.usetoday:
      reldate=strftime("%Y-%m-%d", gmtime())
    else:
      reldate="Unreleased"

    if not os.path.exists(vstr):
      os.mkdir(vstr)

    reloutputs = Outputs("%(ver)s/RELEASENOTES.%(ver)s.md",
      "%(ver)s/RELEASENOTES.%(key)s.%(ver)s.md",
      [], {"ver":v, "date":reldate, "title":title})
    choutputs = Outputs("%(ver)s/CHANGES.%(ver)s.md",
      "%(ver)s/CHANGES.%(key)s.%(ver)s.md",
      [], {"ver":v, "date":reldate, "title":title})

    if (options.license is True):
      reloutputs.writeAll(asflicense)
      choutputs.writeAll(asflicense)

    relhead = '# %(title)s %(key)s %(ver)s Release Notes\n\n' \
      'These release notes cover new developer and user-facing incompatibilities, features, and major improvements.\n\n'
    chhead = '# %(title)s Changelog\n\n' \
      '## Release %(ver)s - %(date)s\n'\
      '\n'

    reloutputs.writeAll(relhead)
    choutputs.writeAll(chhead)
    errorCount=0
    warningCount=0
    lintMessage=""
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
        warningCount+=1
        reloutputs.writeKeyRaw(jira.getProject(),"\n---\n\n")
        reloutputs.writeKeyRaw(jira.getProject(), line)
        line ='\n**WARNING: No release note provided for this incompatible change.**\n\n'
        lintMessage += "\nWARNING: incompatible change %s lacks release notes." % (notableclean(jira.getId()))
        reloutputs.writeKeyRaw(jira.getProject(), line)

      if jira.checkVersionString():
          warningCount+=1
          lintMessage += "\nWARNING: Version string problem for %s " % jira.getId()

      if (jira.checkMissingComponent() or jira.checkMissingAssignee()):
          errorCount+=1
          errorMessage=[]
          jira.checkMissingComponent() and errorMessage.append("component")
          jira.checkMissingAssignee() and errorMessage.append("assignee")
          lintMessage += "\nERROR: missing %s for %s " %  (" and ".join(errorMessage) , jira.getId())

      if (len(jira.getReleaseNote())>0):
        reloutputs.writeKeyRaw(jira.getProject(),"\n---\n\n")
        reloutputs.writeKeyRaw(jira.getProject(), line)
        line ='\n%s\n\n' % (tableclean(jira.getReleaseNote()))
        reloutputs.writeKeyRaw(jira.getProject(), line)

    if (options.lint is True):
        print lintMessage
        print "======================================="
        print "%s: Error:%d, Warning:%d \n" % (vstr, errorCount, warningCount)
        if (errorCount>0):
           haderrors=True
           cleanOutputDir(vstr)
           continue

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
    buildindex(title,options.license)

  if haderrors is True:
    sys.exit(1)

if __name__ == "__main__":
  main()
