#!/usr/bin/python
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import re
import sys
import string
from optparse import OptionParser

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

def docstrip(key,string):
  string=re.sub("^## @%s " % key ,"",string)
  string=string.lstrip()
  string=string.rstrip()
  return string

def toc(list):
  tocout=[]
  header=()
  for i in list:
    if header != i.getinter():
      header=i.getinter()
      line="  * %s\n" % (i.headerbuild())
      tocout.append(line)
    line="    * [%s](#%s)\n" % (i.getname().replace("_","\_"),i.getname())
    tocout.append(line)
  return tocout

class ShellFunction:
  def __init__(self):
    self.reset()

  def __cmp__(self,other):
    if (self.audience == other.audience):
      if (self.stability == other.stability):
        if (self.replaceb == other.replaceb):
          return(cmp(self.name,other.name))
        else:
          if (self.replaceb == "Yes"):
            return -1
          else:
            return 1
      else:
          if (self.stability == "Stable"):
            return -1
          else:
            return 1
    else:
      if (self.audience == "Public"):
        return -1
      else:
        return 1

  def reset(self):
    self.name=None
    self.audience=None
    self.stability=None
    self.replaceb=None
    self.returnt=None
    self.desc=None
    self.params=None

  def setname(self,text):
    definition=text.split();
    self.name=definition[1]

  def getname(self):
    if (self.name is None):
      return "None"
    else:
      return self.name

  def setaudience(self,text):
    self.audience=docstrip("audience",text)
    self.audience=self.audience.capitalize()

  def getaudience(self):
    if (self.audience is None):
      return "None"
    else:
      return self.audience

  def setstability(self,text):
    self.stability=docstrip("stability",text)
    self.stability=self.stability.capitalize()

  def getstability(self):
    if (self.stability is None):
      return "None"
    else:
      return self.stability

  def setreplace(self,text):
    self.replaceb=docstrip("replaceable",text)
    self.replaceb=self.replaceb.capitalize()

  def getreplace(self):
    if (self.replaceb is None):
      return "None"
    else:
      return self.replaceb

  def getinter(self):
    return( (self.getaudience(), self.getstability(), self.getreplace()))

  def addreturn(self,text):
    if (self.returnt is None):
      self.returnt = []
    self.returnt.append(docstrip("return",text))

  def getreturn(self):
    if (self.returnt is None):
      return "Nothing"
    else:
      return "\n\n".join(self.returnt)

  def adddesc(self,text):
    if (self.desc is None):
      self.desc = []
    self.desc.append(docstrip("description",text))

  def getdesc(self):
    if (self.desc is None):
      return "None"
    else:
      return " ".join(self.desc)

  def addparam(self,text):
    if (self.params is None):
      self.params = []
    self.params.append(docstrip("param",text))

  def getparams(self):
    if (self.params is None):
      return ""
    else:
      return " ".join(self.params)

  def getusage(self):
    line="%s %s" % (self.name, self.getparams())
    return line

  def headerbuild(self):
    if self.getreplace() == "Yes":
      replacetext="Replaceable"
    else:
      replacetext="Not Replaceable"
    line="%s/%s/%s" % (self.getaudience(), self.getstability(), replacetext)
    return(line)

  def getdocpage(self):
    line="### `%s`\n\n"\
         "* Synopsis\n\n"\
         "```\n%s\n"\
         "```\n\n" \
         "* Description\n\n" \
         "%s\n\n" \
         "* Returns\n\n" \
         "%s\n\n" \
         "| Classification | Level |\n" \
         "| :--- | :--- |\n" \
         "| Audience | %s |\n" \
         "| Stability | %s |\n" \
         "| Replaceable | %s |\n\n" \
         % (self.getname(),
            self.getusage(),
            self.getdesc(),
            self.getreturn(),
            self.getaudience(),
            self.getstability(),
            self.getreplace())
    return line

  def __str__(self):
    line="{%s %s %s %s}" \
      % (self.getname(),
         self.getaudience(),
         self.getstability(),
         self.getreplace())
    return line

def main():
  parser=OptionParser(usage="usage: %prog --skipprnorep --output OUTFILE --input INFILE [--input INFILE ...]")
  parser.add_option("-o","--output", dest="outfile",
     action="store", type="string",
     help="file to create", metavar="OUTFILE")
  parser.add_option("-i","--input", dest="infile",
     action="append", type="string",
     help="file to read", metavar="INFILE")
  parser.add_option("--skipprnorep", dest="skipprnorep",
     action="store_true", help="Skip Private & Not Replaceable")

  (options, args)=parser.parse_args()

  allfuncs=[]
  for filename in options.infile:
    with open(filename,"r") as shellcode:
      funcdef=ShellFunction()
      for line in shellcode:
        if line.startswith('## @description'):
          funcdef.adddesc(line)
        elif line.startswith('## @audience'):
          funcdef.setaudience(line)
        elif line.startswith('## @stability'):
          funcdef.setstability(line)
        elif line.startswith('## @replaceable'):
          funcdef.setreplace(line)
        elif line.startswith('## @param'):
          funcdef.addparam(line)
        elif line.startswith('## @return'):
          funcdef.addreturn(line)
        elif line.startswith('function'):
          funcdef.setname(line)
          if options.skipprnorep and \
            funcdef.getaudience() == "Private" and \
            funcdef.getreplace() == "No":
               pass
          else:
            allfuncs.append(funcdef)
          funcdef=ShellFunction()

  allfuncs=sorted(allfuncs)

  outfile=open(options.outfile, "w")
  outfile.write(asflicense)
  for line in toc(allfuncs):
    outfile.write(line)

  outfile.write("\n------\n\n")

  header=[]
  for funcs in allfuncs:
    if header != funcs.getinter():
      header=funcs.getinter()
      line="## %s\n" % (funcs.headerbuild())
      outfile.write(line)
    outfile.write(funcs.getdocpage())
  outfile.close()

if __name__ == "__main__":
  main()

