#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Launches a yarn service

WORK IN PROGRESS, IGNORE

This is as work in progress project to build as new launcher script for
any Hadoop service
A key feature here is that the configs are defined in JSON files -
files that are read in the order passed down, and merged into each other.

The final merged file is used to define the java command to execute
-and hadoop XML files.


It uses a JSON config file 
  --jfile configuration file (JSON format)
  -class classname
  -Dname=value -arbitrary value to pass down to the JVM
  --java: any JVM arg
  -javaX: javaX value


 after an -- , all following commands are passed straight down to the invoked process.
  # -xJ name=value JVM options. No: this is just another param
  -xF file  file to load next. Files are loaded in order. 
  -xD name=value again, values are loaded in order
  -xU undefine
  -xX main class, 'eXecute'

  --  end of arguments
  

"""

import sys
# see : http://simplejson.readthedocs.org/en/latest/
# and install w/ easy_install simplejson
import simplejson

KEY_JFILE = "-xF"
KEY_DEF = "-xD"
KEY_UNDEF = "-xU"
KEY_EXEC = "-xX"
KEY_ARGS = "--"

COMMANDS = [KEY_JFILE, KEY_DEF, KEY_EXEC]

#

def debug(string) :
  print string


def pop_required_arg(arglist, previousArg) :
  """
  Pop the first element off the list and return it.
  If the list is empty, raise an exception about a missing argument after the $previousArgument
  """
  if not len(arglist) :
    raise Exception, "Missing required parameter after %s" % previousArg
  head = arglist[0]
  del arglist[0]
  return head


def parse_one_jfile(filename) :
  """
  read in the given config file
  """
  parsed = simplejson.load(open(filename, "r"))
  return parsed

# hand down sys.argv:
def extract_jfiles(args) :
  """ takes a list of arg strings and separates them into jfile references
  and other arguments.
  """
  l = len(args)
  stripped = []
  jfiles = []
  index = 0
  while index < l :
    elt = args[index]
    index += 1
    if KEY_JFILE == elt :
      # a match
      if index == l :
        #overshoot
        raise Exception("Missing filename after " + KEY_JFILE)
      filename = args[index]
      debug("jfile " + filename)
      jfiles.append(filename)
      index += 1
    else :
      stripped.append(elt)
  return jfiles, stripped


def extract_args(args) :
  """
  Take a list of args, parse them or fail, generating a dictionary of actions
  Return: dictionary and all leftover arguments
  """
  jfiles = []
  execs = []
  defs = []
  remainder = []
  while len(args) :
    # the next call cannot fail, because of the len(args)
    arg = pop_required_arg(args, "")
    if KEY_JFILE == arg :
      jfiles.append(pop_required_arg(args, KEY_JFILE))
    elif KEY_DEF == arg :
      defs.append((KEY_DEF, pop_required_arg(args, KEY_DEF)))
    elif KEY_UNDEF == arg :
      defs.append((KEY_UNDEF, pop_required_arg(args, KEY_UNDEF)))
    elif KEY_EXEC == arg :
      execs.append(pop_required_arg(args, KEY_EXEC))
    elif KEY_ARGS == arg :
      remainder += args
      args = []
    else :
      remainder.append(arg)
      #build the action list
  actions = {
    KEY_JFILE : jfiles,
    KEY_EXEC : execs,
    KEY_DEF : defs,
    KEY_ARGS : remainder
  }
  #end of the run, there's a dictionary and a list of unparsed values
  return actions


def get(conf, key, defVal) :
  if conf.has_key(key) :
    return conf[key]
  else :
    return defVal


def merge_json(conf, json) :
  """ merge in a json dict with the existing one
  in: configuration dict, json dict
  out: configuration'
  """
  for (key, val) in json.items() :
    if key in conf :
      #there's a match, do a more detailed merge
      oldval = conf[key]
      if type(oldval) == dict and type(val) == dict :
      # two dictionary instances -merge
        merge_json(oldval, val)
      else :
        conf[key] = val
    else :
      conf[key] = val
  return conf


def merge_jfile(conf, filename) :
  json = parse_one_jfile(filename)
  return merge_json(conf, json)


def merge_jfile_list(conf, jfiles) :
  """ merge a list of jfiles on top of a conf dict
  """
  for jfile in jfiles :
    conf = merge_jfile(conf, jfile)
  return conf


def split_to_keyval_tuple(param) :
  """
  Split a key=value string into the (key,value) tuple
  * an exception is raised on any string "=value"
  * if there is no string: exception.
  * a key only definition maps to (key, None)
  * a "key=" definition maps to (key, "")
  """
  if not len(param) :
    raise Exception, "Empty string cannot be a key=value definition"
  equalsPos = param.find("=")
  if equalsPos < 0 :
    return param, None
  elif not equalsPos :
    raise Exception, "no key in argument %s" % param
  else :
    key = param[:(equalsPos - 1)]
    value = param[(equalsPos + 1) :]
    return key, value


def recursive_define(conf, path, value) :
  if not len(path) :
    #fallen off the end of the world
    return
  entry = path[0]
  if len(path) == 1 :
    #end of list, apply it.
    conf[entry] = value
  else :
    #there's 1+ elements below, yet there's a subdir here.
    if conf.has_key(entry) and type(conf[entry]) == dict :
      #it's a subdir, simple: recurse.
      recursive_define(conf[entry], path[1 :], value)
    else :
      #either there is an entry that isn't a conf, or its not there. Same outcome.
      subconf = {}
      conf[entry] = subconf
      recursive_define(subconf, path[1 :], value)

def recursive_undef(conf, path) :
  if not len(path) :
    #fallen off the end of the world
    return
  entry = path[0]
  if len(path) == 1 :
    #end of list, apply it.
    del conf[entry]
  else :
    #there's 1+ elements below, yet there's a subdir here.
    if conf.has_key(entry) and type(conf[entry]) == dict :
      #it's a subdir, simple: recurse.
      recursive_undef(conf[entry], path[1 :])
    else :
      #either there is an entry that isn't a conf, or its not there. Same outcome.
      pass

def apply_action(conf, action, key, value) :
  """
  Apply either a def or undef action; splitting the key into a path and running through it.
  """
  keypath = key.split("/")
  #now have a split key,
  if KEY_DEF == action :
    recursive_define(conf, keypath, value)
  elif KEY_UNDEF == action :
    recursive_undef(conf, keypath)


def apply_local_definitions(conf, definitions) :
  """
  Run through the definition actions and apply them one by one
  """
  for defn in definitions :
    # split into key=value; no value -> empty string
    (action, param) = defn
    if KEY_DEF == action :
      (key, val) = split_to_keyval_tuple(param)
      apply_action(conf, KEY_DEF, key, val)

  return conf


#def parse_args(conf, args) :
#  """
#   split an arg string, parse the jfiles & merge over the conf
#  (configuration, args[]) -> (conf', stripped, jfiles[])
#  """
#  (jfiles, stripped) = extract_jfiles(args)
#
#  actions = extract_args(args)
#  jfiles = actions[KEY_JFILE]
#  conf = merge_jfile_list(conf, jfiles)
#  return conf, actions


def print_conf(conf) :
  """ dump the configuration to the console
  """
  print "{"
  for (key, val) in conf.items() :
    if type(val) == dict :
      print key
      print_conf(val)
    else :
      print "" + key + " => " + str(val)
  print "}"


def list_to_str(l, spacer) :
  result = ""
  for elt in l :
    if len(result) > 0 :
      result += spacer
    result += elt
  return result


def list_to_hxml_str(l) :
  return list_to_str(l, ",")


def export_kv_xml(output, key, value) :
  line = "<property><name>" + key + "</name><value>" + str(value) + "</value>\n"
  print line
  output.write(line)


def export_to_hadoop_xml(output, conf) :
  """ export the conf to hadoop XML
  dictionaries are skipped.
  """
  output.write("<configuration>\n")
  for (key, value) in conf.items() :
    if type(value) is list :
      # list print
      export_kv_xml(output, key, list_to_hxml_str(value))
    else :
      if type(value) is dict :
        print "skipping dict " + key
      else :
        export_kv_xml(output, key, value)
  output.write("</configuration>\n")


def start(conf, stripped_args) :
  """
  start the process by grabbing exec/args for the arguments
  """
  ex = conf["exec"]
  args = []
  jsonargs = get(ex, "args", [])
  args.extend(jsonargs)
  args.extend(stripped_args)
  classname = get(ex, "classname", "")
  if not len(classname) :
    raise Exception, "No classname supplied"
  classname = get(ex, "classname", "")
  commandline = ["java"]
  classpath = []
  jvmargs = []
  commandline.extend(jvmargs)
  commandline.append("-classpath")
  commandline.append(list_to_str(classpath, ":"))
  commandline.append("org.apache.hadoop.yarn.service.launcher.ServiceLauncher")
  commandline.append(classname)
  commandline.extend(args)
  print "ready to exec : %s" % commandline


def main() :
#  (conf, stripped, jfiles) = parse_args({}, sys.argv[1 :])
  actions = extract_args(sys.argv[1 :])
  jfiles = actions[KEY_JFILE]
  conf = merge_jfile_list({}, jfiles)
  apply_local_definitions(conf, actions[KEY_DEF])
  exec_args = actions[KEY_ARGS]

  print_conf(conf)
  #  if len(stripped) > 0 :
  #got an output file
  #    filename = stripped[0]
  #    print "Writing XML configuration to " + filename
  #    output = open(filename, "w")
  #    export_to_hadoop_xml(output, conf["site"])
  start(conf, exec_args)


if __name__ == "__main__" :
  main()


