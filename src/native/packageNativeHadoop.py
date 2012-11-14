#! /usr/bin/python

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


# packageNativeHadoop.py - A simple script to help package native-hadoop libraries

#
# Note: 
# This script relies on the following environment variables to function correctly:
#  * BASE_NATIVE_LIB_DIR
#  * BUILD_NATIVE_DIR
#  * DIST_LIB_DIR
# All these are setup by build.xml.
#

import os
import sys
import re
from glob import iglob
from shutil import copy2
from shutil import copytree
from os.path import join
from os.path import basename

# Convert Windows paths with back-slashes into canonical Unix paths
# with forward slashes.  Unix paths are unchanged.  Mixed formats
# just naively have back-slashes converted to forward slashes.
def canonicalpath(path):
  return re.sub('\\\\','/',path)

def copy_files(src_glob, dst_folder):
  for fname in iglob(src_glob):
    src_file = canonicalpath(fname)
    dst_file = canonicalpath(join(dst_folder, basename(fname)))
    if (os.path.isdir(src_file)):
      copytree(src_file, dst_file)
    else:
      copy2(src_file, dst_file)

def main(argv=None):
  dist_lib_dir = os.environ["DIST_LIB_DIR"]
  if not os.path.isdir(dist_lib_dir):
    os.makedirs(dist_lib_dir)

  # Copy the pre-built libraries in BASE_NATIVE_LIB_DIR
  if os.path.isdir(os.environ["BASE_NATIVE_LIB_DIR"]):
    for platform in os.listdir(os.environ["BASE_NATIVE_LIB_DIR"]):
      base_path = canonicalpath(os.path.join(os.environ["BASE_NATIVE_LIB_DIR"], platform))
      dist_path = canonicalpath(os.path.join(dist_lib_dir, platform))
      if not os.path.isdir(dist_path):
        os.makedirs(dist_path)
        print "Created %s" % dist_path
      print "Copying libraries from %s to %s" % (base_path+ "/*hadoop*", dist_path)
      copy_files(base_path + "/*hadoop*", dist_path)

  # Copy the custom-built libraries in BUILD_NATIVE_DIR
  if os.path.isdir(os.environ["BUILD_NATIVE_DIR"]):
    for platform in os.listdir(os.environ["BUILD_NATIVE_DIR"]):
      base_path = canonicalpath(os.path.join(os.environ["BUILD_NATIVE_DIR"], platform))
      dist_path = canonicalpath(os.path.join(dist_lib_dir, platform))
      if not os.path.isdir(dist_path):
        os.makedirs(dist_path)
        print "Created %s" % dist_path
      print "Copying libraries from %s to %s" % (base_path+ "/lib/*hadoop*", dist_path)
      copy_files(base_path + "/lib/*hadoop*", dist_path)

  if "BUNDLE_SNAPPY_LIB" in os.environ and os.environ["BUNDLE_SNAPPY_LIB"] == "true":
    if "SNAPPY_LIB_DIR" in os.environ:
      if os.path.isdir(os.environ["SNAPPY_LIB_DIR"]):
        print "Copying Snappy library in %s to %s/" % (os.environ["SNAPPY_LIB_DIR"], dist_lib_dir)
        copy_files(os.environ["SNAPPY_LIB_DIR"] + "/*", dist_lib_dir)
      else:
        print "Snappy lib directory '%s' does not exist" % os.environ["SNAPPY_LIB_DIR"]
        return 1
    else:
      print "Snappy SNAPPY_LIB_DIR environment variable undefined"
      return 1

  return 0

##########################
if __name__ == "__main__":
  sys.exit(main())
