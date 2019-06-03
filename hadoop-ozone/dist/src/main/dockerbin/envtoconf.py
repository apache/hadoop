#!/usr/bin/python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""convert environment variables to config"""

import os
import re

import argparse

import sys
import transformation

class Simple(object):
  """Simple conversion"""
  def __init__(self, args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--destination", help="Destination directory", required=True)
    self.args = parser.parse_args(args=args)
    # copy the default files to file.raw in destination directory

    self.known_formats = ['xml', 'properties', 'yaml', 'yml', 'env', "sh", "cfg", 'conf']
    self.output_dir = self.args.destination
    self.excluded_envs = ['HADOOP_CONF_DIR']
    self.configurables = {}

  def destination_file_path(self, name, extension):
    """destination file path"""
    return os.path.join(self.output_dir, "{}.{}".format(name, extension))

  def write_env_var(self, name, extension, key, value):
    """Write environment variables"""
    with open(self.destination_file_path(name, extension) + ".raw", "a") as myfile:
      myfile.write("{}: {}\n".format(key, value))

  def process_envs(self):
    """Process environment variables"""
    for key in os.environ.keys():
      if key in self.excluded_envs:
          continue
      pattern = re.compile("[_\\.]")
      parts = pattern.split(key)
      extension = None
      name = parts[0].lower()
      if len(parts) > 1:
        extension = parts[1].lower()
        config_key = key[len(name) + len(extension) + 2:].strip()
      if extension and "!" in extension:
        splitted = extension.split("!")
        extension = splitted[0]
        fmt = splitted[1]
        config_key = key[len(name) + len(extension) + len(fmt) + 3:].strip()
      else:
        fmt = extension

      if extension and extension in self.known_formats:
        if name not in self.configurables.keys():
          with open(self.destination_file_path(name, extension) + ".raw", "w") as myfile:
            myfile.write("")
        self.configurables[name] = (extension, fmt)
        self.write_env_var(name, extension, config_key, os.environ[key])
      else:
        for configurable_name in self.configurables:
          if key.lower().startswith(configurable_name.lower()):
            self.write_env_var(configurable_name,
                               self.configurables[configurable_name],
                               key[len(configurable_name) + 1:],
                               os.environ[key])

  def transform(self):
    """transform"""
    for configurable_name in self.configurables:
      name = configurable_name
      extension, fmt = self.configurables[name]

      destination_path = self.destination_file_path(name, extension)

      with open(destination_path + ".raw", "r") as myfile:
        content = myfile.read()
        transformer_func = getattr(transformation, "to_" + fmt)
        content = transformer_func(content)
        with open(destination_path, "w") as myfile:
          myfile.write(content)

  def main(self):
    """main"""

    # add the
    self.process_envs()

    # copy file.ext.raw to file.ext in the destination directory, and
    # transform to the right format (eg. key: value ===> XML)
    self.transform()


def main():
  """main"""
  Simple(sys.argv[1:]).main()


if __name__ == '__main__':
  Simple(sys.argv[1:]).main()
