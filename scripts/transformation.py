#!/usr/bin/python
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
#

"""This module transform properties into different format"""
def render_yaml(yaml_root, prefix=""):
  """render yaml"""
  result = ""
  if isinstance(yaml_root, dict):
    if prefix:
      result += "\n"
      for key in yaml_root:
        result += "{}{}: {}".format(prefix, key, render_yaml(
            yaml_root[key], prefix + "   "))
  elif isinstance(yaml_root, list):
    result += "\n"
    for item in yaml_root:
      result += prefix + " - " + render_yaml(item, prefix + " ")
  else:
    result += "{}\n".format(yaml_root)
  return result


def to_yaml(content):
  """transform to yaml"""
  props = process_properties(content)

  keys = props.keys()
  yaml_props = {}
  for key in keys:
    parts = key.split(".")
    node = yaml_props
    prev_part = None
    parent_node = {}
    for part in parts[:-1]:
      if part.isdigit():
        if isinstance(node, dict):
          parent_node[prev_part] = []
          node = parent_node[prev_part]
        while len(node) <= int(part):
          node.append({})
        parent_node = node
        node = node[int(node)]
      else:
        if part not in node:
          node[part] = {}
        parent_node = node
        node = node[part]
      prev_part = part
    if parts[-1].isdigit():
      if isinstance(node, dict):
        parent_node[prev_part] = []
        node = parent_node[prev_part]
      node.append(props[key])
    else:
      node[parts[-1]] = props[key]

  return render_yaml(yaml_props)


def to_yml(content):
  """transform to yml"""
  return to_yaml(content)


def to_properties(content):
  """transform to properties"""
  result = ""
  props = process_properties(content)
  for key, val in props.items():
    result += "{}: {}\n".format(key, val)
  return result


def to_env(content):
  """transform to environment variables"""
  result = ""
  props = process_properties(content)
  for key, val in props:
    result += "{}={}\n".format(key, val)
  return result


def to_sh(content):
  """transform to shell"""
  result = ""
  props = process_properties(content)
  for key, val in props:
    result += "export {}=\"{}\"\n".format(key, val)
  return result


def to_cfg(content):
  """transform to config"""
  result = ""
  props = process_properties(content)
  for key, val in props:
    result += "{}={}\n".format(key, val)
  return result


def to_conf(content):
  """transform to configuration"""
  result = ""
  props = process_properties(content)
  for key, val in props:
    result += "export {}={}\n".format(key, val)
  return result


def to_xml(content):
  """transform to xml"""
  result = "<configuration>\n"
  props = process_properties(content)
  for key in props:
    result += "<property><name>{0}</name><value>{1}</value></property>\n". \
      format(key, props[key])
  result += "</configuration>"
  return result


def process_properties(content, sep=': ', comment_char='#'):
  """
  Read the file passed as parameter as a properties file.
  """
  props = {}
  for line in content.split("\n"):
    sline = line.strip()
    if sline and not sline.startswith(comment_char):
      key_value = sline.split(sep)
      key = key_value[0].strip()
      value = sep.join(key_value[1:]).strip().strip('"')
      props[key] = value

  return props
