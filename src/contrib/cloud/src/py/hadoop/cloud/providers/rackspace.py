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

from __future__ import with_statement

import base64
import os
import subprocess
import sys
import time
import uuid

from hadoop.cloud.cluster import Cluster
from hadoop.cloud.cluster import Instance
from hadoop.cloud.cluster import TimeoutException
from hadoop.cloud.service import HadoopService
from hadoop.cloud.service import TASKTRACKER
from libcloud.drivers.rackspace import RackspaceNodeDriver
from libcloud.base import Node
from libcloud.base import NodeImage

RACKSPACE_KEY = os.environ['RACKSPACE_KEY']
RACKSPACE_SECRET = os.environ['RACKSPACE_SECRET']

STATE_MAP = { 'running': 'ACTIVE' }
STATE_MAP_REVERSED = dict((v, k) for k, v in STATE_MAP.iteritems())

USER_DATA_FILENAME = "/etc/init.d/rackspace-init.sh"

class RackspaceCluster(Cluster):
  """
  A cluster of instances running on Rackspace Cloud Servers. A cluster has a
  unique name, which is stored under the "cluster" metadata key of each server.

  Every instance in the cluster has one or more roles, stored as a
  comma-separated string under the "roles" metadata key. For example, an instance
  with roles "foo" and "bar" has a "foo,bar" "roles" key.
  
  At boot time two files are injected into an instance's filesystem: the user
  data file (which is used as a boot script), and the user's public key.
  """
  @staticmethod
  def get_clusters_with_role(role, state="running", driver=None):
    driver = driver or RackspaceNodeDriver(RACKSPACE_KEY, RACKSPACE_SECRET)
    all_nodes = RackspaceCluster._list_nodes(driver)
    clusters = set()
    for node in all_nodes:
      try:
        if node.extra['metadata'].has_key('cluster') and \
            role in node.extra['metadata']['roles'].split(','):
          if node.state == STATE_MAP[state]:
            clusters.add(node.extra['metadata']['cluster'])
      except KeyError:
        pass
    return clusters
  
  @staticmethod
  def _list_nodes(driver, retries=5):
    attempts = 0
    while True:
      try:
        return driver.list_nodes()
      except IOError:
        attempts = attempts + 1
        if attempts > retries:
          raise
        time.sleep(5)

  def __init__(self, name, config_dir, driver=None):
    super(RackspaceCluster, self).__init__(name, config_dir)
    self.driver = driver or RackspaceNodeDriver(RACKSPACE_KEY, RACKSPACE_SECRET)

  def get_provider_code(self):
    return "rackspace"
  
  def _get_nodes(self, state_filter=None):
    all_nodes = RackspaceCluster._list_nodes(self.driver)
    nodes = []
    for node in all_nodes:
      try:
        if node.extra['metadata']['cluster'] == self.name:
          if state_filter == None or node.state == STATE_MAP[state_filter]:
            nodes.append(node)
      except KeyError:
        pass
    return nodes

  def _to_instance(self, node):
    return Instance(node.id, node.public_ip[0], node.private_ip[0])
  
  def _get_nodes_in_role(self, role, state_filter=None):
    all_nodes = RackspaceCluster._list_nodes(self.driver)
    nodes = []
    for node in all_nodes:
      try:
        if node.extra['metadata']['cluster'] == self.name and \
          role in node.extra['metadata']['roles'].split(','):
          if state_filter == None or node.state == STATE_MAP[state_filter]:
            nodes.append(node)
      except KeyError:
        pass
    return nodes
  
  def get_instances_in_role(self, role, state_filter=None):
    """
    Get all the instances in a role, filtered by state.

    @param role: the name of the role
    @param state_filter: the state that the instance should be in
      (e.g. "running"), or None for all states
    """
    return [self._to_instance(node) for node in \
            self._get_nodes_in_role(role, state_filter)]

  def _print_node(self, node, out):
    out.write("\t".join((node.extra['metadata']['roles'], node.id,
      node.name,
      self._ip_list_to_string(node.public_ip),
      self._ip_list_to_string(node.private_ip),
      STATE_MAP_REVERSED[node.state])))
    out.write("\n")
    
  def _ip_list_to_string(self, ips):
    if ips is None:
      return ""
    return ",".join(ips)

  def print_status(self, roles=None, state_filter="running", out=sys.stdout):
    if not roles:
      for node in self._get_nodes(state_filter):
        self._print_node(node, out)
    else:
      for role in roles:
        for node in self._get_nodes_in_role(role, state_filter):
          self._print_node(node, out)

  def launch_instances(self, roles, number, image_id, size_id,
                       instance_user_data, **kwargs):
    metadata = {"cluster": self.name, "roles": ",".join(roles)}
    node_ids = []
    files = { USER_DATA_FILENAME: instance_user_data.read() }
    if "public_key" in kwargs:
      files["/root/.ssh/authorized_keys"] = open(kwargs["public_key"]).read()
    for dummy in range(number):
      node = self._launch_instance(roles, image_id, size_id, metadata, files)
      node_ids.append(node.id)
    return node_ids

  def _launch_instance(self, roles, image_id, size_id, metadata, files):
    instance_name = "%s-%s" % (self.name, uuid.uuid4().hex[-8:])
    node = self.driver.create_node(instance_name, self._find_image(image_id),
                                   self._find_size(size_id), metadata=metadata,
                                   files=files)
    return node

  def _find_image(self, image_id):
    return NodeImage(id=image_id, name=None, driver=None)

  def _find_size(self, size_id):
    matches = [i for i in self.driver.list_sizes() if i.id == str(size_id)]
    if len(matches) != 1:
      return None
    return matches[0]

  def wait_for_instances(self, instance_ids, timeout=600):
    start_time = time.time()
    while True:
      if (time.time() - start_time >= timeout):
        raise TimeoutException()
      try:
        if self._all_started(instance_ids):
          break
      except Exception:
        pass
      sys.stdout.write(".")
      sys.stdout.flush()
      time.sleep(1)

  def _all_started(self, node_ids):
    all_nodes = RackspaceCluster._list_nodes(self.driver)
    node_id_to_node = {}
    for node in all_nodes:
      node_id_to_node[node.id] = node
    for node_id in node_ids:
      try:
        if node_id_to_node[node_id].state != STATE_MAP["running"]:
          return False
      except KeyError:
        return False
    return True

  def terminate(self):
    nodes = self._get_nodes("running")
    print nodes
    for node in nodes:
      self.driver.destroy_node(node)

class RackspaceHadoopService(HadoopService):
    
  def _update_cluster_membership(self, public_key, private_key):
    """
    Creates a cluster-wide hosts file and copies it across the cluster.
    This is a stop gap until DNS is configured on the cluster. 
    """
    ssh_options = '-o StrictHostKeyChecking=no'

    time.sleep(30) # wait for SSH daemon to start
    nodes = self.cluster._get_nodes('running')
    # create hosts file
    hosts_file = 'hosts'
    with open(hosts_file, 'w') as f:
      f.write("127.0.0.1 localhost localhost.localdomain\n")
      for node in nodes:
        f.write(node.public_ip[0] + "\t" + node.name + "\n")
    # copy to each node in the cluster
    for node in nodes:
      self._call('scp -i %s %s %s root@%s:/etc/hosts' \
                 % (private_key, ssh_options, hosts_file, node.public_ip[0]))
    os.remove(hosts_file)

  def _call(self, command):
    print command
    try:
      subprocess.call(command, shell=True)
    except Exception, e:
      print e
  
