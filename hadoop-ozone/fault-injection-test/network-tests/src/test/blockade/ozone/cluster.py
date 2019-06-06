#!/usr/bin/python

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

import logging
import os
import re
import subprocess
import yaml
import util
from os import environ
from subprocess import call
from blockadeUtils.blockade import Blockade


class Command(object):
  docker = "docker"
  blockade = "blockade"
  docker_compose = "docker-compose"
  ozone = "/opt/hadoop/bin/ozone"
  freon = "/opt/hadoop/bin/ozone freon"


class Configuration:
  """
  Configurations to be used while starting Ozone Cluster.
  Here @property decorators is used to achieve getters, setters and delete
  behaviour for 'datanode_count' attribute.
  @datanode_count.setter will set the value for 'datanode_count' attribute.
  @datanode_count.deleter will delete the current value of 'datanode_count'
  attribute.
  """

  def __init__(self):
    if "MAVEN_TEST" in os.environ:
      compose_dir = environ.get("MAVEN_TEST")
      self.docker_compose_file = os.path.join(compose_dir, "docker-compose.yaml")
    elif "OZONE_HOME" in os.environ:
      compose_dir = os.path.join(environ.get("OZONE_HOME"), "compose", "ozoneblockade")
      self.docker_compose_file = os.path.join(compose_dir, "docker-compose.yaml")
    else:
      __parent_dir__ = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.realpath(__file__)))))
      self.docker_compose_file = os.path.join(__parent_dir__,
                                              "compose", "ozoneblockade",
                                              "docker-compose.yaml")
    self._datanode_count = 3
    os.environ["DOCKER_COMPOSE_FILE"] = self.docker_compose_file

  @property
  def datanode_count(self):
    return self._datanode_count

  @datanode_count.setter
  def datanode_count(self, datanode_count):
    self._datanode_count = datanode_count

  @datanode_count.deleter
  def datanode_count(self):
    del self._datanode_count


class Cluster(object):
  """
  This represents Ozone Cluster.
  Here @property decorators is used to achieve getters, setters and delete
  behaviour for 'om', 'scm', 'datanodes' and 'clients' attributes.
  """

  __logger__ = logging.getLogger(__name__)

  def __init__(self, conf):
    self.conf = conf
    self.docker_compose_file = conf.docker_compose_file
    self._om = None
    self._scm = None
    self._datanodes = None
    self._clients = None
    self.scm_uuid = None
    self.datanode_dir = None

  @property
  def om(self):
    return self._om

  @om.setter
  def om(self, om):
    self._om = om

  @om.deleter
  def om(self):
    del self._om

  @property
  def scm(self):
    return self._scm

  @scm.setter
  def scm(self, scm):
    self._scm = scm

  @scm.deleter
  def scm(self):
    del self._scm

  @property
  def datanodes(self):
    return self._datanodes

  @datanodes.setter
  def datanodes(self, datanodes):
    self._datanodes = datanodes

  @datanodes.deleter
  def datanodes(self):
    del self._datanodes

  @property
  def clients(self):
    return self._clients

  @clients.setter
  def clients(self, clients):
    self._clients = clients

  @clients.deleter
  def clients(self):
    del self._clients

  @classmethod
  def create(cls, config=Configuration()):
    return Cluster(config)

  def start(self):
    """
    Start Ozone Cluster in docker containers.
    """
    Cluster.__logger__.info("Starting Ozone Cluster")
    Blockade.blockade_destroy()
    call([Command.docker_compose, "-f", self.docker_compose_file,
          "up", "-d", "--scale",
          "datanode=" + str(self.conf.datanode_count)])
    Cluster.__logger__.info("Waiting 10s for cluster start up...")
    # Remove the sleep and wait only till the cluster is out of safemode
    # time.sleep(10)
    output = subprocess.check_output([Command.docker_compose, "-f",
                                      self.docker_compose_file, "ps"])
    node_list = []
    for out in output.split("\n")[2:-1]:
      node = out.split(" ")[0]
      node_list.append(node)
      Blockade.blockade_add(node)

    Blockade.blockade_status()
    self.om = filter(lambda x: 'om' in x, node_list)[0]
    self.scm = filter(lambda x: 'scm' in x, node_list)[0]
    self.datanodes = sorted(list(filter(lambda x: 'datanode' in x, node_list)))
    self.clients = filter(lambda x: 'ozone_client' in x, node_list)
    self.scm_uuid = self.__get_scm_uuid__()
    self.datanode_dir = self.get_conf_value("hdds.datanode.dir")

    assert node_list, "no node found in the cluster!"
    Cluster.__logger__.info("blockade created with nodes %s",
                            ' '.join(node_list))

  def get_conf_value(self, key):
    """
    Returns the value of given configuration key.
    """
    command = [Command.ozone, "getconf -confKey " + key]
    exit_code, output = self.__run_docker_command__(command, self.om)
    return str(output).strip()

  def scale_datanode(self, datanode_count):
    """
    Commission new datanodes to the running cluster.
    """
    call([Command.docker_compose, "-f", self.docker_compose_file,
          "up", "-d", "--scale", "datanode=" + datanode_count])

  def partition_network(self, *args):
    """
    Partition the network which is used by the cluster.
    """
    Blockade.blockade_create_partition(*args)


  def restore_network(self):
    """
    Restores the network partition.
    """
    Blockade.blockade_join()


  def __get_scm_uuid__(self):
    """
    Returns SCM's UUID.
    """
    ozone_metadata_dir = self.get_conf_value("ozone.metadata.dirs")
    command = "cat %s/scm/current/VERSION" % ozone_metadata_dir
    exit_code, output = self.__run_docker_command__(command, self.scm)
    output_list = output.split("\n")
    key_value = [x for x in output_list if re.search(r"\w+=\w+", x)]
    uuid = [token for token in key_value if 'scmUuid' in token]
    return uuid.pop().split("=")[1].strip()

  def get_container_states(self, datanode):
    """
    Returns the state of all the containers in the given datanode.
    """
    container_parent_path = "%s/hdds/%s/current/containerDir0" % \
                            (self.datanode_dir, self.scm_uuid)
    command = "find %s -type f -name '*.container'" % container_parent_path
    exit_code, output = self.__run_docker_command__(command, datanode)
    container_state = {}

    container_list = map(str.strip, output.split("\n"))
    for container_path in container_list:
      # Reading the container file.
      exit_code, output = self.__run_docker_command__(
        "cat " + container_path, datanode)
      if exit_code is not 0:
        continue
      data = output.split("\n")
      # Reading key value pairs from container file.
      key_value = [x for x in data if re.search(r"\w+:\s\w+", x)]
      content = "\n".join(key_value)
      content_yaml = yaml.load(content)
      if content_yaml is None:
        continue
      for key, value in content_yaml.items():
        content_yaml[key] = str(value).lstrip()
      # Stores the container state in a dictionary.
      container_state[content_yaml['containerID']] = content_yaml['state']
    return container_state

  def run_freon(self, num_volumes, num_buckets, num_keys, key_size,
                replication_type="RATIS", replication_factor="THREE",
                run_on=None):
    """
    Runs freon on the cluster.
    """
    if run_on is None:
      run_on = self.om
    command = [Command.freon,
               " rk",
               " --numOfVolumes " + str(num_volumes),
               " --numOfBuckets " + str(num_buckets),
               " --numOfKeys " + str(num_keys),
               " --keySize " + str(key_size),
               " --replicationType " + replication_type,
               " --factor " + replication_factor]
    return self.__run_docker_command__(command, run_on)

  def __run_docker_command__(self, command, run_on):
    if isinstance(command, list):
      command = ' '.join(command)
    command = [Command.docker,
               "exec " + run_on,
               command]
    return util.run_cmd(command)

  def stop(self):
    """
    Stops the Ozone Cluster.
    """
    Cluster.__logger__.info("Stopping Ozone Cluster")
    call([Command.docker_compose, "-f", self.docker_compose_file, "down"])
    Blockade.blockade_destroy()

  def container_state_predicate_all_closed(self, datanodes):
    for datanode in datanodes:
      container_states_dn = self.get_container_states(datanode)
      if not container_states_dn \
              or container_states_dn.popitem()[1] != 'CLOSED':
        return False
    return True

  def container_state_predicate_one_closed(self, datanodes):
    for datanode in datanodes:
      container_states_dn = self.get_container_states(datanode)
      if container_states_dn and container_states_dn.popitem()[1] == 'CLOSED':
        return True
    return False

  def container_state_predicate(self, datanode, state):
    container_states_dn = self.get_container_states(datanode)
    if container_states_dn and container_states_dn.popitem()[1] == state:
      return True
    return False
