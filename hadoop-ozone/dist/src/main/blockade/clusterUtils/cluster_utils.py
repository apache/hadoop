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


from subprocess import call
import subprocess
import logging
import time
import re
import os
import yaml


logger = logging.getLogger(__name__)


class ClusterUtils(object):
  """
  This class contains all cluster related operations.
  """

  @classmethod
  def cluster_setup(cls, docker_compose_file, datanode_count,
                    destroy_existing_cluster=True):
    """start a blockade cluster"""
    logger.info("compose file :%s", docker_compose_file)
    logger.info("number of DNs :%d", datanode_count)
    if destroy_existing_cluster:
      call(["docker-compose", "-f", docker_compose_file, "down"])
    call(["docker-compose", "-f", docker_compose_file, "up", "-d",
          "--scale", "datanode=" + str(datanode_count)])

    logger.info("Waiting 30s for cluster start up...")
    time.sleep(30)
    output = subprocess.check_output(["docker-compose", "-f",
                                      docker_compose_file, "ps"])
    output_array = output.split("\n")[2:-1]

    container_list = []
    for out in output_array:
      container = out.split(" ")[0]
      container_list.append(container)
      call(["blockade", "add", container])
      time.sleep(2)

    assert container_list, "no container found!"
    logger.info("blockade created with containers %s",
                ' '.join(container_list))

    return container_list

  @classmethod
  def cluster_destroy(cls, docker_compose_file):
    logger.info("Running docker-compose -f %s down", docker_compose_file)
    call(["docker-compose", "-f", docker_compose_file, "down"])

  @classmethod
  def run_freon(cls, docker_compose_file, num_volumes, num_buckets,
                num_keys, key_size, replication_type, replication_factor,
                freon_client='om'):
    # run freon
    cmd = "docker-compose -f %s " \
          "exec %s /opt/hadoop/bin/ozone " \
          "freon rk " \
          "--numOfVolumes %s " \
          "--numOfBuckets %s " \
          "--numOfKeys %s " \
          "--keySize %s " \
          "--replicationType %s " \
          "--factor %s" % (docker_compose_file, freon_client, num_volumes,
                           num_buckets, num_keys, key_size,
                           replication_type, replication_factor)
    exit_code, output = cls.run_cmd(cmd)
    return exit_code, output

  @classmethod
  def run_cmd(cls, cmd):
    command = cmd
    if isinstance(cmd, list):
      command = ' '.join(cmd)
    logger.info(" RUNNING: %s", command)
    all_output = ""
    myprocess = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, shell=True)
    while myprocess.poll() is None:
      op = myprocess.stdout.readline()
      if op:
        all_output += op
        logger.info(op)
    other_output = myprocess.communicate()
    other_output = other_output[0].strip()
    if other_output != "":
      all_output += other_output
      for each_line in other_output.split("\n"):
        logger.info(" %s", each_line.strip())
    reg = re.compile(r"(\r\n|\n)$")
    all_output = reg.sub("", all_output, 1)

    return myprocess.returncode, all_output

  @classmethod
  def get_ozone_confkey_value(cls, docker_compose_file, key_name):
    cmd = "docker-compose -f %s " \
          "exec om /opt/hadoop/bin/ozone " \
          "getconf -confKey %s" \
          % (docker_compose_file, key_name)
    exit_code, output = cls.run_cmd(cmd)
    assert exit_code == 0, "getconf of key=[%s] failed with output=[%s]" \
                           % (key_name, output)
    return str(output).strip()

  @classmethod
  def find_scm_uuid(cls, docker_compose_file):
    """
    This function returns scm uuid.
    """
    ozone_metadata_dir = cls.get_ozone_confkey_value(docker_compose_file,
                                                     "ozone.metadata.dirs")
    cmd = "docker-compose -f %s exec scm cat %s/scm/current/VERSION" % \
          (docker_compose_file, ozone_metadata_dir)
    exit_code, output = cls.run_cmd(cmd)
    assert exit_code == 0, "get scm UUID failed with output=[%s]" % output
    output_list = output.split("\n")
    output_list = [x for x in output_list if re.search(r"\w+=\w+", x)]
    output_dict = dict(x.split("=") for x in output_list)
    return str(output_dict['scmUuid']).strip()

  @classmethod
  def find_container_status(cls, docker_compose_file, datanode_index):
    """
    This function returns the datanode's container replica state.
    In this function, it finds all <containerID>.container files.
    Then, it opens each file and checks the state of the containers
    in the datanode.
    It returns 'None' as container state if it cannot find any
    <containerID>.container file in the datanode.
    Sample <containerID>.container contains state as following:
      state: <STATE OF THE CONTAINER REPLICA>
    """

    datanode_dir = cls.get_ozone_confkey_value(docker_compose_file,
                                               "hdds.datanode.dir")
    scm_uuid = cls.find_scm_uuid(docker_compose_file)
    container_parent_path = "%s/hdds/%s/current/containerDir0" % \
                            (datanode_dir, scm_uuid)
    cmd = "docker-compose -f %s exec --index=%s datanode find %s -type f " \
          "-name '*.container'" \
          % (docker_compose_file, datanode_index, container_parent_path)
    exit_code, output = cls.run_cmd(cmd)
    container_state = "None"
    if exit_code == 0 and output:
      container_list = map(str.strip, output.split("\n"))
      for container_path in container_list:
        cmd = "docker-compose -f %s exec --index=%s datanode cat %s" \
              % (docker_compose_file, datanode_index, container_path)
        exit_code, output = cls.run_cmd(cmd)
        assert exit_code == 0, \
          "command=[%s] failed with output=[%s]" % (cmd, output)
        container_db_list = output.split("\n")
        container_db_list = [x for x in container_db_list
                             if re.search(r"\w+:\s\w+", x)]
        # container_db_list will now contain the lines which has got
        # yaml representation , i.e , key: value
        container_db_info = "\n".join(container_db_list)
        container_db_dict = yaml.load(container_db_info)
        for key, value in container_db_dict.items():
          container_db_dict[key] = str(value).lstrip()
        if container_state == "None":
          container_state = container_db_dict['state']
        else:
          assert container_db_dict['state'] == container_state, \
            "all containers are not in same state"

    return container_state

  @classmethod
  def findall_container_status(cls, docker_compose_file, scale):
    """
    This function returns container replica states of all datanodes.
    """
    all_datanode_container_status = []
    for index in range(scale):
      all_datanode_container_status.append(
          cls.find_container_status(docker_compose_file, index + 1))
    logger.info("All datanodes container status: %s",
                ' '.join(all_datanode_container_status))

    return all_datanode_container_status

  @classmethod
  def create_volume(cls, docker_compose_file, volume_name):
    command = "docker-compose -f %s " \
              "exec ozone_client /opt/hadoop/bin/ozone " \
              "sh volume create /%s --user root" % \
              (docker_compose_file, volume_name)
    logger.info("Creating Volume %s", volume_name)
    exit_code, output = cls.run_cmd(command)
    assert exit_code == 0, "Ozone volume create failed with output=[%s]" \
                           % output

  @classmethod
  def delete_volume(cls, docker_compose_file, volume_name):
    command = "docker-compose -f %s " \
              "exec ozone_client /opt/hadoop/bin/ozone " \
              "sh volume delete /%s" % (docker_compose_file, volume_name)
    logger.info("Deleting Volume %s", volume_name)
    exit_code, output = cls.run_cmd(command)
    return exit_code, output

  @classmethod
  def create_bucket(cls, docker_compose_file, bucket_name, volume_name):
    command = "docker-compose -f %s " \
              "exec ozone_client /opt/hadoop/bin/ozone " \
              "sh bucket create /%s/%s" % (docker_compose_file,
                                           volume_name, bucket_name)
    logger.info("Creating Bucket %s in volume %s",
                bucket_name, volume_name)
    exit_code, output = cls.run_cmd(command)
    assert exit_code == 0, "Ozone bucket create failed with output=[%s]" \
                           % output

  @classmethod
  def delete_bucket(cls, docker_compose_file, bucket_name, volume_name):
    command = "docker-compose -f %s " \
              "exec ozone_client /opt/hadoop/bin/ozone " \
              "sh bucket delete /%s/%s" % (docker_compose_file,
                                           volume_name, bucket_name)
    logger.info("Running delete bucket of %s/%s", volume_name, bucket_name)
    exit_code, output = cls.run_cmd(command)
    return exit_code, output

  @classmethod
  def put_key(cls, docker_compose_file, bucket_name, volume_name,
              filepath, key_name=None, replication_factor=None):
    command = "docker-compose -f %s " \
              "exec ozone_client ls  %s" % (docker_compose_file, filepath)
    exit_code, output = cls.run_cmd(command)
    assert exit_code == 0, "%s does not exist" % filepath
    if key_name is None:
      key_name = os.path.basename(filepath)
    command = "docker-compose -f %s " \
              "exec ozone_client /opt/hadoop/bin/ozone " \
              "sh key put /%s/%s/%s %s" % (docker_compose_file,
                                           volume_name, bucket_name,
                                           key_name, filepath)
    if replication_factor:
      command = "%s --replication=%s" % (command, replication_factor)
    logger.info("Creating key %s in %s/%s", key_name,
                volume_name, bucket_name)
    exit_code, output = cls.run_cmd(command)
    assert exit_code == 0, "Ozone put Key failed with output=[%s]" % output

  @classmethod
  def delete_key(cls, docker_compose_file, bucket_name, volume_name,
                 key_name):
    command = "docker-compose -f %s " \
              "exec ozone_client /opt/hadoop/bin/ozone " \
              "sh key delete /%s/%s/%s" \
              % (docker_compose_file, volume_name, bucket_name, key_name)
    logger.info("Running delete key %s in %s/%s",
                key_name, volume_name, bucket_name)
    exit_code, output = cls.run_cmd(command)
    return exit_code, output

  @classmethod
  def get_key(cls, docker_compose_file, bucket_name, volume_name,
              key_name, filepath=None):
    if filepath is None:
      filepath = '.'
    command = "docker-compose -f %s " \
              "exec ozone_client /opt/hadoop/bin/ozone " \
              "sh key get /%s/%s/%s %s" % (docker_compose_file,
                                           volume_name, bucket_name,
                                           key_name, filepath)
    logger.info("Running get key %s in %s/%s", key_name,
                volume_name, bucket_name)
    exit_code, output = cls.run_cmd(command)
    assert exit_code == 0, "Ozone get Key failed with output=[%s]" % output

  @classmethod
  def find_checksum(cls, docker_compose_file, filepath):
    command = "docker-compose -f %s " \
              "exec ozone_client md5sum  %s" % (docker_compose_file, filepath)
    exit_code, output = cls.run_cmd(command)
    assert exit_code == 0, "Cant find checksum"
    myoutput = output.split("\n")
    finaloutput = ""
    for line in myoutput:
      if line.find("Warning") >= 0 or line.find("is not a tty") >= 0:
        logger.info("skip this line: %s", line)
      else:
        finaloutput = finaloutput + line
    checksum = finaloutput.split(" ")
    logger.info("Checksum of %s is : %s", filepath, checksum[0])
    return checksum[0]

  @classmethod
  def get_pipelines(cls, docker_compose_file):
    command = "docker-compose -f %s " \
                         + "exec ozone_client /opt/hadoop/bin/ozone scmcli " \
                         + "listPipelines" % (docker_compose_file)
    exit_code, output = cls.run_cmd(command)
    assert exit_code == 0, "list pipeline command failed"
    return output

  @classmethod
  def find_om_scm_client_datanodes(cls, container_list):

      om = filter(lambda x: 'om_1' in x, container_list)
      scm = filter(lambda x: 'scm' in x, container_list)
      datanodes = sorted(
          list(filter(lambda x: 'datanode' in x, container_list)))
      client = filter(lambda x: 'ozone_client' in x, container_list)
      return om, scm, client, datanodes