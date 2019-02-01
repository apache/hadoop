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
import yaml


logger = logging.getLogger(__name__)


class ClusterUtils(object):

    @classmethod
    def cluster_setup(cls, docker_compose_file, datanode_count):
        """start a blockade cluster"""
        logger.info("compose file :%s", docker_compose_file)
        logger.info("number of DNs :%d", datanode_count)
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
                  num_keys, key_size, replication_type, replication_factor):
        # run freon
        cmd = "docker-compose -f %s " \
              "exec ozoneManager /opt/hadoop/bin/ozone " \
              "freon rk " \
              "--numOfVolumes %s " \
              "--numOfBuckets %s " \
              "--numOfKeys %s " \
              "--keySize %s " \
              "--replicationType %s " \
              "--factor %s" % (docker_compose_file, num_volumes,
                               num_buckets, num_keys, key_size,
                               replication_type, replication_factor)
        exit_code, output = cls.run_cmd(cmd)
        return exit_code, output

    @classmethod
    def run_cmd(cls, cmd):
        command = cmd
        if isinstance(cmd, list):
            command = ' '.join(cmd)
        logger.info(" RUNNING: " + command)
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
                logger.info(" " + each_line.strip())
        reg = re.compile(r"(\r\n|\n)$")
        all_output = reg.sub("", all_output, 1)

        return myprocess.returncode, all_output

    @classmethod
    def get_ozone_confkey_value(cls, docker_compose_file, key_name):
        cmd = "docker-compose -f %s " \
              "exec ozoneManager /opt/hadoop/bin/ozone " \
              "getconf -confKey %s" \
              % (docker_compose_file, key_name)
        exit_code, output = cls.run_cmd(cmd)
        assert exit_code == 0, "getconf of key=[%s] failed with output=[%s]" \
                               %(key_name, output)
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
        output_list = list(filter(lambda x: re.search("\w+=\w+", x),
                                  output_list))
        output_dict = dict(map(lambda x: x.split("="), output_list))
        return str(output_dict['scmUuid']).strip()

    @classmethod
    def find_datanode_container_status(cls, docker_compose_file,
                                       datanode_index):
        """
        This function returns the datanode's container replica state.
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
        assert exit_code == 0, "command=[%s] failed with output=[%s]" % \
                               (cmd, output)
        assert output, "No container info present"
        container_list = map(str.strip, output.split("\n"))
        container_state = None
        for container_path in container_list:
            cmd = "docker-compose -f %s exec --index=%s datanode cat %s" \
                  % (docker_compose_file, datanode_index, container_path)
            exit_code, output = cls.run_cmd(cmd)
            assert exit_code == 0, "command=[%s] failed with output=[%s]" % \
                                   (cmd, output)
            container_db_list = output.split("\n")
            container_db_list = \
                list(filter(lambda x: re.search("\w+:\s\w+", x),
                            container_db_list))
            container_db_info = "\n".join(container_db_list)
            container_db_dict = yaml.load(container_db_info)
            for key, value in container_db_dict.items():
                container_db_dict[key] = str(value).lstrip()
            if not container_state:
                container_state = container_db_dict['state']
            else:
                assert container_db_dict['state'] == container_state, \
                    "all containers are not in same state"

        return container_state

    @classmethod
    def find_all_datanodes_container_status(cls, docker_compose_file, scale):
        """
        This function returns container replica states of all datanodes.
        """
        all_datanode_container_status = []
        for index in range(scale):
            all_datanode_container_status.append(
                cls.find_datanode_container_status(docker_compose_file,
                                                   index+1))
        logger.info("All datanodes container status: %s",
                    ' '.join(all_datanode_container_status))

        return all_datanode_container_status