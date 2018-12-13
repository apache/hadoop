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

"""This module has apis to create and remove a blockade cluster"""

from subprocess import call
import subprocess
import logging
import time


logger = logging.getLogger(__name__)


class ClusterUtils(object):

    @classmethod
    def cluster_setup(cls, docker_compose_file, datanode_count):
        """start a blockade cluster"""
        logger.info("compose file :%s", docker_compose_file)
        logger.info("number of DNs :%d", datanode_count)
        call(["docker-compose", "-f", docker_compose_file, "down"])
        call(["docker-compose", "-f", docker_compose_file, "up", "-d", "--scale", "datanode=" + str(datanode_count)])

        logger.info("Waiting 30s for cluster start up...")
        time.sleep(30)
        output = subprocess.check_output(["docker-compose", "-f", docker_compose_file, "ps"])
        output_array = output.split("\n")[2:-1]

        container_list = []
        for out in output_array:
            container = out.split(" ")[0]
            container_list.append(container)
            call(["blockade", "add", container])
            time.sleep(2)

        assert container_list, "no container found!"
        logger.info("blockade created with containers %s", ' '.join(container_list))

        return container_list

    @classmethod
    def cluster_destroy(cls, docker_compose_file):
        call(["docker-compose", "-f", docker_compose_file, "down"])

    @classmethod
    def run_freon(cls, docker_compose_file, num_volumes, num_buckets, num_keys, key_size,
                  replication_type, replication_factor):
        # run freon
        logger.info("Running freon ...")
        output = call(["docker-compose", "-f", docker_compose_file,
                                          "exec", "ozoneManager",
                                          "/opt/hadoop/bin/ozone",
                                          "freon", "rk",
                                          "--numOfVolumes", str(num_volumes),
                                          "--numOfBuckets", str(num_buckets),
                                          "--numOfKeys", str(num_keys),
                                          "--keySize", str(key_size),
                                          "--replicationType", replication_type,
                                          "--factor", replication_factor])
        assert output == 0, "freon run failed with exit code=[%s]" % output
