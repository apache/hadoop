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
import random
from clusterUtils.cluster_utils import ClusterUtils

logger = logging.getLogger(__name__)


class Blockade(object):

    @classmethod
    def blockade_destroy(cls):
        logger.info("Running blockade destroy")
        call(["blockade", "destroy"])

    @classmethod
    def blockade_up(cls):
        call(["blockade", "up"])

    @classmethod
    def blockade_status(cls):
        exit_code, output = ClusterUtils.run_cmd("blockade status")
        return exit_code, output

    @classmethod
    def make_flaky(cls, flaky_node, container_list):
        # make the network flaky
        om, scm, _, datanodes = \
            ClusterUtils.find_om_scm_client_datanodes(container_list)
        node_dict = {
                "all": "--all",
                "scm" : scm[0],
                "om" : om[0],
                "datanode": random.choice(datanodes)
                }[flaky_node]
        logger.info("flaky node: %s", node_dict)

        output = call(["blockade", "flaky", node_dict])
        assert output == 0, "flaky command failed with exit code=[%s]" % output

    @classmethod
    def blockade_fast_all(cls):
        output = call(["blockade", "fast", "--all"])
        assert output == 0, "fast command failed with exit code=[%s]" % output

    @classmethod
    def blockade_create_partition(cls, *args):
        nodes = ""
        for node_list in args:
            nodes = nodes + ','.join(node_list) + " "
        exit_code, output = \
            ClusterUtils.run_cmd("blockade partition %s" % nodes)
        assert exit_code == 0, \
            "blockade partition command failed with exit code=[%s]" % output

    @classmethod
    def blockade_join(cls):
        output = call(["blockade", "join"])
        assert output == 0, "blockade join command failed with exit code=[%s]" \
                            % output

    @classmethod
    def blockade_stop(cls, node, all_nodes=False):
        if all_nodes:
            output = call(["blockade", "stop", "--all"])
        else:
            output = call(["blockade", "stop", node])
        assert output == 0, "blockade stop command failed with exit code=[%s]" \
                            % output

    @classmethod
    def blockade_start(cls, node, all_nodes=False):
        if all_nodes:
            output = call(["blockade", "start", "--all"])
        else:
            output = call(["blockade", "start", node])
        assert output == 0, "blockade start command failed with " \
                            "exit code=[%s]" % output