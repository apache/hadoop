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

logger = logging.getLogger(__name__)


class Blockade(object):

    @classmethod
    def blockade_destroy(cls):
        call(["blockade", "destroy"])

    @classmethod
    def blockade_status(cls):
        output = call(["blockade", "status"])
        return output

    @classmethod
    def make_flaky(cls, flaky_node, container_list):
        # make the network flaky
        om = filter(lambda x: 'ozoneManager' in x, container_list)
        scm = filter(lambda x: 'scm' in x, container_list)
        datanodes = filter(lambda x: 'datanode' in x, container_list)
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
