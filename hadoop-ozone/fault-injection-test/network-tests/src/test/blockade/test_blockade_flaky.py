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

import os
import logging
import random
import pytest
from os import environ
from blockadeUtils.blockade import Blockade
from ozone.cluster import Cluster


logger = logging.getLogger(__name__)
if "MAVEN_TEST" in os.environ:
  compose_dir = environ.get("MAVEN_TEST")
  FILE = os.path.join(compose_dir, "docker-compose.yaml")
elif "OZONE_HOME" in os.environ:
  compose_dir = environ.get("OZONE_HOME")
  FILE = os.path.join(compose_dir, "compose", "ozoneblockade", \
         "docker-compose.yaml")
else:
  parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
  FILE = os.path.join(parent_dir, "compose", "ozoneblockade", \
         "docker-compose.yaml")

os.environ["DOCKER_COMPOSE_FILE"] = FILE
SCALE = 6
CONTAINER_LIST = []


def setup_function(function):
  global cluster
  cluster = Cluster.create()
  cluster.start()


def teardown_function(function):
  cluster.stop()


@pytest.mark.parametrize("flaky_node", ["datanode", "scm", "om", "all"])
def test_flaky(flaky_node):
    """
    In these tests, we make the network of the nodes as flaky using blockade.
    There are 4 tests :
    1) one of the datanodes selected randomly and network of the datanode is
    made flaky.
    2) scm network is made flaky.
    3) om network is made flaky.
    4) Network of all the nodes are made flaky.

    """
    flaky_container_name = {
        "scm": cluster.scm,
        "om": cluster.om,
        "datanode": random.choice(cluster.datanodes),
        "all": "--all"
    }[flaky_node]

    Blockade.make_flaky(flaky_container_name)
    Blockade.blockade_status()
    exit_code, output = cluster.run_freon(1, 1, 1, 10240)
    assert exit_code == 0, "freon run failed with output=[%s]" % output
