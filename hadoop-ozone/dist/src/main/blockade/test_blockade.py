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
import os
import logging
import pytest
from blockadeUtils.blockade import Blockade
from clusterUtils.cluster_utils import ClusterUtils


logger = logging.getLogger(__name__)
parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
FILE = os.path.join(parent_dir, "compose", "ozone", "docker-compose.yaml")
SCALE = 6
CONTAINER_LIST = []


def setup_module():
    global CONTAINER_LIST
    CONTAINER_LIST = ClusterUtils.cluster_setup(FILE, SCALE)
    output = Blockade.blockade_status()
    assert output == 0, "blockade status command failed with exit code=[%s]" % output


def teardown_module():
    Blockade.blockade_destroy()
    ClusterUtils.cluster_destroy(FILE)


def teardown():
    logger.info("Inside teardown")
    Blockade.blockade_fast_all()


@pytest.mark.parametrize("flaky_nodes", ["datanode", "scm", "om", "all"])
def test_flaky(flaky_nodes):
    Blockade.make_flaky(flaky_nodes, CONTAINER_LIST)
    Blockade.blockade_status()
    ClusterUtils.run_freon(FILE, 1, 1, 1, 10240, "RATIS", "THREE")
