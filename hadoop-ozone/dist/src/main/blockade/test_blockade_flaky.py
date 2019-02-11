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
import time
import logging
import pytest
from blockadeUtils.blockade import Blockade
from clusterUtils.cluster_utils import ClusterUtils


logger = logging.getLogger(__name__)
parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
FILE = os.path.join(parent_dir, "compose", "ozoneblockade",
                    "docker-compose.yaml")
os.environ["DOCKER_COMPOSE_FILE"] = FILE
SCALE = 6
CONTAINER_LIST = []


def setup_module():
    global CONTAINER_LIST
    Blockade.blockade_destroy()
    CONTAINER_LIST = ClusterUtils.cluster_setup(FILE, SCALE)
    exit_code, output = Blockade.blockade_status()
    assert exit_code == 0, "blockade status command failed with output=[%s]" % \
                           output


def teardown_module():
    Blockade.blockade_destroy()
    ClusterUtils.cluster_destroy(FILE)


def teardown():
    logger.info("Inside teardown")
    Blockade.blockade_fast_all()
    time.sleep(5)


@pytest.mark.parametrize("flaky_nodes", ["datanode", "scm", "om", "all"])
def test_flaky(flaky_nodes):
    Blockade.make_flaky(flaky_nodes, CONTAINER_LIST)
    Blockade.blockade_status()
    exit_code, output = ClusterUtils.run_freon(FILE, 1, 1, 1, 10240, "RATIS",
                                               "THREE")
    assert exit_code == 0, "freon run failed with output=[%s]" % output