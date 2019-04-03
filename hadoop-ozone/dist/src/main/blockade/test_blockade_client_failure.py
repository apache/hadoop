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
import re
import time
import logging
from blockadeUtils.blockade import Blockade
from clusterUtils.cluster_utils import ClusterUtils


logger = logging.getLogger(__name__)
parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
FILE = os.path.join(parent_dir, "compose", "ozoneblockade",
                    "docker-compose.yaml")
os.environ["DOCKER_COMPOSE_FILE"] = FILE
SCALE = 3
CONTAINER_LIST = []
OM = []
SCM = []
DATANODES = []
CLIENT = []


def setup():
    global CONTAINER_LIST, OM, SCM, DATANODES, CLIENT, ORIG_CHECKSUM, \
        TEST_VOLUME_NAME, TEST_BUCKET_NAME
    epoch_time = int(time.time())
    TEST_VOLUME_NAME = "%s%s" % ("volume", epoch_time)
    TEST_BUCKET_NAME = "%s%s" % ("bucket", epoch_time)
    Blockade.blockade_destroy()
    CONTAINER_LIST = ClusterUtils.cluster_setup(FILE, SCALE)
    exit_code, output = Blockade.blockade_status()
    assert exit_code == 0, "blockade status command failed with output=[%s]" % \
                           output
    OM, SCM, CLIENT, DATANODES = \
        ClusterUtils.find_om_scm_client_datanodes(CONTAINER_LIST)
    exit_code, output = ClusterUtils.run_freon(FILE, 1, 1, 1, 10240, "RATIS",
                                               "THREE", "ozone_client")
    assert exit_code == 0, "freon run failed with output=[%s]" % output
    ClusterUtils.create_volume(FILE, TEST_VOLUME_NAME)
    ClusterUtils.create_bucket(FILE, TEST_BUCKET_NAME, TEST_VOLUME_NAME)
    ORIG_CHECKSUM = ClusterUtils.find_checksum(FILE, "/etc/passwd")


def teardown():
    logger.info("Inside teardown")
    Blockade.blockade_destroy()


def teardown_module():
    ClusterUtils.cluster_destroy(FILE)


def test_client_failure_isolate_two_datanodes():
    """
    In this test, all datanodes are isolated from each other.
    two of the datanodes cannot communicate with any other node in the cluster.
    Expectation :
    Write should fail.
    Keys written before parition created can be read.
    """
    test_key_name = "testkey1"
    ClusterUtils.put_key(FILE, TEST_BUCKET_NAME, TEST_VOLUME_NAME,
                         "/etc/passwd", key_name=test_key_name,
                         replication_factor='THREE')
    first_set = [OM[0], SCM[0], DATANODES[0], CLIENT[0]]
    second_set = [DATANODES[1]]
    third_set = [DATANODES[2]]
    Blockade.blockade_create_partition(first_set, second_set, third_set)
    Blockade.blockade_status()
    exit_code, output = \
        ClusterUtils.run_freon(FILE, 1, 1, 1, 10240, "RATIS", "THREE")
    assert re.search(
        "Status: Failed",
        output) is not None
    ClusterUtils.get_key(FILE, TEST_BUCKET_NAME, TEST_VOLUME_NAME,
                         test_key_name, "/tmp/")
    key_checksum = ClusterUtils.find_checksum(FILE, "/tmp/%s" % test_key_name)

    assert key_checksum == ORIG_CHECKSUM


def test_client_failure_isolate_one_datanode():
    """
    In this test, one of the datanodes is isolated from all other nodes.
    Expectation :
    Write should pass.
    Keys written before partition created can be read.
    """
    test_key_name = "testkey2"
    ClusterUtils.put_key(FILE, TEST_BUCKET_NAME, TEST_VOLUME_NAME,
                         "/etc/passwd", key_name=test_key_name,
                         replication_factor='THREE')
    first_set = [OM[0], SCM[0], DATANODES[0], DATANODES[1], CLIENT[0]]
    second_set = [DATANODES[2]]
    Blockade.blockade_create_partition(first_set, second_set)
    Blockade.blockade_status()
    exit_code, output = \
        ClusterUtils.run_freon(FILE, 1, 1, 1, 10240, "RATIS", "THREE")
    assert re.search("3 way commit failed", output) is not None
    assert re.search("Status: Success", output) is not None
    ClusterUtils.get_key(FILE, TEST_BUCKET_NAME, TEST_VOLUME_NAME,
                         test_key_name, "/tmp/")
    key_checksum = ClusterUtils.find_checksum(FILE, "/tmp/%s" % test_key_name)

    assert key_checksum == ORIG_CHECKSUM