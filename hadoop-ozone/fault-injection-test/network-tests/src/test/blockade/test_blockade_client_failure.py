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

import re
import time
import logging
import ozone.util

from ozone.cluster import OzoneCluster

logger = logging.getLogger(__name__)


def setup_function():
    global cluster
    cluster = OzoneCluster.create()
    cluster.start()


def teardown_function():
    cluster.stop()


def test_client_failure_isolate_two_datanodes():
    """
    In this test, all DNs are isolated from each other.
    two of the DNs cannot communicate with any other node in the cluster.
    Expectation :
    Write should fail.
    Keys written before partition created should be read.
    """
    om = cluster.om
    scm = cluster.scm
    dns = cluster.datanodes
    client = cluster.client
    oz_client = cluster.get_client()

    epoch_time = int(time.time())
    volume_name = "%s-%s" % ("volume", epoch_time)
    bucket_name = "%s-%s" % ("bucket", epoch_time)
    key_name = "key-1"

    oz_client.create_volume(volume_name)
    oz_client.create_bucket(volume_name, bucket_name)
    oz_client.put_key("/etc/passwd", volume_name, bucket_name, key_name, "THREE")

    first_set = [om, scm, dns[0], client]
    second_set = [dns[1]]
    third_set = [dns[2]]

    logger.info("Partitioning the network")
    cluster.partition_network(first_set, second_set, third_set)

    exit_code, output = oz_client.run_freon(1, 1, 1, 10240)
    assert exit_code != 0, "freon run should have failed."

    oz_client.get_key(volume_name, bucket_name, key_name, "/tmp/")

    file_checksum = ozone.util.get_checksum("/etc/passwd", client)
    key_checksum = ozone.util.get_checksum("/tmp/%s" % key_name, client)

    assert file_checksum == key_checksum


def test_client_failure_isolate_one_datanode():
    """
    In this test, one of the DNs is isolated from all other nodes.
    Expectation :
    Write should pass.
    Keys written before partition created can be read.
    """
    om = cluster.om
    scm = cluster.scm
    dns = cluster.datanodes
    client = cluster.client
    oz_client = cluster.get_client()

    epoch_time = int(time.time())
    volume_name = "%s-%s" % ("volume", epoch_time)
    bucket_name = "%s-%s" % ("bucket", epoch_time)
    key_name = "key-1"

    oz_client.create_volume(volume_name)
    oz_client.create_bucket(volume_name, bucket_name)
    oz_client.put_key("/etc/passwd", volume_name, bucket_name, key_name, "THREE")

    first_set = [om, scm, dns[0], dns[1], client]
    second_set = [dns[2]]

    logger.info("Partitioning the network")
    cluster.partition_network(first_set, second_set)

    exit_code, output = oz_client.run_freon(1, 1, 1, 10240)
    assert re.search("3 way commit failed", output) is not None
    assert exit_code == 0, "freon run failed with output=[%s]" % output

    oz_client.get_key(volume_name, bucket_name, key_name, "/tmp/")

    file_checksum = ozone.util.get_checksum("/etc/passwd", client)
    key_checksum = ozone.util.get_checksum("/tmp/%s" % key_name, cluster.client)

    assert file_checksum == key_checksum

