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

import time
import logging

from ozone.cluster import OzoneCluster

logger = logging.getLogger(__name__)


def setup_function():
    global cluster
    cluster = OzoneCluster.create()
    cluster.start()


def teardown_function():
    cluster.stop()


def test_three_dns_isolate_one_scm_failure():
    """
    In this test, all DNs are isolated from each other.
    One of the DNs (third DN) cannot communicate with SCM.
    Expectation :
    The container replica state in first DN should be closed.
    The container replica state in second DN should be closed.
    The container replica state in third DN should be open.
    """
    om = cluster.om
    scm = cluster.scm
    dns = cluster.datanodes
    client = cluster.client
    oz_client = cluster.get_client()

    oz_client.run_freon(1, 1, 1, 10240)

    first_set = [om, scm, dns[0], client]
    second_set = [om, scm, dns[1], client]
    third_set = [om, dns[2], client]

    cluster.partition_network(first_set, second_set, third_set)
    containers = cluster.get_containers_on_datanode(dns[0])
    for container in containers:
        container.wait_until_replica_is_closed(dns[0])

    for container in containers:
        assert container.get_state(dns[0]) == 'CLOSED'
        assert container.get_state(dns[1]) == 'CLOSED'
        assert container.get_state(dns[2]) == 'OPEN'

    cluster.restore_network()
    for container in containers:
        container.wait_until_all_replicas_are_closed()
    for container in containers:
        assert container.get_state(dns[0]) == 'CLOSED'
        assert container.get_state(dns[1]) == 'CLOSED'
        assert container.get_state(dns[2]) == 'CLOSED'

    exit_code, output = oz_client.run_freon(1, 1, 1, 10240)
    assert exit_code == 0, "freon run failed with output=[%s]" % output


def test_three_dns_isolate_two_scm_failure():
    """
    In this test, all DNs are isolated from each other.
    two DNs cannot communicate with SCM (second DN and third DN)
    Expectation :
    The container replica state in first DN should be quasi-closed.
    The container replica state in second DN should be open.
    The container replica state in third DN should be open.
    """
    om = cluster.om
    scm = cluster.scm
    dns = cluster.datanodes
    client = cluster.client
    oz_client = cluster.get_client()

    oz_client.run_freon(1, 1, 1, 10240)

    first_set = [om, scm, dns[0], client]
    second_set = [om, dns[1], client]
    third_set = [om, dns[2], client]

    cluster.partition_network(first_set, second_set, third_set)
    containers = cluster.get_containers_on_datanode(dns[0])
    for container in containers:
        container.wait_until_replica_is_quasi_closed(dns[0])

    for container in containers:
        assert container.get_state(dns[0]) == 'QUASI_CLOSED'
        assert container.get_state(dns[1]) == 'OPEN'
        assert container.get_state(dns[2]) == 'OPEN'

    cluster.restore_network()
    for container in containers:
        container.wait_until_all_replicas_are_closed()
    for container in containers:
        assert container.get_state(dns[0]) == 'CLOSED'
        assert container.get_state(dns[1]) == 'CLOSED'
        assert container.get_state(dns[2]) == 'CLOSED'

    exit_code, output = oz_client.run_freon(1, 1, 1, 10240)
    assert exit_code == 0, "freon run failed with output=[%s]" % output


def test_three_dns_isolate_three_scm_failure():
    """
    In this test, all DNs are isolated from each other and also cannot
    communicate with SCM.
    Expectation :
    The container replica state in first DN should be open.
    The container replica state in second DN should be open.
    The container replica state in third DN should be open.
    """
    om = cluster.om
    dns = cluster.datanodes
    client = cluster.client
    oz_client = cluster.get_client()

    oz_client.run_freon(1, 1, 1, 10240)

    first_set = [om, dns[0], client]
    second_set = [om, dns[1], client]
    third_set = [om, dns[2], client]

    cluster.partition_network(first_set, second_set, third_set)

    # Wait till the datanodes are marked as stale by SCM
    time.sleep(150)

    containers = cluster.get_containers_on_datanode(dns[0])
    for container in containers:
        assert container.get_state(dns[0]) == 'OPEN'
        assert container.get_state(dns[1]) == 'OPEN'
        assert container.get_state(dns[2]) == 'OPEN'

    cluster.restore_network()

    for container in containers:
        container.wait_until_all_replicas_are_closed()

    for container in containers:
        assert container.get_state(dns[0]) == 'CLOSED'
        assert container.get_state(dns[1]) == 'CLOSED'
        assert container.get_state(dns[2]) == 'CLOSED'

    exit_code, output = oz_client.run_freon(1, 1, 1, 10240)
    assert exit_code == 0, "freon run failed with output=[%s]" % output
