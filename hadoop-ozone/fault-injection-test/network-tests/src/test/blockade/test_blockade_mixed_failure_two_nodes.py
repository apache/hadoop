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

import logging

from ozone.cluster import OzoneCluster

logger = logging.getLogger(__name__)


def setup_function():
    global cluster
    cluster = OzoneCluster.create()
    cluster.start()


def teardown_function():
    cluster.stop()


def test_two_dns_isolate_scm_same_partition():
    """
    In this test, there are three DNs,
    DN1 is on a network partition and
    DN2, DN3 are on a different network partition.
    DN2 and DN3 cannot communicate with SCM.
    Expectation :
    The container replica state in DN1 should be quasi-closed.
    The container replica state in DN2 should be open.
    The container replica state in DN3 should be open.
    """
    om = cluster.om
    scm = cluster.scm
    dns = cluster.datanodes
    client = cluster.client
    oz_client = cluster.get_client()

    oz_client.run_freon(1, 1, 1, 10240)

    first_set = [om, dns[1], dns[2], client]
    second_set = [om, scm, dns[0], client]
    cluster.partition_network(first_set, second_set)
    oz_client.run_freon(1, 1, 1, 10240)

    containers = cluster.get_containers_on_datanode(dns[0])

    for container in containers:
        container.wait_until_one_replica_is_quasi_closed()

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


def test_two_dns_isolate_scm_different_partition():
    """
    In this test, there are three DNs,
    DN1 is on a network partition and
    DN2, DN3 are on a different network partition.
    DN1 and DN2 cannot communicate with SCM.
    Expectation :
    The container replica state in DN1 should be open.
    The container replica states can be either 'closed'
    in DN2 and DN3 or 'open' in DN2 and 'quasi-closed' in DN3.
    """

    om = cluster.om
    scm = cluster.scm
    dns = cluster.datanodes
    client = cluster.client
    oz_client = cluster.get_client()

    oz_client.run_freon(1, 1, 1, 10240)

    first_set = [om, dns[0], client]
    second_set = [om, dns[1], dns[2], client]
    third_set = [scm, dns[2], client]
    cluster.partition_network(first_set, second_set, third_set)
    oz_client.run_freon(1, 1, 1, 10240)

    containers = cluster.get_containers_on_datanode(dns[2])

    for container in containers:
        container.wait_until_replica_is_not_open_anymore(dns[2])

    for container in containers:
        assert container.get_state(dns[0]) == 'OPEN'
        assert (container.get_state(dns[1]) == 'CLOSED' and
                container.get_state(dns[2]) == 'CLOSED') or \
               (container.get_state(dns[1]) == 'OPEN' and
                container.get_state(dns[2]) == 'QUASI_CLOSED')

    cluster.restore_network()

    for container in containers:
        container.wait_until_all_replicas_are_closed()

    for container in containers:
        assert container.get_state(dns[0]) == 'CLOSED'
        assert container.get_state(dns[1]) == 'CLOSED'
        assert container.get_state(dns[2]) == 'CLOSED'

    exit_code, output = oz_client.run_freon(1, 1, 1, 10240)
    assert exit_code == 0, "freon run failed with output=[%s]" % output
