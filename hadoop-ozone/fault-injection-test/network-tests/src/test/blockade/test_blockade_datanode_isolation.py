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
from ozone.exceptions import ContainerNotFoundError

logger = logging.getLogger(__name__)


def setup_function():
    global cluster
    cluster = OzoneCluster.create()
    cluster.start()


def teardown_function():
    cluster.stop()


def test_isolate_single_datanode():
    """
    In this test case we will create a network partition in such a way that
    one of the DN will not be able to communicate with other datanodes
    but it will be able to communicate with SCM.

    Once the network partition happens, SCM detects it and closes the pipeline,
    which in-turn closes the containers.

    The container on the first two DN will get CLOSED as they have quorum.
    The container replica on the third node will be QUASI_CLOSED as it is not
    able to connect with the other DNs and it doesn't have latest BCSID.

    Once we restore the network, the stale replica on the third DN will be
    deleted and a latest replica will be copied from any one of the other
    DNs.

    """
    om = cluster.om
    scm = cluster.scm
    dns = cluster.datanodes
    client = cluster.client
    oz_client = cluster.get_client()

    oz_client.run_freon(1, 1, 1, 10240)

    # Partition the network
    first_set = [om, scm, dns[0], dns[1], client]
    second_set = [om, scm, dns[2], client]
    logger.info("Partitioning the network")
    cluster.partition_network(first_set, second_set)

    oz_client.run_freon(1, 1, 1, 10240)

    containers = cluster.get_containers_on_datanode(dns[0])

    # The same set of containers should also be in datanode[2]

    for container in containers:
        assert container.is_on(dns[2])

    logger.info("Waiting for container to be CLOSED")
    for container in containers:
        container.wait_until_one_replica_is_closed()

    for container in containers:
        assert container.get_state(dns[0]) == 'CLOSED'
        assert container.get_state(dns[1]) == 'CLOSED'
        try:
            assert container.get_state(dns[2]) == 'CLOSING' or \
                   container.get_state(dns[2]) == 'QUASI_CLOSED'
        except ContainerNotFoundError:
            assert True

    # Since the replica in datanode[2] doesn't have the latest BCSID,
    # ReplicationManager will delete it and copy a closed replica.
    # We will now restore the network and datanode[2] should get a
    # closed replica of the container
    logger.info("Restoring the network")
    cluster.restore_network()

    logger.info("Waiting for the replica to be CLOSED")
    for container in containers:
        container.wait_until_replica_is_closed(dns[2])

    for container in containers:
        assert container.get_state(dns[0]) == 'CLOSED'
        assert container.get_state(dns[1]) == 'CLOSED'
        assert container.get_state(dns[2]) == 'CLOSED'

    exit_code, output = oz_client.run_freon(1, 1, 1, 10240)
    assert exit_code == 0, "freon run failed with output=[%s]" % output


def test_datanode_isolation_all():
    """
    In this test case we will create a network partition in such a way that
    all DNs cannot communicate with each other.
    All DNs will be able to communicate with SCM.

    Once the network partition happens, SCM detects it and closes the pipeline,
    which in-turn tries to close the containers.
    At least one of the replica should be in closed state

    Once we restore the network, there will be three closed replicas.

    """
    om = cluster.om
    scm = cluster.scm
    dns = cluster.datanodes
    client = cluster.client
    oz_client = cluster.get_client()

    oz_client.run_freon(1, 1, 1, 10240)

    logger.info("Partitioning the network")
    first_set = [om, scm, dns[0], client]
    second_set = [om, scm, dns[1], client]
    third_set = [om, scm, dns[2], client]
    cluster.partition_network(first_set, second_set, third_set)

    containers = cluster.get_containers_on_datanode(dns[0])
    container = containers.pop()

    logger.info("Waiting for a replica to be CLOSED")
    container.wait_until_one_replica_is_closed()

    # At least one of the replica should be in closed state
    assert 'CLOSED' in container.get_datanode_states()

    logger.info("Restoring the network")
    cluster.restore_network()

    logger.info("Waiting for the container to be replicated")
    container.wait_until_all_replicas_are_closed()
    # After restoring the network all the replicas should be in CLOSED state
    for state in container.get_datanode_states():
        assert state == 'CLOSED'

    exit_code, output = oz_client.run_freon(1, 1, 1, 10240)
    assert exit_code == 0, "freon run failed with output=[%s]" % output
