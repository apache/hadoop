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
import util
from ozone.cluster import Cluster

logger = logging.getLogger(__name__)

def setup_function(function):
  global cluster
  cluster = Cluster.create()
  cluster.start()


def teardown_function(function):
  cluster.stop()


def test_isolate_single_datanode():
  """
  In this test case we will create a network partition in such a way that
  one of the datanode will not be able to communicate with other datanodes
  but it will be able to communicate with SCM.

  Once the network partition happens, SCM detects it and closes the pipeline,
  which in-turn closes the containers.

  The container on the first two datanode will get CLOSED as they have quorum.
  The container replica on the third node will be QUASI_CLOSED as it is not
  able to connect with the other datanodes and it doesn't have latest BCSID.

  Once we restore the network, the stale replica on the third datanode will be
  deleted and a latest replica will be copied from any one of the other
  datanodes.

  """
  cluster.run_freon(1, 1, 1, 10240)
  first_set = [cluster.om, cluster.scm,
               cluster.datanodes[0], cluster.datanodes[1]]
  second_set = [cluster.om, cluster.scm, cluster.datanodes[2]]
  logger.info("Partitioning the network")
  cluster.partition_network(first_set, second_set)
  cluster.run_freon(1, 1, 1, 10240)
  logger.info("Waiting for container to be QUASI_CLOSED")

  util.wait_until(lambda: cluster.get_container_states(cluster.datanodes[2])
                  .popitem()[1] == 'QUASI_CLOSED',
                  int(os.environ["CONTAINER_STATUS_SLEEP"]), 10)
  container_states_dn_0 = cluster.get_container_states(cluster.datanodes[0])
  container_states_dn_1 = cluster.get_container_states(cluster.datanodes[1])
  container_states_dn_2 = cluster.get_container_states(cluster.datanodes[2])
  assert len(container_states_dn_0) != 0
  assert len(container_states_dn_1) != 0
  assert len(container_states_dn_2) != 0
  for key in container_states_dn_0:
    assert container_states_dn_0.get(key) == 'CLOSED'
  for key in container_states_dn_1:
    assert container_states_dn_1.get(key) == 'CLOSED'
  for key in container_states_dn_2:
    assert container_states_dn_2.get(key) == 'QUASI_CLOSED'

  # Since the replica in datanode[2] doesn't have the latest BCSID,
  # ReplicationManager will delete it and copy a closed replica.
  # We will now restore the network and datanode[2] should get a
  # closed replica of the container
  logger.info("Restoring the network")
  cluster.restore_network()

  logger.info("Waiting for the replica to be CLOSED")
  util.wait_until(
    lambda: cluster.container_state_predicate(cluster.datanodes[2], 'CLOSED'),
    int(os.environ["CONTAINER_STATUS_SLEEP"]), 10)
  container_states_dn_2 = cluster.get_container_states(cluster.datanodes[2])
  assert len(container_states_dn_2) != 0
  for key in container_states_dn_2:
    assert container_states_dn_2.get(key) == 'CLOSED'


def test_datanode_isolation_all():
  """
  In this test case we will create a network partition in such a way that
  all datanodes cannot communicate with each other.
  All datanodes will be able to communicate with SCM.

  Once the network partition happens, SCM detects it and closes the pipeline,
  which in-turn tries to close the containers.
  At least one of the replica should be in closed state

  Once we restore the network, there will be three closed replicas.

  """
  cluster.run_freon(1, 1, 1, 10240)

  assert len(cluster.get_container_states(cluster.datanodes[0])) != 0
  assert len(cluster.get_container_states(cluster.datanodes[1])) != 0
  assert len(cluster.get_container_states(cluster.datanodes[2])) != 0

  logger.info("Partitioning the network")
  first_set = [cluster.om, cluster.scm, cluster.datanodes[0]]
  second_set = [cluster.om, cluster.scm, cluster.datanodes[1]]
  third_set = [cluster.om, cluster.scm, cluster.datanodes[2]]
  cluster.partition_network(first_set, second_set, third_set)

  logger.info("Waiting for the replica to be CLOSED")
  util.wait_until(
    lambda: cluster.container_state_predicate_one_closed(cluster.datanodes),
    int(os.environ["CONTAINER_STATUS_SLEEP"]), 10)

  # At least one of the replica should be in closed state
  assert cluster.container_state_predicate_one_closed(cluster.datanodes)

  # After restoring the network all the replicas should be in
  # CLOSED state
  logger.info("Restoring the network")
  cluster.restore_network()

  logger.info("Waiting for the container to be replicated")
  util.wait_until(
    lambda: cluster.container_state_predicate_all_closed(cluster.datanodes),
    int(os.environ["CONTAINER_STATUS_SLEEP"]), 10)
  assert cluster.container_state_predicate_all_closed(cluster.datanodes)
