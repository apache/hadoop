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
import util
from ozone.exceptions import ContainerNotFoundError


class Container:

    def __init__(self, container_id, cluster):
        self.container_id = container_id
        self.cluster = cluster

    def is_on(self, datanode):
        return self.cluster.is_container_replica_exist(self.container_id, datanode)

    def get_datanode_states(self):
        dns = self.cluster.get_container_datanodes(self.container_id)
        states = []
        for dn in dns:
            states.append(self.get_state(dn))
        return states

    def get_state(self, datanode):
        return self.cluster.get_container_state(self.container_id, datanode)

    def wait_until_replica_is_quasi_closed(self, datanode):
        def predicate():
            try:
                if self.cluster.get_container_state(self.container_id, datanode) == 'QUASI_CLOSED':
                    return True
                else:
                    return False
            except ContainerNotFoundError:
                return False

        util.wait_until(predicate, int(os.environ["CONTAINER_STATUS_SLEEP"]), 10)
        if not predicate():
            raise Exception("Replica is not quasi closed!")

    def wait_until_one_replica_is_quasi_closed(self):
        def predicate():
            dns = self.cluster.get_container_datanodes(self.container_id)
            for dn in dns:
                if self.cluster.get_container_state(self.container_id, dn) == 'QUASI_CLOSED':
                    return True
                else:
                    return False

        util.wait_until(predicate, int(os.environ["CONTAINER_STATUS_SLEEP"]), 10)
        if not predicate():
            raise Exception("None of the container replica is quasi closed!")

    def wait_until_replica_is_closed(self, datanode):
        def predicate():
            try:
                if self.cluster.get_container_state(self.container_id, datanode) == 'CLOSED':
                    return True
                else:
                    return False
            except ContainerNotFoundError:
                return False

        util.wait_until(predicate, int(os.environ["CONTAINER_STATUS_SLEEP"]), 10)
        if not predicate():
            raise Exception("Replica is not closed!")

    def wait_until_one_replica_is_closed(self):
        def predicate():
            dns = self.cluster.get_container_datanodes(self.container_id)
            for dn in dns:
                if self.cluster.get_container_state(self.container_id, dn) == 'CLOSED':
                    return True
            return False

        util.wait_until(predicate, int(os.environ["CONTAINER_STATUS_SLEEP"]), 10)
        if not predicate():
            raise Exception("None of the container replica is closed!")

    def wait_until_two_replicas_are_closed(self):
        def predicate():
            dns = self.cluster.get_container_datanodes(self.container_id)
            closed_count = 0
            for dn in dns:
                if self.cluster.get_container_state(self.container_id, dn) == 'CLOSED':
                    closed_count = closed_count + 1
            if closed_count > 1:
                return True
            return False

        util.wait_until(predicate, int(os.environ["CONTAINER_STATUS_SLEEP"]), 10)
        if not predicate():
            raise Exception("None of the container replica is closed!")

    def wait_until_all_replicas_are_closed(self):
        def predicate():
            try:
                dns = self.cluster.get_container_datanodes(self.container_id)
                for dn in dns:
                    if self.cluster.get_container_state(self.container_id, dn) != 'CLOSED':
                        return False
                return True
            except ContainerNotFoundError:
                return False

        util.wait_until(predicate, int(os.environ["CONTAINER_STATUS_SLEEP"]), 10)
        if not predicate():
            raise Exception("Not all the replicas are closed!")

    def wait_until_replica_is_not_open_anymore(self, datanode):
        def predicate():
            try:
                if self.cluster.get_container_state(self.container_id, datanode) != 'OPEN' and \
                  self.cluster.get_container_state(self.container_id, datanode) != 'CLOSING':
                    return True
                else:
                    return False
            except ContainerNotFoundError:
                return False

        util.wait_until(predicate, int(os.environ["CONTAINER_STATUS_SLEEP"]), 10)
        if not predicate():
            raise Exception("Replica is not closed!")
