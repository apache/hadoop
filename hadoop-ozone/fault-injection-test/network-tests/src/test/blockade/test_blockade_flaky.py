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
import random
import pytest

from ozone.blockade import Blockade
from ozone.cluster import OzoneCluster


logger = logging.getLogger(__name__)


def setup_function():
    global cluster
    cluster = OzoneCluster.create()
    cluster.start()


def teardown_function():
    cluster.stop()


@pytest.mark.parametrize("flaky_node", ["datanode"])
def test_flaky(flaky_node):
    """
    In these tests, we make the network of the nodes as flaky using blockade.
    One of the DNs selected randomly and network of the DN is made flaky.

    Once HA is in place, we can go ahead and make OM and SCM network flaky.

    """
    flaky_container_name = {
        "scm": cluster.scm,
        "om": cluster.om,
        "datanode": random.choice(cluster.datanodes),
        "all": "--all"
    }[flaky_node]

    Blockade.make_flaky(flaky_container_name)
    exit_code, output = cluster.get_client().run_freon(1, 1, 1, 10240)
    assert exit_code == 0, "freon run failed with output=[%s]" % output
