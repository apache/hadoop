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

import StringIO
import unittest

from hadoop.cloud.providers.rackspace import RackspaceCluster

class TestCluster(unittest.TestCase):

  class DriverStub(object):
    def list_nodes(self):
      class NodeStub(object):
        def __init__(self, name, metadata):
          self.id = name
          self.name = name
          self.state = 'ACTIVE'
          self.public_ip = ['100.0.0.1']
          self.private_ip = ['10.0.0.1']
          self.extra = { 'metadata': metadata }
      return [NodeStub('random_instance', {}),
              NodeStub('cluster1-nj-000', {'cluster': 'cluster1', 'roles': 'nn,jt'}),
              NodeStub('cluster1-dt-000', {'cluster': 'cluster1', 'roles': 'dn,tt'}),
              NodeStub('cluster1-dt-001', {'cluster': 'cluster1', 'roles': 'dn,tt'}),
              NodeStub('cluster2-dt-000', {'cluster': 'cluster2', 'roles': 'dn,tt'}),
              NodeStub('cluster3-nj-000', {'cluster': 'cluster3', 'roles': 'nn,jt'})]

  def test_get_clusters_with_role(self):
    self.assertEqual(set(['cluster1', 'cluster2']),
      RackspaceCluster.get_clusters_with_role('dn', 'running',
                                           TestCluster.DriverStub()))
    
  def test_get_instances_in_role(self):
    cluster = RackspaceCluster('cluster1', None, TestCluster.DriverStub())
    
    instances = cluster.get_instances_in_role('nn')
    self.assertEquals(1, len(instances))
    self.assertEquals('cluster1-nj-000', instances[0].id)

    instances = cluster.get_instances_in_role('tt')
    self.assertEquals(2, len(instances))
    self.assertEquals(set(['cluster1-dt-000', 'cluster1-dt-001']),
                      set([i.id for i in instances]))
    
  def test_print_status(self):
    cluster = RackspaceCluster('cluster1', None, TestCluster.DriverStub())
    
    out = StringIO.StringIO()
    cluster.print_status(None, "running", out)
    self.assertEquals("""nn,jt cluster1-nj-000 cluster1-nj-000 100.0.0.1 10.0.0.1 running
dn,tt cluster1-dt-000 cluster1-dt-000 100.0.0.1 10.0.0.1 running
dn,tt cluster1-dt-001 cluster1-dt-001 100.0.0.1 10.0.0.1 running
""", out.getvalue().replace("\t", " "))

    out = StringIO.StringIO()
    cluster.print_status(["dn"], "running", out)
    self.assertEquals("""dn,tt cluster1-dt-000 cluster1-dt-000 100.0.0.1 10.0.0.1 running
dn,tt cluster1-dt-001 cluster1-dt-001 100.0.0.1 10.0.0.1 running
""", out.getvalue().replace("\t", " "))

if __name__ == '__main__':
  unittest.main()