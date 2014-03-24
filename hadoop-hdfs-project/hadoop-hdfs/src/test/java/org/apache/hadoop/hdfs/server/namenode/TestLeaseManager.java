/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;
import org.mockito.Mockito;


public class TestLeaseManager {
  final Configuration conf = new HdfsConfiguration();
  
  @Test
  public void testRemoveLeaseWithPrefixPath() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();

    LeaseManager lm = NameNodeAdapter.getLeaseManager(cluster.getNamesystem());
    lm.addLease("holder1", "/a/b");
    lm.addLease("holder2", "/a/c");
    assertNotNull(lm.getLeaseByPath("/a/b"));
    assertNotNull(lm.getLeaseByPath("/a/c"));

    lm.removeLeaseWithPrefixPath("/a");

    assertNull(lm.getLeaseByPath("/a/b"));
    assertNull(lm.getLeaseByPath("/a/c"));

    lm.addLease("holder1", "/a/b");
    lm.addLease("holder2", "/a/c");

    lm.removeLeaseWithPrefixPath("/a/");

    assertNull(lm.getLeaseByPath("/a/b"));
    assertNull(lm.getLeaseByPath("/a/c"));
  }
}
