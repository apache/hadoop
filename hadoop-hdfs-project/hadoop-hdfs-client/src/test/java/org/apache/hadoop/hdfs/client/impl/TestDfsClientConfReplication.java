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
package org.apache.hadoop.hdfs.client.impl;

import java.net.URI;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_REPLICATION_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test the per-cluster default replication, dfs.replication.CLUSTER.
 */
public class TestDfsClientConfReplication {

  private static final URI NS1 = URI.create("hdfs://ns1");
  private static final URI NS2 = URI.create("hdfs://ns2/path");
  private static final URI NN_WITH_PORT = URI.create("hdfs://nn1.example:8020");

  @Test
  public void testDefaultWhenUnset() {
    Configuration conf = new Configuration(false);
    assertReplication(DFS_REPLICATION_DEFAULT, conf, NS1);
    assertReplication(DFS_REPLICATION_DEFAULT, conf, null);
  }

  @Test
  public void testGlobalValueWithoutClusterValue() {
    Configuration conf = new Configuration(false);
    conf.setInt(DFS_REPLICATION_KEY, 2);
    assertReplication(2, conf, NS1);
    assertReplication(2, conf, null);
  }

  @Test
  public void testClusterValueOverridesGlobalValue() {
    Configuration conf = new Configuration(false);
    conf.setInt(DFS_REPLICATION_KEY, 3);
    conf.setInt(DFS_REPLICATION_KEY + ".ns1", 1);
    conf.setInt(DFS_REPLICATION_KEY + ".ns2", 2);
    assertReplication(1, conf, NS1);
    assertReplication(2, conf, NS2);
    // other clusters, or no URI, keep the global value
    assertReplication(3, conf, URI.create("hdfs://ns3"));
    assertReplication(3, conf, null);
  }

  @Test
  public void testClusterValueWithoutGlobalValue() {
    Configuration conf = new Configuration(false);
    conf.setInt(DFS_REPLICATION_KEY + ".ns1", 1);
    assertReplication(1, conf, NS1);
    assertReplication(DFS_REPLICATION_DEFAULT, conf, NS2);
  }

  @Test
  public void testClusterIsHostWithoutPort() {
    Configuration conf = new Configuration(false);
    conf.setInt(DFS_REPLICATION_KEY + ".nn1.example", 2);
    assertReplication(2, conf, NN_WITH_PORT);
  }

  private static void assertReplication(int expected, Configuration conf,
      URI uri) {
    assertEquals(expected,
        new DfsClientConf(conf, uri).getDefaultReplication(),
        "default replication for " + uri);
  }
}
