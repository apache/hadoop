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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import static org.junit.Assert.assertNotEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.hdfs.DFSConfigKeys;

public class TestMalformedURLs {
  private MiniDFSCluster cluster;
  Configuration config;

  @Before
  public void setUp() throws Exception {
    Configuration.addDefaultResource("hdfs-site.malformed.xml");
    config = new Configuration();
  }

  @Test
  public void testTryStartingCluster() throws Exception {
    // if we are able to start the cluster, it means
    // that we were able to read the configuration
    // correctly.

    assertNotEquals(config.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY),
      config.getTrimmed(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY));
    cluster = new MiniDFSCluster.Builder(config).build();
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
