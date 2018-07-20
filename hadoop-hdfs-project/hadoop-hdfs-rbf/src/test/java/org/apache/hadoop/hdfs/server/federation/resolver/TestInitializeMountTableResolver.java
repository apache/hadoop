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
package org.apache.hadoop.hdfs.server.federation.resolver;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMESERVICES;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_DEFAULT_NAMESERVICE;
import static org.junit.Assert.assertEquals;

/**
 * Test {@link MountTableResolver} initialization.
 */
public class TestInitializeMountTableResolver {

  @Test
  public void testDefaultNameserviceIsMissing() {
    Configuration conf = new Configuration();
    MountTableResolver mountTable = new MountTableResolver(conf);
    assertEquals("", mountTable.getDefaultNamespace());
  }

  @Test
  public void testDefaultNameserviceWithEmptyString() {
    Configuration conf = new Configuration();
    conf.set(DFS_ROUTER_DEFAULT_NAMESERVICE, "");
    MountTableResolver mountTable = new MountTableResolver(conf);
    assertEquals("", mountTable.getDefaultNamespace());
  }

  @Test
  public void testRouterDefaultNameservice() {
    Configuration conf = new Configuration();
    conf.set(DFS_ROUTER_DEFAULT_NAMESERVICE, "router_ns"); // this is priority
    conf.set(DFS_NAMESERVICE_ID, "ns_id");
    conf.set(DFS_NAMESERVICES, "nss");
    MountTableResolver mountTable = new MountTableResolver(conf);
    assertEquals("router_ns", mountTable.getDefaultNamespace());
  }

  @Test
  public void testNameserviceID() {
    Configuration conf = new Configuration();
    conf.set(DFS_NAMESERVICE_ID, "ns_id"); // this is priority
    conf.set(DFS_NAMESERVICES, "nss");
    MountTableResolver mountTable = new MountTableResolver(conf);
    assertEquals("ns_id", mountTable.getDefaultNamespace());
  }

  @Test
  public void testSingleNameservices() {
    Configuration conf = new Configuration();
    conf.set(DFS_NAMESERVICES, "ns1");
    MountTableResolver mountTable = new MountTableResolver(conf);
    assertEquals("ns1", mountTable.getDefaultNamespace());
  }

  @Test
  public void testMultipleNameservices() {
    Configuration conf = new Configuration();
    conf.set(DFS_NAMESERVICES, "ns1,ns2");
    MountTableResolver mountTable = new MountTableResolver(conf);
    assertEquals("ns1", mountTable.getDefaultNamespace());
  }
}