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
package org.apache.hadoop.util.curator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the manager for ZooKeeper Curator.
 */
public class TestZKCuratorManager {

  private TestingServer server;
  private ZKCuratorManager curator;

  @Before
  public void setup() throws Exception {
    this.server = new TestingServer();

    Configuration conf = new Configuration();
    conf.set(
        CommonConfigurationKeys.ZK_ADDRESS, this.server.getConnectString());

    this.curator = new ZKCuratorManager(conf);
    this.curator.start();
  }

  @After
  public void teardown() throws Exception {
    this.curator.close();
    if (this.server != null) {
      this.server.close();
      this.server = null;
    }
  }

  @Test
  public void testReadWriteData() throws Exception {
    String testZNode = "/test";
    String expectedString = "testString";
    assertFalse(curator.exists(testZNode));
    curator.create(testZNode);
    assertTrue(curator.exists(testZNode));
    curator.setData(testZNode, expectedString, -1);
    String testString = curator.getSringData("/test");
    assertEquals(expectedString, testString);
  }

  @Test
  public void testChildren() throws Exception {
    List<String> children = curator.getChildren("/");
    assertEquals(1, children.size());

    assertFalse(curator.exists("/node1"));
    curator.create("/node1");
    assertTrue(curator.exists("/node1"));

    assertFalse(curator.exists("/node2"));
    curator.create("/node2");
    assertTrue(curator.exists("/node2"));

    children = curator.getChildren("/");
    assertEquals(3, children.size());

    curator.delete("/node2");
    assertFalse(curator.exists("/node2"));
    children = curator.getChildren("/");
    assertEquals(2, children.size());
  }
}