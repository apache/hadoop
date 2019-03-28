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
package org.apache.hadoop.hdds.scm.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.net.NetConstants.DEFAULT_NODEGROUP;
import static org.apache.hadoop.hdds.scm.net.NetConstants.DEFAULT_RACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test the node schema loader. */
public class TestNodeSchemaManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestNodeSchemaManager.class);
  private ClassLoader classLoader =
      Thread.currentThread().getContextClassLoader();
  private NodeSchemaManager manager;
  private Configuration conf;

  public TestNodeSchemaManager() {
    conf = new Configuration();
    String filePath = classLoader.getResource(
        "./networkTopologyTestFiles/good.xml").getPath();
    conf.set(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE, filePath);
    manager = NodeSchemaManager.getInstance();
    manager.init(conf);
  }

  @Rule
  public Timeout testTimeout = new Timeout(30000);

  @Test(expected = IllegalArgumentException.class)
  public void testFailure1() {
    manager.getCost(0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailure2() {
    manager.getCost(manager.getMaxLevel() + 1);
  }

  @Test
  public void testPass() {
    assertEquals(4, manager.getMaxLevel());
    for (int i  = 1; i <= manager.getMaxLevel(); i++) {
      assertTrue(manager.getCost(i) == 1 || manager.getCost(i) == 0);
    }
  }

  @Test
  public void testInitFailure() {
    String filePath = classLoader.getResource(
        "./networkTopologyTestFiles/good.xml").getPath() + ".backup";
    conf.set(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE, filePath);
    try {
      manager.init(conf);
      fail("should fail");
    } catch (Throwable e) {
      assertTrue(e.getMessage().contains("Fail to load schema file:" +
          filePath));
    }
  }

  @Test
  public void testComplete() {
    // successful complete action
    String path = "/node1";
    assertEquals(DEFAULT_RACK + DEFAULT_NODEGROUP + path,
        manager.complete(path));
    assertEquals("/rack" + DEFAULT_NODEGROUP + path,
        manager.complete("/rack" + path));
    assertEquals(DEFAULT_RACK + "/nodegroup" + path,
        manager.complete("/nodegroup" + path));

    // failed complete action
    assertEquals(null, manager.complete("/dc" + path));
  }
}
