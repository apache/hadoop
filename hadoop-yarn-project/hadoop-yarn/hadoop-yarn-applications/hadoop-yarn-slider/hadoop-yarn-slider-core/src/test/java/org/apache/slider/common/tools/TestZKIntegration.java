/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.common.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.registry.server.services.MicroZookeeperServiceKeys;
import org.apache.slider.client.SliderClient;
import org.apache.slider.core.zk.ZKIntegration;
import org.apache.slider.utils.KeysForTests;
import org.apache.slider.utils.YarnZKMiniClusterTestBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Test ZK integration.
 */
public class TestZKIntegration extends YarnZKMiniClusterTestBase implements
    KeysForTests {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestZKIntegration.class);

  public static final String USER = KeysForTests.USERNAME;
  public static final int CONNECT_TIMEOUT = 5000;
  private ZKIntegration zki;

  @Before
  public void createCluster() {
    Configuration conf = getConfiguration();
    String name = methodName.getMethodName();
    File zkdir = new File("target/zk/${name}");
    FileUtil.fullyDelete(zkdir);
    conf.set(MicroZookeeperServiceKeys.KEY_ZKSERVICE_DIR, zkdir
        .getAbsolutePath());
    createMicroZKCluster("-"+ name, conf);
  }

  @After
  public void closeZKI() throws IOException {
    if (zki != null) {
      zki.close();
      zki = null;
    }
  }

  public ZKIntegration initZKI() throws IOException, InterruptedException {
    zki = createZKIntegrationInstance(
        getZKBinding(), methodName.getMethodName(), true, false,
        CONNECT_TIMEOUT);
    return zki;
  }

  @Test
  public void testListUserClustersWithoutAnyClusters() throws Throwable {
    assertHasZKCluster();
    initZKI();
    String userPath = ZKIntegration.mkSliderUserPath(USER);
    List<String> clusters = this.zki.getClusters();
    assertTrue(SliderUtils.isEmpty(clusters));
  }

  @Test
  public void testListUserClustersWithOneCluster() throws Throwable {
    assertHasZKCluster();

    initZKI();
    String userPath = ZKIntegration.mkSliderUserPath(USER);
    String fullPath = zki.createPath(userPath, "/cluster-",
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.EPHEMERAL_SEQUENTIAL);
    LOG.info("Ephemeral path {}", fullPath);
    List<String> clusters = zki.getClusters();
    assertEquals(1, clusters.size());
    assertTrue(fullPath.endsWith(clusters.get(0)));
  }

  @Test
  public void testListUserClustersWithTwoCluster() throws Throwable {
    initZKI();
    String userPath = ZKIntegration.mkSliderUserPath(USER);
    String c1 = createEphemeralChild(zki, userPath);
    LOG.info("Ephemeral path $c1");
    String c2 = createEphemeralChild(zki, userPath);
    LOG.info("Ephemeral path $c2");
    List<String> clusters = zki.getClusters();
    assertEquals(2, clusters.size());
    assertTrue((c1.endsWith(clusters.get(0)) && c2.endsWith(clusters.get(1))) ||
        (c1.endsWith(clusters.get(1)) && c2.endsWith(clusters.get(0))));
  }

  @Test
  public void testCreateAndDeleteDefaultZKPath() throws Throwable {
    MockSliderClient client = new MockSliderClient();

    String path = client.createZookeeperNodeInner("cl1", true);
    zki = client.getLastZKIntegration();

    String zkPath = ZKIntegration.mkClusterPath(USER, "cl1");
    assertEquals("zkPath must be as expected", zkPath,
        "/services/slider/users/" + USER + "/cl1");
    assertEquals(path, zkPath);
    assertNull("ZKIntegration should be null.", zki);
    zki = createZKIntegrationInstance(getZKBinding(), "cl1", true, false,
        CONNECT_TIMEOUT);
    assertFalse(zki.exists(zkPath));

    path = client.createZookeeperNodeInner("cl1", false);
    zki = client.getLastZKIntegration();
    assertNotNull(zki);
    assertEquals("zkPath must be as expected", zkPath,
        "/services/slider/users/" + USER + "/cl1");
    assertEquals(path, zkPath);
    assertTrue(zki.exists(zkPath));
    zki.createPath(zkPath, "/cn", ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode
        .PERSISTENT);
    assertTrue(zki.exists(zkPath + "/cn"));
    client.deleteZookeeperNode("cl1");
    assertFalse(zki.exists(zkPath));
  }

  public static String createEphemeralChild(ZKIntegration zki, String userPath)
      throws KeeperException, InterruptedException {
    return zki.createPath(userPath, "/cluster-",
                          ZooDefs.Ids.OPEN_ACL_UNSAFE,
                          CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  /**
   * Test slider client that overriddes ZK client.
   */
  public class MockSliderClient extends SliderClient {
    private ZKIntegration zki;

    @Override
    public String getUsername() {
      return USER;
    }

    @Override
    protected ZKIntegration getZkClient(String clusterName, String user) {
      try {
        zki = createZKIntegrationInstance(getZKBinding(), clusterName, true,
            false, CONNECT_TIMEOUT);
      } catch (Exception e) {
        fail("creating ZKIntergration threw an exception");
      }
      return zki;
    }

    @Override
    public Configuration getConfig() {
      return new Configuration();
    }

    public ZKIntegration getLastZKIntegration() {
      return zki;
    }

  }

}
