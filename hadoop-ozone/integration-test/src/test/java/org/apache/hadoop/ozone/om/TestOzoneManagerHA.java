/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;


import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.rpc.ha.OMProxyInfo;
import org.apache.hadoop.ozone.client.rpc.ha.OMProxyProvider;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.MiniOzoneHAClusterImpl
    .NODE_FAILURE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;

/**
 * Test Ozone Manager operation in distributed handler scenario.
 */
public class TestOzoneManagerHA {

  private MiniOzoneHAClusterImpl cluster = null;
  private StorageHandler storageHandler;
  private UserArgs userArgs;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private int numOfOMs = 3;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public Timeout timeout = new Timeout(60_000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    cluster.waitForClusterToBeReady();
    storageHandler = new ObjectStoreHandler(conf).getStorageHandler();
    userArgs = new UserArgs(null, OzoneUtils.getRequestID(),
        null, null, null, null);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test a client request when all OM nodes are running. The request should
   * succeed.
   * @throws Exception
   */
  @Test
  public void testAllOMNodesRunning() throws Exception {
    testCreateVolume(true);
  }

  /**
   * Test client request succeeds even if one OM is down.
   */
  @Test
  public void testOneOMNodeDown() throws Exception {
    cluster.stopOzoneManager(1);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 2);

    testCreateVolume(true);
  }

  /**
   * Test client request fails when 2 OMs are down.
   */
  @Test
  @Ignore("TODO:HDDS-1158")
  public void testTwoOMNodesDown() throws Exception {
    cluster.stopOzoneManager(1);
    cluster.stopOzoneManager(2);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 2);

    testCreateVolume(false);
  }

  /**
   * Create a volume and test its attribute.
   */
  private void testCreateVolume(boolean checkSuccess) throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);

    try {
      storageHandler.createVolume(createVolumeArgs);

      VolumeArgs getVolumeArgs = new VolumeArgs(volumeName, userArgs);
      VolumeInfo retVolumeinfo = storageHandler.getVolumeInfo(getVolumeArgs);

      if (checkSuccess) {
        Assert.assertTrue(retVolumeinfo.getVolumeName().equals(volumeName));
        Assert.assertTrue(retVolumeinfo.getOwner().getName().equals(userName));
      } else {
        // Verify that the request failed
        Assert.assertTrue(retVolumeinfo.getVolumeName().isEmpty());
        Assert.fail("There is no quorum. Request should have failed");
      }
    } catch (OMException e) {
      if (!checkSuccess) {
        GenericTestUtils.assertExceptionContains(
            "RaftRetryFailureException", e);
      } else {
        throw e;
      }
    }
  }

  /**
   * Test that OMProxyProvider creates an OM proxy for each OM in the cluster.
   */
  @Test
  public void testOMClientProxyProvide() throws Exception {
    OzoneClient rpcClient = cluster.getRpcClient();
    OMProxyProvider omProxyProvider =
        rpcClient.getObjectStore().getClientProxy().getOMProxyProvider();
    List<OMProxyInfo> omProxies = omProxyProvider.getOMProxies();

    Assert.assertEquals(numOfOMs, omProxies.size());

    for (int i = 0; i < numOfOMs; i++) {
      InetSocketAddress omRpcServerAddr =
          cluster.getOzoneManager(i).getOmRpcServerAddr();
      boolean omClientProxyExists = false;
      for (OMProxyInfo omProxyInfo : omProxies) {
        if (omProxyInfo.getAddress().equals(omRpcServerAddr)) {
          omClientProxyExists = true;
          break;
        }
      }
      Assert.assertTrue("There is no OM Client Proxy corresponding to OM " +
              "node" + cluster.getOzoneManager(i).getOMNodId(),
          omClientProxyExists);
    }
  }
}
