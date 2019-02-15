/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.client;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.RestClient;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/** The same as {@link TestVolume} except that this test is Ratis enabled. */
@Ignore("Disabling Ratis tests for pipeline work.")
@RunWith(value = Parameterized.class)
public class TestVolumeRatis {
  @Rule
  public Timeout testTimeout = new Timeout(300000);
  private static ClientProtocol client;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  @Parameterized.Parameters
  public static Collection<Object[]> clientProtocol() {
    Object[][] params = new Object[][] {
        {RpcClient.class},
        {RestClient.class}};
    return Arrays.asList(params);
  }

  @Parameterized.Parameter
  @SuppressWarnings("visibilitymodifier")
  public Class clientProtocol;

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();

    // This enables Ratis in the cluster.
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY, true);


    String path = GenericTestUtils
        .getTempPath(TestVolume.class.getSimpleName());
    FileUtils.deleteDirectory(new File(path));

    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.DEBUG);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    final int port = cluster.getHddsDatanodes().get(0)
        .getDatanodeDetails()
        .getPort(DatanodeDetails.Port.Name.REST).getValue();
  }

  @Before
  public void setup() throws Exception {
    if (clientProtocol.equals(RestClient.class)) {
      client = new RestClient(conf);
    } else {
      client = new RpcClient(conf);
    }
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }

  }

  @Test
  public void testCreateVolume() throws Exception {
    TestVolume.runTestCreateVolume(client);
  }

  @Test
  public void testCreateDuplicateVolume() throws Exception {
    TestVolume.runTestCreateDuplicateVolume(client);
  }

  @Test
  public void testDeleteVolume() throws OzoneException, IOException {
    TestVolume.runTestDeleteVolume(client);
  }

  @Test
  public void testChangeOwnerOnVolume() throws Exception {
    TestVolume.runTestChangeOwnerOnVolume(client);
  }

  @Test
  public void testChangeQuotaOnVolume() throws Exception {
    TestVolume.runTestChangeQuotaOnVolume(client);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore("listVolumes not implemented in DistributedStorageHandler")
  @Test
  public void testListVolume() throws OzoneException, IOException {
    TestVolume.runTestListVolume(client);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore("See TestVolume.testListVolumePagination()")
  @Test
  public void testListVolumePagination() throws OzoneException, IOException {
    TestVolume.runTestListVolumePagination(client);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore("See TestVolume.testListAllVolumes()")
  @Test
  public void testListAllVolumes() throws Exception {
    TestVolume.runTestListAllVolumes(client);
  }

  @Ignore("Disabling Ratis tests for pipeline work.")
  @Test
  public void testListVolumes() throws Exception {
    TestVolume.runTestListVolumes(client);
  }
}
