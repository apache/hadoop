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
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;

import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;

/** The same as {@link TestVolume} except that this test is Ratis enabled. */
@Ignore("Disabling Ratis tests for pipeline work.")
public class TestVolumeRatis {
  @Rule
  public Timeout testTimeout = new Timeout(300000);
  private static OzoneRestClient ozoneClient;
  private static MiniOzoneClassicCluster cluster;

  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    // This enables Ratis in the cluster.
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY, true);


    String path = GenericTestUtils
        .getTempPath(TestVolume.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
    FileUtils.deleteDirectory(new File(path));

    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);
    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.DEBUG);

    cluster = new MiniOzoneClassicCluster.Builder(conf).numDataNodes(3)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    DataNode dataNode = cluster.getDataNodes().get(0);
    final int port = dataNode.getInfoPort();

    ozoneClient = new OzoneRestClient(
        String.format("http://localhost:%d", port));
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }

  }

  @Test
  public void testCreateVolume() throws Exception {
    TestVolume.runTestCreateVolume(ozoneClient);
  }

  @Test
  public void testCreateDuplicateVolume() throws OzoneException {
    TestVolume.runTestCreateDuplicateVolume(ozoneClient);
  }

  @Test
  public void testDeleteVolume() throws OzoneException {
    TestVolume.runTestDeleteVolume(ozoneClient);
  }

  @Test
  public void testChangeOwnerOnVolume() throws Exception {
    TestVolume.runTestChangeOwnerOnVolume(ozoneClient);
  }

  @Test
  public void testChangeQuotaOnVolume() throws Exception {
    TestVolume.runTestChangeQuotaOnVolume(ozoneClient);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore("listVolumes not implemented in DistributedStorageHandler")
  @Test
  public void testListVolume() throws OzoneException, IOException {
    TestVolume.runTestListVolume(ozoneClient);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore("See TestVolume.testListVolumePagination()")
  @Test
  public void testListVolumePagination() throws OzoneException, IOException {
    TestVolume.runTestListVolumePagination(ozoneClient);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore("See TestVolume.testListAllVolumes()")
  @Test
  public void testListAllVolumes() throws Exception {
    TestVolume.runTestListAllVolumes(ozoneClient);
  }

  @Ignore("Disabling Ratis tests for pipeline work.")
  @Test
  public void testListVolumes() throws Exception {
    TestVolume.runTestListVolumes(ozoneClient);
  }
}
