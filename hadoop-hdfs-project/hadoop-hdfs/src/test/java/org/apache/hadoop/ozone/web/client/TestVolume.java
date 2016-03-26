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

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestVolume {
  private static MiniDFSCluster cluster = null;
  private static int port = 0;
  private static OzoneClient client = null;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting DFS_OBJECTSTORE_ENABLED_KEY = true and
   * DFS_STORAGE_HANDLER_TYPE_KEY = "local" , which uses a local directory to
   * emulate Ozone backend.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws IOException, OzoneException,
      URISyntaxException {
    OzoneConfiguration conf = new OzoneConfiguration();

    URL p = conf.getClass().getResource("");
    String path = p.getPath().concat(TestVolume.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT,
        OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT_DEFAULT);

    conf.set(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT, path);
    conf.setBoolean(OzoneConfigKeys.DFS_OBJECTSTORE_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.DFS_STORAGE_HANDLER_TYPE_KEY, "local");
    conf.setBoolean(OzoneConfigKeys.DFS_OBJECTSTORE_TRACE_ENABLED_KEY, true);
    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.DEBUG);

    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    DataNode dataNode = cluster.getDataNodes().get(0);
    port = dataNode.getInfoPort();

    client = new OzoneClient(String.format("http://localhost:%d", port));
  }

  /**
   * shutdown MiniDFSCluster
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCreateVolume() throws OzoneException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");

    assertEquals(vol.getVolumeName(), volumeName);
    assertEquals(vol.getCreatedby(), "hdfs");
    assertEquals(vol.getOwnerName(), "bilbo");
    assertEquals(vol.getQuota().getUnit(), OzoneQuota.Units.TB);
    assertEquals(vol.getQuota().getSize(), 100);
  }

  @Test
  public void testCreateDuplicateVolume() throws OzoneException {
    try {
      client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
      client.createVolume("testvol", "bilbo", "100TB");
      client.createVolume("testvol", "bilbo", "100TB");
      assertFalse(true);
    } catch (OzoneException ex) {
      // OZone will throw saying volume already exists
      assertEquals(ex.getShortMessage(),"volumeAlreadyExists");
    }
  }

  @Test
  public void testDeleteVolume() throws OzoneException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    client.deleteVolume(vol.getVolumeName());
  }

  @Test
  public void testChangeOwnerOnVolume() throws OzoneException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    client.setVolumeOwner(volumeName, "frodo");
    OzoneVolume newVol = client.getVolume(volumeName);
    assertEquals(newVol.getOwnerName(), "frodo");
  }

  @Test
  public void testChangeQuotaOnVolume() throws OzoneException, IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    client.setVolumeQuota(volumeName, "1000MB");
    OzoneVolume newVol = client.getVolume(volumeName);
    assertEquals(newVol.getQuota().getSize(), 1000);
    assertEquals(newVol.getQuota().getUnit(), OzoneQuota.Units.MB);
  }

  @Test
  public void testListVolume() throws OzoneException, IOException {
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    for (int x = 0; x < 10; x++) {
      String volumeName = OzoneUtils.getRequestID().toLowerCase();
      OzoneVolume vol = client.createVolume(volumeName, "frodo", "100TB");
      assertNotNull(vol);
    }

    List<OzoneVolume> ovols = client.listVolumes("frodo");
    assertTrue(ovols.size() >= 10);
  }

}
