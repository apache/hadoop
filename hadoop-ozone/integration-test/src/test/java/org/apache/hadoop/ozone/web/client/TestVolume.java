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

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.RestClient;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test Ozone Volumes Lifecycle.
 */
@RunWith(value = Parameterized.class)
public class TestVolume {
  private static MiniOzoneCluster cluster = null;
  private static ClientProtocol client = null;
  private static OzoneConfiguration conf;

  @Parameterized.Parameters
  public static Collection<Object[]> clientProtocol() {
    Object[][] params = new Object[][] {
        {RpcClient.class}};
    return Arrays.asList(params);
  }

  @SuppressWarnings("visibilitymodifier")
  @Parameterized.Parameter
  public Class clientProtocol;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();

    String path = GenericTestUtils
        .getTempPath(TestVolume.class.getSimpleName());
    FileUtils.deleteDirectory(new File(path));

    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.DEBUG);

    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
  }

  @Before
  public void setup() throws Exception {
    if (clientProtocol.equals(RestClient.class)) {
      client = new RestClient(conf);
    } else {
      client = new RpcClient(conf);
    }
  }

  /**
   * shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCreateVolume() throws Exception {
    runTestCreateVolume(client);
  }

  static void runTestCreateVolume(ClientProtocol clientProtocol)
      throws OzoneException, IOException, ParseException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();

    long currentTime = Time.now();

    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner("bilbo")
        .setQuota("100TB")
        .setAdmin("hdfs")
        .build();
    clientProtocol.createVolume(volumeName, volumeArgs);
    OzoneVolume vol = clientProtocol.getVolumeDetails(volumeName);

    assertEquals(vol.getName(), volumeName);
    assertEquals(vol.getAdmin(), "hdfs");
    assertEquals(vol.getOwner(), "bilbo");
    assertEquals(vol.getQuota(), OzoneQuota.parseQuota("100TB").sizeInBytes());

    // verify the key creation time
    assertTrue((vol.getCreationTime()
        / 1000) >= (currentTime / 1000));

    // Test create a volume with invalid volume name,
    // not use Rule here because the test method is static.
    try {
      String invalidVolumeName = "#" + OzoneUtils.getRequestID().toLowerCase();
      clientProtocol.createVolume(invalidVolumeName);
      /*
      //TODO: RestClient and RpcClient should use HddsClientUtils to verify name
      fail("Except the volume creation be failed because the"
          + " volume name starts with an invalid char #");*/
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Bucket or Volume name"
          + " has an unsupported character : #"));
    }
  }

  @Test
  public void testCreateDuplicateVolume() throws Exception {
    runTestCreateDuplicateVolume(client);
  }

  static void runTestCreateDuplicateVolume(ClientProtocol clientProtocol)
      throws Exception {

    clientProtocol.createVolume("testvol");
    OzoneTestUtils.expectOmException(ResultCodes.VOLUME_ALREADY_EXISTS,
        () -> clientProtocol.createVolume("testvol"));
  }

  @Test
  public void testDeleteVolume() throws OzoneException, IOException {
    runTestDeleteVolume(client);
  }

  static void runTestDeleteVolume(ClientProtocol clientProtocol)
      throws OzoneException, IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    clientProtocol.createVolume(volumeName);
    clientProtocol.deleteVolume(volumeName);
  }

  @Test
  public void testChangeOwnerOnVolume() throws Exception {
    runTestChangeOwnerOnVolume(client);
  }

  static void runTestChangeOwnerOnVolume(ClientProtocol clientProtocol)
      throws OzoneException, ParseException, IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    clientProtocol.createVolume(volumeName);
    clientProtocol.getVolumeDetails(volumeName);
    clientProtocol.setVolumeOwner(volumeName, "frodo");
    OzoneVolume newVol = clientProtocol.getVolumeDetails(volumeName);
    assertEquals(newVol.getOwner(), "frodo");
    // verify if the creation time is missing after setting owner operation
    assertTrue(newVol.getCreationTime() > 0);
  }

  @Test
  public void testChangeQuotaOnVolume() throws Exception {
    runTestChangeQuotaOnVolume(client);
  }

  static void runTestChangeQuotaOnVolume(ClientProtocol clientProtocol)
      throws OzoneException, IOException, ParseException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    clientProtocol.createVolume(volumeName);
    clientProtocol.setVolumeQuota(volumeName, OzoneQuota.parseQuota("1000MB"));
    OzoneVolume newVol = clientProtocol.getVolumeDetails(volumeName);
    assertEquals(newVol.getQuota(),
        OzoneQuota.parseQuota("1000MB").sizeInBytes());
    // verify if the creation time is missing after setting quota operation
    assertTrue(newVol.getCreationTime() > 0);
  }

  // Listing all volumes in the cluster feature has to be fixed after HDDS-357.
  // TODO: fix this
  @Ignore
  @Test
  public void testListVolume() throws OzoneException, IOException {
    runTestListVolume(client);
  }

  static void runTestListVolume(ClientProtocol clientProtocol)
      throws OzoneException, IOException {
    for (int x = 0; x < 10; x++) {
      String volumeName = OzoneUtils.getRequestID().toLowerCase();
      clientProtocol.createVolume(volumeName);
    }

    List<OzoneVolume> ovols = clientProtocol.listVolumes(null, null, 100);
    assertTrue(ovols.size() >= 10);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore("Takes 3m to run, disable for now.")
  @Test
  public void testListVolumePagination() throws OzoneException, IOException {
    runTestListVolumePagination(client);
  }

  static void runTestListVolumePagination(ClientProtocol clientProtocol)
      throws OzoneException, IOException {
    final int volCount = 2000;
    final int step = 100;
    for (int x = 0; x < volCount; x++) {
      String volumeName = OzoneUtils.getRequestID().toLowerCase();
      clientProtocol.createVolume(volumeName);
    }
    String prevKey = null;
    int count = 0;
    int pagecount = 0;
    while (count < volCount) {
      List<OzoneVolume> ovols = clientProtocol.listVolumes(null, prevKey, step);
      count += ovols.size();
      prevKey = ovols.get(ovols.size() - 1).getName();
      pagecount++;
    }
    assertEquals(volCount / step, pagecount);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore
  @Test
  public void testListAllVolumes() throws OzoneException, IOException {
    runTestListAllVolumes(client);
  }

  static void runTestListAllVolumes(ClientProtocol clientProtocol)
      throws OzoneException, IOException {
    final int volCount = 200;
    final int step = 10;
    for (int x = 0; x < volCount; x++) {
      String userName =
          "frodo" + RandomStringUtils.randomAlphabetic(5).toLowerCase();
      String volumeName =
          "vol" + RandomStringUtils.randomAlphabetic(5).toLowerCase();
      VolumeArgs volumeArgs = VolumeArgs.newBuilder()
          .setOwner(userName)
          .setQuota("100TB")
          .setAdmin("hdfs")
          .build();
      clientProtocol.createVolume(volumeName, volumeArgs);
      OzoneVolume vol = clientProtocol.getVolumeDetails(volumeName);
      assertNotNull(vol);
    }
    String prevKey = null;
    int count = 0;
    int pagecount = 0;
    while (count < volCount) {
      List<OzoneVolume> ovols = clientProtocol.listVolumes(null, prevKey, step);
      count += ovols.size();
      if (ovols.size() > 0) {
        prevKey = ovols.get(ovols.size() - 1).getName();
      }
      pagecount++;
    }
    // becasue we are querying an existing ozone store, there will
    // be volumes created by other tests too. So we should get more page counts.
    assertEquals(volCount / step, pagecount);
  }

  // Listing all volumes in the cluster feature has to be fixed after HDDS-357.
  // TODO: fix this
  @Ignore
  @Test
  public void testListVolumes() throws Exception {
    runTestListVolumes(client);
  }

  static void runTestListVolumes(ClientProtocol clientProtocol)
      throws OzoneException, IOException, ParseException {
    final int volCount = 20;
    final String user1 = "test-user-a";
    final String user2 = "test-user-b";

    long currentTime = Time.now();
    // Create 20 volumes, 10 for user1 and another 10 for user2.
    for (int x = 0; x < volCount; x++) {
      String volumeName;
      String userName;

      if (x % 2 == 0) {
        // create volume [test-vol0, test-vol2, ..., test-vol18] for user1
        userName = user1;
        volumeName = "test-vol" + x;
      } else {
        // create volume [test-vol1, test-vol3, ..., test-vol19] for user2
        userName = user2;
        volumeName = "test-vol" + x;
      }
      VolumeArgs volumeArgs = VolumeArgs.newBuilder()
          .setOwner(userName)
          .setQuota("100TB")
          .setAdmin("hdfs")
          .build();
      clientProtocol.createVolume(volumeName, volumeArgs);
      OzoneVolume vol = clientProtocol.getVolumeDetails(volumeName);
      assertNotNull(vol);
    }

    // list all the volumes belong to user1
    List<OzoneVolume> volumeList =
        clientProtocol.listVolumes(user1, null, null, 100);
    assertEquals(10, volumeList.size());
    // verify the owner name and creation time of volume
    for (OzoneVolume vol : volumeList) {
      assertTrue(vol.getOwner().equals(user1));
      assertTrue((vol.getCreationTime()
          / 1000) >= (currentTime / 1000));
    }

    // test max key parameter of listing volumes
    volumeList = clientProtocol.listVolumes(user1, null, null, 2);
    assertEquals(2, volumeList.size());

    // test prefix parameter of listing volumes
    volumeList = clientProtocol.listVolumes(user1, "test-vol10", null, 10);
    assertTrue(volumeList.size() == 1
        && volumeList.get(0).getName().equals("test-vol10"));

    volumeList = clientProtocol.listVolumes(user1, "test-vol1", null, 10);
    assertEquals(5, volumeList.size());

    // test start key parameter of listing volumes
    volumeList = clientProtocol.listVolumes(user2, null, "test-vol15", 10);
    assertEquals(2, volumeList.size());

    String volumeName;
    for (int x = 0; x < volCount; x++) {
      volumeName = "test-vol" + x;
      clientProtocol.deleteVolume(volumeName);
    }
  }
}
