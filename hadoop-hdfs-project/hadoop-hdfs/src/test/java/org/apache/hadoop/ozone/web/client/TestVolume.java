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
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test Ozone Volumes Lifecycle.
 */
public class TestVolume {
  private static MiniOzoneCluster cluster = null;
  private static OzoneRestClient ozoneRestClient = null;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true and
   * OZONE_HANDLER_TYPE_KEY = "local" , which uses a local directory to
   * emulate Ozone backend.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    String path = GenericTestUtils
        .getTempPath(TestVolume.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
    FileUtils.deleteDirectory(new File(path));

    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);
    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.DEBUG);

    cluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    DataNode dataNode = cluster.getDataNodes().get(0);
    final int port = dataNode.getInfoPort();

    ozoneRestClient = new OzoneRestClient(
        String.format("http://localhost:%d", port));
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
    runTestCreateVolume(ozoneRestClient);
  }

  static void runTestCreateVolume(OzoneRestClient client)
      throws OzoneException, IOException, ParseException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);

    long currentTime = Time.now();
    OzoneRestClient mockClient = Mockito.spy(client);
    List<CloseableHttpClient> mockedClients = mockHttpClients(mockClient);
    OzoneVolume vol = mockClient.createVolume(volumeName, "bilbo", "100TB");
    // Verify http clients are properly closed.
    verifyHttpConnectionClosed(mockedClients);

    assertEquals(vol.getVolumeName(), volumeName);
    assertEquals(vol.getCreatedby(), "hdfs");
    assertEquals(vol.getOwnerName(), "bilbo");
    assertEquals(vol.getQuota().getUnit(), OzoneQuota.Units.TB);
    assertEquals(vol.getQuota().getSize(), 100);

    // verify the key creation time
    assertTrue((OzoneUtils.formatDate(vol.getCreatedOn())
        / 1000) >= (currentTime / 1000));

    // Test create a volume with invalid volume name,
    // not use Rule here because the test method is static.
    try {
      String invalidVolumeName = "#" + OzoneUtils.getRequestID().toLowerCase();
      client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
      mockClient.createVolume(invalidVolumeName, "bilbo", "100TB");
      fail("Except the volume creation be failed because the"
          + " volume name starts with an invalid char #");
    } catch (Exception e) {
      assertTrue(e instanceof OzoneRestClientException);
      assertTrue(e.getMessage().contains("Bucket or Volume name"
          + " has an unsupported character : #"));
    }
  }

  @Test
  public void testCreateDuplicateVolume() throws OzoneException {
    runTestCreateDuplicateVolume(ozoneRestClient);
  }

  static void runTestCreateDuplicateVolume(OzoneRestClient client)
      throws OzoneException {
    try {
      client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
      client.createVolume("testvol", "bilbo", "100TB");
      client.createVolume("testvol", "bilbo", "100TB");
      assertFalse(true);
    } catch (OzoneException ex) {
      // Ozone will throw saying volume already exists
      GenericTestUtils.assertExceptionContains(
          Status.VOLUME_ALREADY_EXISTS.toString(), ex);
    }
  }

  @Test
  public void testDeleteVolume() throws OzoneException {
    runTestDeleteVolume(ozoneRestClient);
  }

  static void runTestDeleteVolume(OzoneRestClient client)
      throws OzoneException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    client.deleteVolume(vol.getVolumeName());
  }

  @Test
  public void testChangeOwnerOnVolume() throws Exception {
    runTestChangeOwnerOnVolume(ozoneRestClient);
  }

  static void runTestChangeOwnerOnVolume(OzoneRestClient client)
      throws OzoneException, ParseException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    client.setVolumeOwner(volumeName, "frodo");
    OzoneVolume newVol = client.getVolume(volumeName);
    assertEquals(newVol.getOwnerName(), "frodo");
    // verify if the creation time is missing after setting owner operation
    assertTrue(OzoneUtils.formatDate(newVol.getCreatedOn()) > 0);
  }

  @Test
  public void testChangeQuotaOnVolume() throws Exception {
    runTestChangeQuotaOnVolume(ozoneRestClient);
  }

  static void runTestChangeQuotaOnVolume(OzoneRestClient client)
      throws OzoneException, IOException, ParseException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    client.setVolumeQuota(volumeName, "1000MB");
    OzoneVolume newVol = client.getVolume(volumeName);
    assertEquals(newVol.getQuota().getSize(), 1000);
    assertEquals(newVol.getQuota().getUnit(), OzoneQuota.Units.MB);
    // verify if the creation time is missing after setting quota operation
    assertTrue(OzoneUtils.formatDate(newVol.getCreatedOn()) > 0);
  }

  @Test
  public void testListVolume() throws OzoneException, IOException {
    runTestListVolume(ozoneRestClient);
  }

  static void runTestListVolume(OzoneRestClient client)
      throws OzoneException, IOException {
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    for (int x = 0; x < 10; x++) {
      String volumeName = OzoneUtils.getRequestID().toLowerCase();
      OzoneVolume vol = client.createVolume(volumeName, "frodo", "100TB");
      assertNotNull(vol);
    }

    List<OzoneVolume> ovols = client.listVolumes("frodo");
    assertTrue(ovols.size() >= 10);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore("Takes 3m to run, disable for now.")
  @Test
  public void testListVolumePagination() throws OzoneException, IOException {
    runTestListVolumePagination(ozoneRestClient);
  }

  static void runTestListVolumePagination(OzoneRestClient client)
      throws OzoneException, IOException {
    final int volCount = 2000;
    final int step = 100;
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    for (int x = 0; x < volCount; x++) {
      String volumeName = OzoneUtils.getRequestID().toLowerCase();
      OzoneVolume vol = client.createVolume(volumeName, "frodo", "100TB");
      assertNotNull(vol);
    }
    OzoneVolume prevKey = null;
    int count = 0;
    int pagecount = 0;
    while (count < volCount) {
      List<OzoneVolume> ovols = client.listVolumes("frodo", null, step,
          prevKey);
      count += ovols.size();
      prevKey = ovols.get(ovols.size() - 1);
      pagecount++;
    }
    assertEquals(volCount / step, pagecount);
  }

  // TODO: remove @Ignore below once the problem has been resolved.
  @Ignore
  @Test
  public void testListAllVolumes() throws OzoneException, IOException {
    runTestListAllVolumes(ozoneRestClient);
  }

  static void runTestListAllVolumes(OzoneRestClient client)
      throws OzoneException, IOException {
    final int volCount = 200;
    final int step = 10;
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    for (int x = 0; x < volCount; x++) {
      String userName =
          "frodo" + RandomStringUtils.randomAlphabetic(5).toLowerCase();
      String volumeName =
          "vol" + RandomStringUtils.randomAlphabetic(5).toLowerCase();
      OzoneVolume vol = client.createVolume(volumeName, userName, "100TB");
      assertNotNull(vol);
    }
    OzoneVolume prevKey = null;
    int count = 0;
    int pagecount = 0;
    while (count < volCount) {
      List<OzoneVolume> ovols = client.listAllVolumes(null, step, prevKey);
      count += ovols.size();
      if (ovols.size() > 0) {
        prevKey = ovols.get(ovols.size() - 1);
      }
      pagecount++;
    }
    // becasue we are querying an existing ozone store, there will
    // be volumes created by other tests too. So we should get more page counts.
    assertEquals(volCount / step, pagecount);
  }

  @Test
  public void testListVolumes() throws Exception {
    runTestListVolumes(ozoneRestClient);
  }

  static void runTestListVolumes(OzoneRestClient client)
      throws OzoneException, IOException, ParseException {
    final int volCount = 20;
    final String user1 = "test-user-a";
    final String user2 = "test-user-b";

    long currentTime = Time.now();
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
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
      OzoneVolume vol = client.createVolume(volumeName, userName, "100TB");
      assertNotNull(vol);
    }

    // list all the volumes belong to user1
    List<OzoneVolume> volumeList = client.listVolumes(user1,
        null, 100, StringUtils.EMPTY);
    assertEquals(10, volumeList.size());
    // verify the owner name and creation time of volume
    for (OzoneVolume vol : volumeList) {
      assertTrue(vol.getOwnerName().equals(user1));
      assertTrue((OzoneUtils.formatDate(vol.getCreatedOn())
          / 1000) >= (currentTime / 1000));
    }

    // test max key parameter of listing volumes
    volumeList = client.listVolumes(user1, null, 2, StringUtils.EMPTY);
    assertEquals(2, volumeList.size());

    // test prefix parameter of listing volumes
    volumeList = client.listVolumes(user1, "test-vol10", 100,
        StringUtils.EMPTY);
    assertTrue(volumeList.size() == 1
        && volumeList.get(0).getVolumeName().equals("test-vol10"));

    volumeList = client.listVolumes(user1, "test-vol1",
        100, StringUtils.EMPTY);
    assertEquals(5, volumeList.size());

    // test start key parameter of listing volumes
    volumeList = client.listVolumes(user2, null, 100, "test-vol15");
    assertEquals(2, volumeList.size());
  }

  /**
   * Returns a list of mocked {@link CloseableHttpClient} used for testing.
   * The mocked client replaces the actual calls in
   * {@link OzoneRestClient#newHttpClient()}, it is used to verify
   * if the invocation of this client is expected. <b>Note</b>, the output
   * of this method is always used as the input of
   * {@link TestVolume#verifyHttpConnectionClosed(List)}.
   *
   * @param mockedClient mocked ozone client.
   * @return a list of mocked {@link CloseableHttpClient}.
   * @throws IOException
   */
  private static List<CloseableHttpClient> mockHttpClients(
      OzoneRestClient mockedClient)
      throws IOException {
    List<CloseableHttpClient> spyHttpClients = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      CloseableHttpClient spyHttpClient = Mockito
          .spy(OzoneClientUtils.newHttpClient());
      spyHttpClients.add(spyHttpClient);
    }

    List<CloseableHttpClient> nextReturns =
        new ArrayList<>(spyHttpClients.subList(1, spyHttpClients.size()));
    Mockito.when(mockedClient.newHttpClient()).thenReturn(
        spyHttpClients.get(0),
        nextReturns.toArray(new CloseableHttpClient[nextReturns.size()]));
    return spyHttpClients;
  }

  /**
   * This method is used together with
   * {@link TestVolume#mockHttpClients(OzoneRestClient)} to verify
   * if the http client is properly closed. It verifies that as long as
   * a client calls {@link CloseableHttpClient#execute(HttpUriRequest)} to
   * send request, then it must calls {@link CloseableHttpClient#close()}
   * close the http connection.
   *
   * @param mockedHttpClients
   */
  private static void verifyHttpConnectionClosed(
      List<CloseableHttpClient> mockedHttpClients) {
    final AtomicInteger totalCalled = new AtomicInteger();
    assertTrue(mockedHttpClients.stream().allMatch(closeableHttpClient -> {
      boolean clientUsed = false;
      try {
        verify(closeableHttpClient, times(1)).execute(Mockito.any());
        totalCalled.incrementAndGet();
        clientUsed = true;
      } catch (Throwable e) {
        // There might be some redundant instances in mockedHttpClients,
        // it is allowed that a client is not used.
        return true;
      }

      if (clientUsed) {
        try {
          // If a client is used, ensure the close function is called.
          verify(closeableHttpClient, times(1)).close();
          return true;
        } catch (IOException e) {
          return false;
        }
      } else {
        return true;
      }
    }));
    System.out.println("Successful connections " + totalCalled.get());
    assertTrue("The mocked http client should be called at least once.",
        totalCalled.get() > 0);
  }
}
