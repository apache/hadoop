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
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneClientUtils;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestVolume {
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client = null;

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
        .setHandlerType(OzoneConsts.OZONE_HANDLER_LOCAL).build();
    DataNode dataNode = cluster.getDataNodes().get(0);
    final int port = dataNode.getInfoPort();

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
  public void testCreateVolume() throws OzoneException, IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);

    OzoneClient mockClient = Mockito.spy(client);
    List<CloseableHttpClient> mockedClients = mockHttpClients(mockClient);
    OzoneVolume vol = mockClient.createVolume(volumeName, "bilbo", "100TB");
    // Verify http clients are properly closed.
    verifyHttpConnectionClosed(mockedClients);

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

  //@Test
  // Takes 3m to run, disable for now.
  public void testListVolumePagination() throws OzoneException, IOException {
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
    Assert.assertEquals(volCount / step, pagecount);
  }

  //@Test
  public void testListAllVolumes() throws OzoneException, IOException {
    final int volCount = 200;
    final int step = 10;
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
    for (int x = 0; x < volCount; x++) {
      String userName = "frodo" +
          RandomStringUtils.randomAlphabetic(5).toLowerCase();
      String volumeName = "vol" +
          RandomStringUtils.randomAlphabetic(5).toLowerCase();
      OzoneVolume vol = client.createVolume(volumeName, userName, "100TB");
      assertNotNull(vol);
    }
    OzoneVolume prevKey = null;
    int count = 0;
    int pagecount = 0;
    while (count < volCount) {
      List<OzoneVolume> ovols = client.listAllVolumes(null, step,
          prevKey);
      count += ovols.size();
      if(ovols.size() > 0) {
        prevKey = ovols.get(ovols.size() - 1);
      }
      pagecount++;
    }
    // becasue we are querying an existing ozone store, there will
    // be volumes created by other tests too. So we should get more page counts.
    Assert.assertEquals(volCount / step , pagecount);
  }

  /**
   * Returns a list of mocked {@link CloseableHttpClient} used for testing.
   * The mocked client replaces the actual calls in
   * {@link OzoneClient#newHttpClient()}, it is used to verify
   * if the invocation of this client is expected. <b>Note</b>, the output
   * of this method is always used as the input of
   * {@link TestVolume#verifyHttpConnectionClosed(List)}.
   *
   * @param ozoneClient mocked ozone client.
   * @return a list of mocked {@link CloseableHttpClient}.
   * @throws IOException
   */
  private List<CloseableHttpClient> mockHttpClients(OzoneClient ozoneClient)
      throws IOException {
    List<CloseableHttpClient> spyHttpClients = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      CloseableHttpClient spyHttpClient = Mockito
          .spy(OzoneClientUtils.newHttpClient());
      spyHttpClients.add(spyHttpClient);
    }

    List<CloseableHttpClient> nextReturns =
        new ArrayList<>(spyHttpClients.subList(1, spyHttpClients.size()));
    Mockito.when(ozoneClient.newHttpClient()).thenReturn(
        spyHttpClients.get(0),
        nextReturns.toArray(new CloseableHttpClient[nextReturns.size()]));
    return spyHttpClients;
  }

  /**
   * This method is used together with
   * {@link TestVolume#mockHttpClients(OzoneClient)} to verify
   * if the http client is properly closed. It verifies that as long as
   * a client calls {@link CloseableHttpClient#execute(HttpUriRequest)} to
   * send request, then it must calls {@link CloseableHttpClient#close()}
   * close the http connection.
   *
   * @param mockedHttpClients
   */
  private void verifyHttpConnectionClosed(
      List<CloseableHttpClient> mockedHttpClients) {
    final AtomicInteger totalCalled = new AtomicInteger();
    Assert.assertTrue(mockedHttpClients.stream().allMatch(
        closeableHttpClient -> {
          boolean clientUsed = false;
          try {
            verify(closeableHttpClient, times(1))
                .execute(Mockito.any());
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
              verify(closeableHttpClient,
                  times(1)).close();
              return true;
            } catch (IOException e) {
              return false;
            }
          } else {
            return true;
          }
        }));
    System.out.println("Successful connections "
        + totalCalled.get());
    Assert.assertTrue(
        "The mocked http client should be called at least once.",
        totalCalled.get() > 0);
  }
}
