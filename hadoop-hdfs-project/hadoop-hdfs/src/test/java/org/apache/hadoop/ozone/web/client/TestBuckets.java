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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestBuckets {
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static MiniOzoneCluster cluster = null;
  private static OzoneRestClient client = null;

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
  public static void init() throws IOException,
      URISyntaxException, OzoneException {
    OzoneConfiguration conf = new OzoneConfiguration();

    String path = GenericTestUtils
        .getTempPath(TestBuckets.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);

    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);
    cluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_LOCAL).build();
    DataNode dataNode = cluster.getDataNodes().get(0);
    final int port = dataNode.getInfoPort();
    client = new OzoneRestClient(String.format("http://localhost:%d", port));
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
  public void testCreateBucket() throws OzoneException, IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth("hdfs");
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    String[] acls = {"user:frodo:rw", "user:samwise:rw"};

    // create 10 buckets under same volume
    for (int x = 0; x < 10; x++) {
      String bucketName = OzoneUtils.getRequestID().toLowerCase();
      OzoneBucket bucket =
          vol.createBucket(bucketName, acls, StorageType.DEFAULT);
      assertEquals(bucket.getBucketName(), bucketName);
    }
    client.close();

    assertEquals(vol.getVolumeName(), volumeName);
    assertEquals(vol.getCreatedby(), "hdfs");
    assertEquals(vol.getOwnerName(), "bilbo");
    assertEquals(vol.getQuota().getUnit(), OzoneQuota.Units.TB);
    assertEquals(vol.getQuota().getSize(), 100);
  }

  @Test
  public void testAddBucketAcls() throws OzoneException, IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth("hdfs");
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    String[] acls = {"user:frodo:rw", "user:samwise:rw"};
    String bucketName = OzoneUtils.getRequestID().toLowerCase();
    vol.createBucket(bucketName);
    vol.addAcls(bucketName, acls);
    OzoneBucket updatedBucket = vol.getBucket(bucketName);
    assertEquals(updatedBucket.getAcls().size(), 2);
    client.close();
  }

  @Test
  public void testRemoveBucketAcls() throws OzoneException, IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth("hdfs");
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    String[] acls = {"user:frodo:rw", "user:samwise:rw"};
    String bucketName = OzoneUtils.getRequestID().toLowerCase();
    OzoneBucket bucket = vol.createBucket(bucketName, acls);
    assertEquals(bucket.getAcls().size(), 2);
    vol.removeAcls(bucketName, acls);
    OzoneBucket updatedBucket = vol.getBucket(bucketName);

    // We removed all acls
    assertEquals(updatedBucket.getAcls().size(), 0);
    client.close();
  }

  @Test
  public void testDeleteBucket() throws OzoneException, IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth("hdfs");
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    String[] acls = {"user:frodo:rw", "user:samwise:rw"};
    String bucketName = OzoneUtils.getRequestID().toLowerCase();
    vol.createBucket(bucketName, acls);
    vol.deleteBucket(bucketName);
    try {
      OzoneBucket updatedBucket = vol.getBucket(bucketName);
      fail("Fetching deleted bucket, Should not reach here.");
    } catch (Exception ex) {
      // must throw
      assertNotNull(ex);
    }
    client.close();
  }

  @Test
  public void testListBucket() throws OzoneException, IOException {
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    client.setUserAuth("hdfs");
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    String[] acls = {"user:frodo:rw", "user:samwise:rw"};
    for (int x = 0; x < 10; x++) {
      String bucketName = OzoneUtils.getRequestID().toLowerCase();
      vol.createBucket(bucketName, acls);
    }
    List<OzoneBucket> bucketList = vol.listBuckets();
    assertEquals(bucketList.size(), 10);
    client.close();
  }
}
