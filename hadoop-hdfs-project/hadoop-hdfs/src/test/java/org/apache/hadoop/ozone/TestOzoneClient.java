/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * This class is to test all the public facing APIs of Ozone Client.
 */
public class TestOzoneClient {

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true and
   * OZONE_HANDLER_TYPE_KEY = "distributed"
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HANDLER_TYPE_KEY,
        OzoneConsts.OZONE_HANDLER_DISTRIBUTED);
    cluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    ozClient = new OzoneClient(conf);
  }

  @Test
  public void testCreateVolume()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName);
    //Assert to be done once infoVolume is implemented in OzoneClient.
    //For now the test will fail if there are any Exception
    // during volume creation
  }

  @Test
  public void testCreateVolumeWithOwner()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName, "test");
    //Assert has to be done after infoVolume implementation.
  }

  @Test
  public void testCreateVolumeWithQuota()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName, "test", "10GB");
  }

  @Test
  public void testVolumeAlreadyExist()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName);
    try {
      ozClient.createVolume(volumeName);
    } catch (IOException ex) {
      Assert.assertEquals(
          "Volume creation failed, error:VOLUME_ALREADY_EXISTS",
          ex.getMessage());
    }
  }

  @Test
  public void testCreateBucket()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName);
    ozClient.createBucket(volumeName, bucketName);
    //Assert has to be done.
  }

  @Test
  public void testCreateBucketWithVersioning()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName);
    ozClient.createBucket(volumeName, bucketName,
        OzoneConsts.Versioning.ENABLED);
    //Assert has to be done.
  }

  @Test
  public void testCreateBucketWithStorageType()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName);
    ozClient.createBucket(volumeName, bucketName, StorageType.SSD);
    //Assert has to be done.
  }

  @Test
  public void testCreateBucketWithAcls()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER, "test",
        OzoneAcl.OzoneACLRights.READ_WRITE);
    ozClient.createVolume(volumeName);
    ozClient.createBucket(volumeName, bucketName, userAcl);
    //Assert has to be done.
  }

  @Test
  public void testCreateBucketWithAllArgument()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER, "test",
        OzoneAcl.OzoneACLRights.READ_WRITE);
    ozClient.createVolume(volumeName);
    ozClient.createBucket(volumeName, bucketName,
        OzoneConsts.Versioning.ENABLED,
        StorageType.SSD, userAcl);
    //Assert has to be done.
  }

  @Test
  public void testCreateBucketInInvalidVolume()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    try {
      ozClient.createBucket(volumeName, bucketName);
    } catch (IOException ex) {
      Assert.assertEquals(
          "Bucket creation failed, error: VOLUME_NOT_FOUND",
          ex.getMessage());
    }
  }

  @Test
  public void testPutKey()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    ozClient.createVolume(volumeName);
    ozClient.createBucket(volumeName, bucketName);
    ozClient.putKey(volumeName, bucketName, keyName, value.getBytes());
    //Assert has to be done.
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

}
