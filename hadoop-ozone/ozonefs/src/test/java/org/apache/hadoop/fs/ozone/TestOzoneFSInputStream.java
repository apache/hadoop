/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Test OzoneFSInputStream by reading through multiple interfaces.
 */
public class TestOzoneFSInputStream {
  private static MiniOzoneCluster cluster = null;
  private static FileSystem fs;
  private static StorageHandler storageHandler;
  private static Path filePath = null;
  private static byte[] data = null;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 10,
        StorageUnit.MB);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(10)
        .build();
    cluster.waitForClusterToBeReady();
    storageHandler =
        new ObjectStoreHandler(conf).getStorageHandler();

    // create a volume and a bucket to be used by OzoneFileSystem
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    UserArgs userArgs = new UserArgs(null, OzoneUtils.getRequestID(),
        null, null, null, null);
    VolumeArgs volumeArgs = new VolumeArgs(volumeName, userArgs);
    volumeArgs.setUserName(userName);
    volumeArgs.setAdminName(adminName);
    storageHandler.createVolume(volumeArgs);
    BucketArgs bucketArgs = new BucketArgs(volumeName, bucketName, userArgs);
    storageHandler.createBucket(bucketArgs);

    // Fetch the host and port for File System init
    DatanodeDetails datanodeDetails = cluster.getHddsDatanodes().get(0)
        .getDatanodeDetails();

    // Set the fs.defaultFS and start the filesystem
    String uri = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, uri);
    fs =  FileSystem.get(conf);
    int fileLen = 100 * 1024 * 1024;
    data = DFSUtil.string2Bytes(RandomStringUtils.randomAlphanumeric(fileLen));
    filePath = new Path("/" + RandomStringUtils.randomAlphanumeric(5));
    try (FSDataOutputStream stream = fs.create(filePath)) {
      stream.write(data);
    }
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() throws IOException {
    fs.close();
    storageHandler.close();
    cluster.shutdown();
  }

  @Test
  public void testO3FSSingleByteRead() throws IOException {
    FSDataInputStream inputStream = fs.open(filePath);
    byte[] value = new byte[data.length];
    int i = 0;
    while(true) {
      int val = inputStream.read();
      if (val == -1) {
        break;
      }
      value[i] = (byte)val;
      Assert.assertEquals("value mismatch at:" + i, value[i], data[i]);
      i++;
    }
    Assert.assertEquals(i, data.length);
    Assert.assertTrue(Arrays.equals(value, data));
    inputStream.close();
  }

  @Test
  public void testO3FSMultiByteRead() throws IOException {
    FSDataInputStream inputStream = fs.open(filePath);
    byte[] value = new byte[data.length];
    byte[] tmp = new byte[1* 1024 *1024];
    int i = 0;
    while(true) {
      int val = inputStream.read(tmp);
      if (val == -1) {
        break;
      }
      System.arraycopy(tmp, 0, value, i * tmp.length, tmp.length);
      i++;
    }
    Assert.assertEquals(i * tmp.length, data.length);
    Assert.assertTrue(Arrays.equals(value, data));
    inputStream.close();
  }
}
