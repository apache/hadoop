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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.util.Time;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;

/**
 * Test OzoneFileSystem Interfaces.
 *
 * This test will test the various interfaces i.e.
 * create, read, write, getFileStatus
 */
public class TestOzoneFileInterfaces {
  private static MiniOzoneCluster cluster = null;
  private static FileSystem fs;
  private static StorageHandler storageHandler;

  @BeforeClass
  public static void init() throws IOException, OzoneException {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
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
    DataNode dataNode = cluster.getDataNodes().get(0);
    int port = dataNode.getInfoPort();
    String host = dataNode.getDatanodeHostname();

    // Set the fs.defaultFS and start the filesystem
    String uri = String.format("%s://%s:%d/%s/%s",
        Constants.OZONE_URI_SCHEME, host, port, volumeName, bucketName);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, uri);
    fs =  FileSystem.get(conf);
  }

  @AfterClass
  public static void teardown() throws IOException {
    fs.close();
    storageHandler.close();
    cluster.shutdown();
  }

  @Test
  public void testFileSystemInit() throws IOException {
    Assert.assertTrue(fs instanceof OzoneFileSystem);
    Assert.assertEquals(fs.getUri().getScheme(), Constants.OZONE_URI_SCHEME);
  }

  @Test
  public void testOzFsReadWrite() throws IOException {
    long currentTime = Time.now();
    int stringLen = 20;
    String data = RandomStringUtils.randomAlphanumeric(stringLen);
    String filePath = RandomStringUtils.randomAlphanumeric(5);
    Path path = new Path("/" + filePath);
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.writeBytes(data);
    }

    FileStatus status = fs.getFileStatus(path);
    Assert.assertTrue(status.getModificationTime() < currentTime);

    try (FSDataInputStream inputStream = fs.open(path)) {
      byte[] buffer = new byte[stringLen];
      inputStream.readFully(0, buffer);
      String out = new String(buffer, 0, buffer.length);
      Assert.assertEquals(out, data);
    }
  }

  @Test
  public void testDirectory() throws IOException {
    String dirPath = RandomStringUtils.randomAlphanumeric(5);
    Path path = new Path("/" + dirPath);
    Assert.assertTrue(fs.mkdirs(path));

    FileStatus status = fs.getFileStatus(path);
    Assert.assertTrue(status.isDirectory());
    Assert.assertEquals(status.getLen(), 0);

    FileStatus[] statusList = fs.listStatus(new Path("/"));
    Assert.assertEquals(statusList.length, 1);
    Assert.assertEquals(statusList[0], status);
  }
}
