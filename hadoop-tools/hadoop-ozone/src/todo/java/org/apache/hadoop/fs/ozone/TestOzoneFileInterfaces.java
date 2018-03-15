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

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;

import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test OzoneFileSystem Interfaces.
 *
 * This test will test the various interfaces i.e.
 * create, read, write, getFileStatus
 */
@RunWith(Parameterized.class)
public class TestOzoneFileInterfaces {

  private String rootPath;
  private String userName;

  /**
   * Parameter class to set absolute url/defaultFS handling.
   * <p>
   * Hadoop file systems could be used in multiple ways: Using the defaultfs
   * and file path without the schema, or use absolute url-s even with
   * different defaultFS. This parameter matrix would test both the use cases.
   */
  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{false, true}, {true, false}});
  }

  private boolean setDefaultFs;

  private boolean useAbsolutePath;

  private static MiniOzoneClassicCluster cluster = null;

  private static FileSystem fs;

  private static StorageHandler storageHandler;

  public TestOzoneFileInterfaces(boolean setDefaultFs,
      boolean useAbsolutePath) {
    this.setDefaultFs = setDefaultFs;
    this.useAbsolutePath = useAbsolutePath;
  }

  @Before
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = new MiniOzoneClassicCluster.Builder(conf)
        .numDataNodes(10)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED)
        .build();
    storageHandler =
        new ObjectStoreHandler(conf).getStorageHandler();

    // create a volume and a bucket to be used by OzoneFileSystem
    userName = "user" + RandomStringUtils.randomNumeric(5);
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

    rootPath = String
        .format("%s://%s.%s/", Constants.OZONE_URI_SCHEME, bucketName,
            volumeName);
    if (setDefaultFs) {
      // Set the fs.defaultFS and start the filesystem
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
      fs = FileSystem.get(conf);
    } else {
      fs = FileSystem.get(new URI(rootPath + "/test.txt"), conf);
    }
  }

  @After
  public void teardown() throws IOException {
    IOUtils.closeQuietly(fs);
    IOUtils.closeQuietly(storageHandler);
    IOUtils.closeQuietly(cluster);
  }

  @Test
  public void testFileSystemInit() throws IOException {
    if (setDefaultFs) {
      assertTrue(
          "The initialized file system is not OzoneFileSysetem but " +
              fs.getClass(),
          fs instanceof OzoneFileSystem);
      assertEquals(Constants.OZONE_URI_SCHEME, fs.getUri().getScheme());
    }
  }

  @Test
  public void testOzFsReadWrite() throws IOException {
    long currentTime = Time.now();
    int stringLen = 20;
    String data = RandomStringUtils.randomAlphanumeric(stringLen);
    String filePath = RandomStringUtils.randomAlphanumeric(5);
    Path path = createPath("/" + filePath);
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.writeBytes(data);
    }

    FileStatus status = fs.getFileStatus(path);
    // The timestamp of the newly created file should always be greater than
    // the time when the test was started
    assertTrue("Modification time has not been recorded: " + status,
        status.getModificationTime() > currentTime);

    try (FSDataInputStream inputStream = fs.open(path)) {
      byte[] buffer = new byte[stringLen];
      inputStream.readFully(0, buffer);
      String out = new String(buffer, 0, buffer.length);
      assertEquals(data, out);
    }
  }


  @Test
  public void testDirectory() throws IOException {
    String dirPath = RandomStringUtils.randomAlphanumeric(5);
    Path path = createPath("/" + dirPath);
    assertTrue("Makedirs returned with false for the path " + path,
        fs.mkdirs(path));

    FileStatus status = fs.getFileStatus(path);
    assertTrue("The created path is not directory.", status.isDirectory());

    assertEquals(0, status.getLen());

    FileStatus[] statusList = fs.listStatus(createPath("/"));
    assertEquals(1, statusList.length);
    assertEquals(status, statusList[0]);

    FileStatus statusRoot = fs.getFileStatus(createPath("/"));
    assertTrue("Root dir (/) is not a directory.", status.isDirectory());
    assertEquals(0, status.getLen());


  }

  @Test
  public void testPathToKey() throws Exception {
    OzoneFileSystem ozoneFs = (OzoneFileSystem) TestOzoneFileInterfaces.fs;

    assertEquals("a/b/1", ozoneFs.pathToKey(new Path("/a/b/1")));

    assertEquals("user/" + getCurrentUser() + "/key1/key2",
        ozoneFs.pathToKey(new Path("key1/key2")));

    assertEquals("key1/key2",
        ozoneFs.pathToKey(new Path("o3://test1/key1/key2")));
  }

  private String getCurrentUser() {
    try {
      return UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      return OZONE_DEFAULT_USER;
    }
  }

  private Path createPath(String relativePath) {
    if (useAbsolutePath) {
      return new Path(
          rootPath + (relativePath.startsWith("/") ? "" : "/") + relativePath);
    } else {
      return new Path(relativePath);
    }
  }
}
