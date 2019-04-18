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

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

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

  private static MiniOzoneCluster cluster = null;

  private static FileSystem fs;

  private static OzoneFileSystem o3fs;

  private static String volumeName;

  private static String bucketName;

  private static StorageHandler storageHandler;

  private OzoneFSStorageStatistics statistics;

  public TestOzoneFileInterfaces(boolean setDefaultFs,
      boolean useAbsolutePath) {
    this.setDefaultFs = setDefaultFs;
    this.useAbsolutePath = useAbsolutePath;
    GlobalStorageStatistics.INSTANCE.reset();
  }

  @Before
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    storageHandler =
        new ObjectStoreHandler(conf).getStorageHandler();

    // create a volume and a bucket to be used by OzoneFileSystem
    userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    UserArgs userArgs = new UserArgs(null, OzoneUtils.getRequestID(),
        null, null, null, null);
    VolumeArgs volumeArgs = new VolumeArgs(volumeName, userArgs);
    volumeArgs.setUserName(userName);
    volumeArgs.setAdminName(adminName);
    storageHandler.createVolume(volumeArgs);
    BucketArgs bucketArgs = new BucketArgs(volumeName, bucketName, userArgs);
    storageHandler.createBucket(bucketArgs);

    rootPath = String
        .format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME, bucketName,
            volumeName);
    if (setDefaultFs) {
      // Set the fs.defaultFS and start the filesystem
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
      fs = FileSystem.get(conf);
    } else {
      fs = FileSystem.get(new URI(rootPath + "/test.txt"), conf);
    }
    o3fs = (OzoneFileSystem) fs;
    statistics = (OzoneFSStorageStatistics) o3fs.getOzoneFSOpsCountStatistics();
  }

  @After
  public void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
    IOUtils.closeQuietly(storageHandler);
  }

  @Test
  public void testFileSystemInit() throws IOException {
    if (setDefaultFs) {
      assertTrue(
          "The initialized file system is not OzoneFileSystem but " +
              fs.getClass(),
          fs instanceof OzoneFileSystem);
      assertEquals(OzoneConsts.OZONE_URI_SCHEME, fs.getUri().getScheme());
      assertEquals(OzoneConsts.OZONE_URI_SCHEME, statistics.getScheme());
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

    assertEquals(statistics.getLong(
        StorageStatistics.CommonStatisticNames.OP_CREATE).longValue(), 1);
    assertEquals(statistics.getLong("objects_created").longValue(), 1);

    FileStatus status = fs.getFileStatus(path);
    assertEquals(statistics.getLong(
        StorageStatistics.CommonStatisticNames.OP_GET_FILE_STATUS).longValue(),
        1);
    assertEquals(statistics.getLong("objects_query").longValue(), 1);
    // The timestamp of the newly created file should always be greater than
    // the time when the test was started
    assertTrue("Modification time has not been recorded: " + status,
        status.getModificationTime() > currentTime);

    assertFalse(status.isDirectory());
    assertEquals(FsPermission.getFileDefault(), status.getPermission());
    verifyOwnerGroup(status);

    try (FSDataInputStream inputStream = fs.open(path)) {
      byte[] buffer = new byte[stringLen];
      // This read will not change the offset inside the file
      int readBytes = inputStream.read(0, buffer, 0, buffer.length);
      String out = new String(buffer, 0, buffer.length, UTF_8);
      assertEquals(data, out);
      assertEquals(readBytes, buffer.length);
      assertEquals(0, inputStream.getPos());

      // The following read will change the internal offset
      readBytes = inputStream.read(buffer, 0, buffer.length);
      assertEquals(data, out);
      assertEquals(readBytes, buffer.length);
      assertEquals(buffer.length, inputStream.getPos());
    }
    assertEquals(statistics.getLong(
        StorageStatistics.CommonStatisticNames.OP_OPEN).longValue(), 1);
    assertEquals(statistics.getLong("objects_read").longValue(), 1);
  }

  private void verifyOwnerGroup(FileStatus fileStatus) {
    String owner = getCurrentUser();
    assertEquals(owner, fileStatus.getOwner());
    assertEquals(owner, fileStatus.getGroup());
  }


  @Test
  public void testDirectory() throws IOException {
    String dirPath = RandomStringUtils.randomAlphanumeric(5);
    Path path = createPath("/" + dirPath);
    assertTrue("Makedirs returned with false for the path " + path,
        fs.mkdirs(path));

    FileStatus status = fs.getFileStatus(path);
    assertTrue("The created path is not directory.", status.isDirectory());

    assertTrue(status.isDirectory());
    assertEquals(FsPermission.getDirDefault(), status.getPermission());
    verifyOwnerGroup(status);

    assertEquals(0, status.getLen());

    FileStatus[] statusList = fs.listStatus(createPath("/"));
    assertEquals(1, statusList.length);
    assertEquals(status, statusList[0]);

    FileStatus statusRoot = fs.getFileStatus(createPath("/"));
    assertTrue("Root dir (/) is not a directory.", status.isDirectory());
    assertEquals(0, status.getLen());
  }

  @Test
  public void testOzoneManagerFileSystemInterface() throws IOException {
    String dirPath = RandomStringUtils.randomAlphanumeric(5);

    Path path = createPath("/" + dirPath);
    assertTrue("Makedirs returned with false for the path " + path,
        fs.mkdirs(path));

    long numFileStatus =
        cluster.getOzoneManager().getMetrics().getNumGetFileStatus();
    FileStatus status = fs.getFileStatus(path);

    Assert.assertEquals(numFileStatus + 1,
        cluster.getOzoneManager().getMetrics().getNumGetFileStatus());
    assertTrue(status.isDirectory());
    assertEquals(FsPermission.getDirDefault(), status.getPermission());
    verifyOwnerGroup(status);

    long currentTime = System.currentTimeMillis();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(o3fs.pathToKey(path))
        .build();
    OzoneFileStatus omStatus =
        cluster.getOzoneManager().getFileStatus(keyArgs);
    //Another get file status here, incremented the counter.
    Assert.assertEquals(numFileStatus + 2,
        cluster.getOzoneManager().getMetrics().getNumGetFileStatus());

    assertTrue("The created path is not directory.", omStatus.isDirectory());

    // For directories, the time returned is the current time.
    assertEquals(0, omStatus.getLen());
    assertTrue(omStatus.getModificationTime() >= currentTime);
    assertEquals(omStatus.getPath().getName(), o3fs.pathToKey(path));
  }

  @Test
  public void testPathToKey() throws Exception {

    assertEquals("a/b/1", o3fs.pathToKey(new Path("/a/b/1")));

    assertEquals("user/" + getCurrentUser() + "/key1/key2",
        o3fs.pathToKey(new Path("key1/key2")));

    assertEquals("key1/key2",
        o3fs.pathToKey(new Path("o3fs://test1/key1/key2")));
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
