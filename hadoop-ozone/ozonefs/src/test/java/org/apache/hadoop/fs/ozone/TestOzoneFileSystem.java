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

package org.apache.hadoop.fs.ozone;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Ozone file system tests that are not covered by contract tests.
 */
public class TestOzoneFileSystem {

  @Rule
  public Timeout globalTimeout = new Timeout(300_000);

  private static MiniOzoneCluster cluster = null;

  private static FileSystem fs;
  private static OzoneFileSystem o3fs;

  private static StorageHandler storageHandler;
  private static UserArgs userArgs;
  private String volumeName;
  private String bucketName;
  private String userName;
  private String rootPath;

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
    userArgs = new UserArgs(null, OzoneUtils.getRequestID(),
        null, null, null, null);
    VolumeArgs volumeArgs = new VolumeArgs(volumeName, userArgs);
    volumeArgs.setUserName(userName);
    volumeArgs.setAdminName(adminName);
    storageHandler.createVolume(volumeArgs);
    BucketArgs bucketArgs = new BucketArgs(volumeName, bucketName, userArgs);
    storageHandler.createBucket(bucketArgs);

    rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    fs = FileSystem.get(conf);
    o3fs = (OzoneFileSystem) fs;
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
  public void testOzoneFsServiceLoader() throws IOException {
    assertEquals(
        FileSystem.getFileSystemClass(OzoneConsts.OZONE_URI_SCHEME, null),
        OzoneFileSystem.class);
  }

  @Test
  public void testCreateDoesNotAddParentDirKeys() throws Exception {
    Path grandparent = new Path("/testCreateDoesNotAddParentDirKeys");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    ContractTestUtils.touch(fs, child);

    KeyInfo key = getKey(child, false);
    assertEquals(key.getKeyName(), o3fs.pathToKey(child));

    // Creating a child should not add parent keys to the bucket
    try {
      getKey(parent, true);
    } catch (IOException ex) {
      assertKeyNotFoundException(ex);
    }

    // List status on the parent should show the child file
    assertEquals("List status of parent should include the 1 child file", 1L,
        (long)fs.listStatus(parent).length);
    assertTrue("Parent directory does not appear to be a directory",
        fs.getFileStatus(parent).isDirectory());
  }

  @Test
  public void testDeleteCreatesFakeParentDir() throws Exception {
    Path grandparent = new Path("/testDeleteCreatesFakeParentDir");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    ContractTestUtils.touch(fs, child);

    // Verify that parent dir key does not exist
    // Creating a child should not add parent keys to the bucket
    try {
      getKey(parent, true);
    } catch (IOException ex) {
      assertKeyNotFoundException(ex);
    }

    // Delete the child key
    fs.delete(child, false);

    // Deleting the only child should create the parent dir key if it does
    // not exist
    String parentKey = o3fs.pathToKey(parent) + "/";
    KeyInfo parentKeyInfo = getKey(parent, true);
    assertEquals(parentKey, parentKeyInfo.getKeyName());
  }

  @Test
  public void testListStatus() throws Exception {
    Path parent = new Path("/testListStatus");
    Path file1 = new Path(parent, "key1");
    Path file2 = new Path(parent, "key1/key2");
    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);


    // ListStatus on a directory should return all subdirs along with
    // files, even if there exists a file and sub-dir with the same name.
    FileStatus[] fileStatuses = o3fs.listStatus(parent);
    assertEquals("FileStatus did not return all children of the directory",
        2, fileStatuses.length);

    // ListStatus should return only the immediate children of a directory.
    Path file3 = new Path(parent, "dir1/key3");
    Path file4 = new Path(parent, "dir1/key4");
    ContractTestUtils.touch(fs, file3);
    ContractTestUtils.touch(fs, file4);
    fileStatuses = o3fs.listStatus(parent);
    assertEquals("FileStatus did not return all children of the directory",
        3, fileStatuses.length);
  }

  /**
   * Tests listStatus operation on root directory.
   */
  @Test
  public void testListStatusOnRoot() throws Exception {
    Path root = new Path("/");
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    fs.mkdirs(dir12);
    fs.mkdirs(dir2);

    // ListStatus on root should return dir1 (even though /dir1 key does not
    // exist) and dir2 only. dir12 is not an immediate child of root and
    // hence should not be listed.
    FileStatus[] fileStatuses = o3fs.listStatus(root);
    assertEquals("FileStatus should return only the immediate children", 2,
        fileStatuses.length);

    // Verify that dir12 is not included in the result of the listStatus on root
    String fileStatus1 = fileStatuses[0].getPath().toUri().getPath();
    String fileStatus2 = fileStatuses[1].getPath().toUri().getPath();
    assertFalse(fileStatus1.equals(dir12.toString()));
    assertFalse(fileStatus2.equals(dir12.toString()));
  }

  /**
   * Tests listStatus on a path with subdirs.
   */
  @Test
  public void testListStatusOnSubDirs() throws Exception {
    // Create the following key structure
    //      /dir1/dir11/dir111
    //      /dir1/dir12
    //      /dir1/dir12/file121
    //      /dir2
    // ListStatus on /dir1 should return all its immediated subdirs only
    // which are /dir1/dir11 and /dir1/dir12. Super child files/dirs
    // (/dir1/dir12/file121 and /dir1/dir11/dir111) should not be returned by
    // listStatus.
    Path dir1 = new Path("/dir1");
    Path dir11 = new Path(dir1, "dir11");
    Path dir111 = new Path(dir11, "dir111");
    Path dir12 = new Path(dir1, "dir12");
    Path file121 = new Path(dir12, "file121");
    Path dir2 = new Path("/dir2");
    fs.mkdirs(dir111);
    fs.mkdirs(dir12);
    ContractTestUtils.touch(fs, file121);
    fs.mkdirs(dir2);

    FileStatus[] fileStatuses = o3fs.listStatus(dir1);
    assertEquals("FileStatus should return only the immediate children", 2,
        fileStatuses.length);

    // Verify that the two children of /dir1 returned by listStatus operation
    // are /dir1/dir11 and /dir1/dir12.
    String fileStatus1 = fileStatuses[0].getPath().toUri().getPath();
    String fileStatus2 = fileStatuses[1].getPath().toUri().getPath();
    assertTrue(fileStatus1.equals(dir11.toString()) ||
        fileStatus1.equals(dir12.toString()));
    assertTrue(fileStatus2.equals(dir11.toString()) ||
        fileStatus2.equals(dir12.toString()));
  }

  private KeyInfo getKey(Path keyPath, boolean isDirectory)
      throws IOException, OzoneException {
    String key = o3fs.pathToKey(keyPath);
    if (isDirectory) {
      key = key + "/";
    }
    KeyArgs parentKeyArgs = new KeyArgs(volumeName, bucketName, key,
        userArgs);
    return storageHandler.getKeyInfo(parentKeyArgs);
  }

  private void assertKeyNotFoundException(IOException ex) {
    GenericTestUtils.assertExceptionContains("KEY_NOT_FOUND", ex);
  }
}
