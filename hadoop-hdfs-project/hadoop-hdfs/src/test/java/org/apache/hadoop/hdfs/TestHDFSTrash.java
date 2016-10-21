/**
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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TestTrash;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test trash using HDFS
 */
public class TestHDFSTrash {

  public static final Log LOG = LogFactory.getLog(TestHDFSTrash.class);

  private static MiniDFSCluster cluster = null;
  private static FileSystem fs;
  private static Configuration conf = new HdfsConfiguration();

  private final static Path TEST_ROOT = new Path("/TestHDFSTrash-ROOT");
  private final static Path TRASH_ROOT = new Path("/TestHDFSTrash-TRASH");

  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String GROUP3_NAME = "group3";
  final private static String USER1_NAME = "user1";
  final private static String USER2_NAME = "user2";

  private static UserGroupInformation superUser;
  private static UserGroupInformation user1;
  private static UserGroupInformation user2;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    fs = FileSystem.get(conf);

    superUser = UserGroupInformation.getCurrentUser();
    user1 = UserGroupInformation.createUserForTesting(USER1_NAME,
        new String[] {GROUP1_NAME, GROUP2_NAME});
    user2 = UserGroupInformation.createUserForTesting(USER2_NAME,
        new String[] {GROUP2_NAME, GROUP3_NAME});

    // Init test and trash root dirs in HDFS
    fs.mkdirs(TEST_ROOT);
    fs.setPermission(TEST_ROOT, new FsPermission((short) 0777));
    DFSTestUtil.verifyFilePermission(
        fs.getFileStatus(TEST_ROOT),
        superUser.getShortUserName(),
        null, FsAction.ALL, FsAction.ALL, FsAction.ALL);

    fs.mkdirs(TRASH_ROOT);
    fs.setPermission(TRASH_ROOT, new FsPermission((short) 0777));
    DFSTestUtil.verifyFilePermission(
        fs.getFileStatus(TRASH_ROOT),
        superUser.getShortUserName(),
        null, FsAction.ALL, FsAction.ALL, FsAction.ALL);
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) { cluster.shutdown(); }
  }

  @Test
  public void testTrash() throws IOException {
    TestTrash.trashShell(cluster.getFileSystem(), new Path("/"));
  }

  @Test
  public void testNonDefaultFS() throws IOException {
    FileSystem fileSystem = cluster.getFileSystem();
    Configuration config = fileSystem.getConf();
    config.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        fileSystem.getUri().toString());
    TestTrash.trashNonDefaultFS(config);
  }

  @Test
  public void testHDFSTrashPermission() throws IOException {
    FileSystem fileSystem = cluster.getFileSystem();
    Configuration config = fileSystem.getConf();
    config.set(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, "0.2");
    TestTrash.verifyTrashPermission(fileSystem, config);
  }

  @Test
  public void testMoveEmptyDirToTrash() throws IOException {
    FileSystem fileSystem = cluster.getFileSystem();
    Configuration config = fileSystem.getConf();
    config.set(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, "1");
    TestTrash.verifyMoveEmptyDirToTrash(fileSystem, config);
  }

  @Test
  public void testDeleteTrash() throws Exception {
    Configuration testConf = new Configuration(conf);
    testConf.set(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, "10");

    Path user1Tmp = new Path(TEST_ROOT, "test-del-u1");
    Path user2Tmp = new Path(TEST_ROOT, "test-del-u2");

    // login as user1, move something to trash
    // verify user1 can remove its own trash dir
    fs = DFSTestUtil.login(fs, testConf, user1);
    fs.mkdirs(user1Tmp);
    Trash u1Trash = getPerUserTrash(user1, fs, testConf);
    Path u1t = u1Trash.getCurrentTrashDir(user1Tmp);
    assertTrue(String.format("Failed to move %s to trash", user1Tmp),
        u1Trash.moveToTrash(user1Tmp));
    assertTrue(
        String.format(
            "%s should be allowed to remove its own trash directory %s",
            user1.getUserName(), u1t),
        fs.delete(u1t, true));
    assertFalse(fs.exists(u1t));

    // login as user2, move something to trash
    fs = DFSTestUtil.login(fs, testConf, user2);
    fs.mkdirs(user2Tmp);
    Trash u2Trash = getPerUserTrash(user2, fs, testConf);
    u2Trash.moveToTrash(user2Tmp);
    Path u2t = u2Trash.getCurrentTrashDir(user2Tmp);

    try {
      // user1 should not be able to remove user2's trash dir
      fs = DFSTestUtil.login(fs, testConf, user1);
      fs.delete(u2t, true);
      fail(String.format("%s should not be able to remove %s trash directory",
              USER1_NAME, USER2_NAME));
    } catch (AccessControlException e) {
      assertTrue(e instanceof AccessControlException);
      assertTrue("Permission denied messages must carry the username",
          e.getMessage().contains(USER1_NAME));
    }
  }

  /**
   * Return a {@link Trash} instance using giving configuration.
   * The trash root directory is set to an unique directory under
   * {@link #TRASH_ROOT}. Use this method to isolate trash
   * directories for different users.
   */
  private Trash getPerUserTrash(UserGroupInformation ugi,
      FileSystem fileSystem, Configuration config) throws IOException {
    // generate an unique path per instance
    UUID trashId = UUID.randomUUID();
    StringBuffer sb = new StringBuffer()
        .append(ugi.getUserName())
        .append("-")
        .append(trashId.toString());
    Path userTrashRoot = new Path(TRASH_ROOT, sb.toString());
    FileSystem spyUserFs = Mockito.spy(fileSystem);
    Mockito.when(spyUserFs.getTrashRoot(Mockito.any()))
        .thenReturn(userTrashRoot);
    return new Trash(spyUserFs, config);
  }
}
