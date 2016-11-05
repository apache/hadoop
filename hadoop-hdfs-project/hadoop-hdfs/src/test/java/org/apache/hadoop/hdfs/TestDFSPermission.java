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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for permission */
public class TestDFSPermission {
  public static final Log LOG = LogFactory.getLog(TestDFSPermission.class);
  final private static Configuration conf = new HdfsConfiguration();
  
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String GROUP3_NAME = "group3";
  final private static String GROUP4_NAME = "group4";
  final private static String USER1_NAME = "user1";
  final private static String USER2_NAME = "user2";
  final private static String USER3_NAME = "user3";

  private static final UserGroupInformation SUPERUSER;
  private static final UserGroupInformation USER1;
  private static final UserGroupInformation USER2;
  private static final UserGroupInformation USER3;

  final private static short MAX_PERMISSION = 511;
  final private static short DEFAULT_UMASK = 022;
  final private static FsPermission DEFAULT_PERMISSION = 
    FsPermission.createImmutable((short) 0777);
  final static private int NUM_TEST_PERMISSIONS = 
    conf.getInt("test.dfs.permission.num", 10) * (MAX_PERMISSION + 1) / 100;

  final private static String PATH_NAME = "xx";
  final private static Path FILE_DIR_PATH = new Path("/", PATH_NAME);
  final private static Path NON_EXISTENT_PATH = new Path("/parent", PATH_NAME);
  final private static Path NON_EXISTENT_FILE = new Path("/NonExistentFile");

  private FileSystem fs;
  private MiniDFSCluster cluster;
  private static final Random r;

  static {
    try {
      // Initiate the random number generator and logging the seed
      long seed = Time.now();
      r = new Random(seed);
      LOG.info("Random number generator uses seed " + seed);
      LOG.info("NUM_TEST_PERMISSIONS=" + NUM_TEST_PERMISSIONS);
      
      // explicitly turn on permission checking
      conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
      
      // create fake mapping for the groups
      Map<String, String[]> u2g_map = new HashMap<String, String[]> (3);
      u2g_map.put(USER1_NAME, new String[] {GROUP1_NAME, GROUP2_NAME });
      u2g_map.put(USER2_NAME, new String[] {GROUP2_NAME, GROUP3_NAME });
      u2g_map.put(USER3_NAME, new String[] {GROUP3_NAME, GROUP4_NAME });
      DFSTestUtil.updateConfWithFakeGroupMapping(conf, u2g_map);
      
      // Initiate all four users
      SUPERUSER = UserGroupInformation.getCurrentUser();
      USER1 = UserGroupInformation.createUserForTesting(USER1_NAME,
          new String[] { GROUP1_NAME, GROUP2_NAME });
      USER2 = UserGroupInformation.createUserForTesting(USER2_NAME,
          new String[] { GROUP2_NAME, GROUP3_NAME });
      USER3 = UserGroupInformation.createUserForTesting(USER3_NAME,
          new String[] { GROUP3_NAME, GROUP4_NAME });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setUp() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
  }
  
  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
  
  /** This tests if permission setting in create, mkdir, and 
   * setPermission works correctly
   */
  @Test
  public void testPermissionSetting() throws Exception {
    testPermissionSetting(OpType.CREATE); // test file creation
    testPermissionSetting(OpType.MKDIRS); // test directory creation
  }

  private void initFileSystem(short umask) throws Exception {
    // set umask in configuration, converting to padded octal
    conf.set(FsPermission.UMASK_LABEL, String.format("%1$03o", umask));
    fs = FileSystem.get(conf);
  }

  private void closeFileSystem() throws Exception {
    fs.close();
  }
  
  /* check permission setting works correctly for file or directory */
  private void testPermissionSetting(OpType op) throws Exception {
    short uMask = DEFAULT_UMASK;
    // case 1: use default permission but all possible umasks
    PermissionGenerator generator = new PermissionGenerator(r);
    FsPermission permission = new FsPermission(DEFAULT_PERMISSION);
    for (short i = 0; i < NUM_TEST_PERMISSIONS; i++) {
      uMask = generator.next();
      initFileSystem(uMask);
      createAndCheckPermission(op, FILE_DIR_PATH, uMask, permission, true);
      closeFileSystem();
    }
    // case 2: use permission 0643 and the default umask
    uMask = DEFAULT_UMASK;
    initFileSystem(uMask);
    createAndCheckPermission(op, FILE_DIR_PATH, uMask, new FsPermission(
        (short) 0643), true);
    closeFileSystem();

    // case 3: use permission 0643 and umask 0222
    uMask = (short) 0222;
    initFileSystem(uMask);
    createAndCheckPermission(op, FILE_DIR_PATH, uMask, new FsPermission(
        (short) 0643), false);
    closeFileSystem();

    // case 4: set permission
    uMask = (short) 0111;
    initFileSystem(uMask);
    fs.setPermission(FILE_DIR_PATH, new FsPermission(uMask));
    short expectedPermission = (short) 0111;
    checkPermission(FILE_DIR_PATH, expectedPermission, true);
    closeFileSystem();

    // case 5: test non-existent parent directory
    uMask = DEFAULT_UMASK;
    initFileSystem(uMask);
    assertFalse("File shouldn't exists", fs.exists(NON_EXISTENT_PATH));
    createAndCheckPermission(op, NON_EXISTENT_PATH, uMask, new FsPermission(
        DEFAULT_PERMISSION), false);
    Path parent = NON_EXISTENT_PATH.getParent();
    checkPermission(parent, getPermission(parent.getParent()), true);
    closeFileSystem();
  }

  /* get the permission of a file/directory */
  private short getPermission(Path path) throws IOException {
    return fs.getFileStatus(path).getPermission().toShort();
  }

  private void create(OpType op, Path name) throws IOException {
    create(fs, conf, op, name);
  }

  /* create a file/directory with the default umask and permission */
  static void create(final FileSystem fs, final Configuration fsConf,
      OpType op, Path name) throws IOException {
    create(fs, fsConf, op, name, DEFAULT_UMASK, new FsPermission(
        DEFAULT_PERMISSION));
  }

  private void create(OpType op, Path name, short umask,
      FsPermission permission)
      throws IOException {
    create(fs, conf, op, name, umask, permission);
  }

  /* create a file/directory with the given umask and permission */
  static void create(final FileSystem fs, final Configuration fsConf,
      OpType op, Path name, short umask, FsPermission permission)
      throws IOException {
    // set umask in configuration, converting to padded octal
    fsConf.set(FsPermission.UMASK_LABEL, String.format("%1$03o", umask));

    // create the file/directory
    switch (op) {
    case CREATE:
      FSDataOutputStream out = fs.create(name, permission, true, 
          fsConf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
          fs.getDefaultReplication(name), fs.getDefaultBlockSize(name), null);
      out.close();
      break;
    case MKDIRS:
      fs.mkdirs(name, permission);
      break;
    default:
      throw new IOException("Unsupported operation: " + op);
    }
  }

  /* create file/directory with the provided umask and permission; then it
   * checks if the permission is set correctly;
   * If the delete flag is true, delete the file afterwards; otherwise leave
   * it in the file system.
   */
  private void createAndCheckPermission(OpType op, Path name, short umask,
      FsPermission permission, boolean delete) throws Exception {
    // create the file/directory
    create(op, name, umask, permission);

    // get the short form of the permission
    short permissionNum = (DEFAULT_PERMISSION.equals(permission)) ? MAX_PERMISSION
        : permission.toShort();

    // get the expected permission
    short expectedPermission = (op == OpType.CREATE) ? (short) (~umask
        & permissionNum) : (short) (~umask & permissionNum);

    // check if permission is correctly set
    checkPermission(name, expectedPermission, delete);
  }

  /* Check if the permission of a file/directory is the same as the
   * expected permission; If the delete flag is true, delete the
   * file/directory afterwards.
   */
  private void checkPermission(Path name, short expectedPermission,
      boolean delete) throws IOException {
    try {
      // check its permission
      assertEquals(getPermission(name), expectedPermission);
    } finally {
      // delete the file
      if (delete) {
        fs.delete(name, true);
      }
    }
  }

  /**
   * check that ImmutableFsPermission can be used as the argument
   * to setPermission
   */
  @Test
  public void testImmutableFsPermission() throws IOException {
    fs = FileSystem.get(conf);

    // set the permission of the root to be world-wide rwx
    fs.setPermission(new Path("/"),
        FsPermission.createImmutable((short)0777));
  }
  
  @Test(timeout=30000)
  public void testTrashPermission() throws Exception {
    //  /BSS                  user1:group2 777
    //   /BSS/user1            user1:group2 755
    //   /BSS/user1/test       user1:group1 600
    Path rootDir = new Path("/BSS");
    Path user1Dir = new Path("/BSS/user1");
    Path user1File = new Path("/BSS/user1/test");

    try {
      conf.set(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, "10");
      fs = FileSystem.get(conf);

      fs.mkdirs(rootDir);
      fs.setPermission(rootDir, new FsPermission((short) 0777));

      fs = DFSTestUtil.login(fs, conf, USER1);
      fs.mkdirs(user1Dir);
      fs.setPermission(user1Dir, new FsPermission((short) 0755));
      fs.setOwner(user1Dir, USER1.getShortUserName(), GROUP2_NAME);

      create(OpType.CREATE, user1File);
      fs.setOwner(user1File, USER1.getShortUserName(), GROUP1_NAME);
      fs.setPermission(user1File, new FsPermission((short) 0600));

      try {
        // login as user2, attempt to delete /BSS/user1
        // this should fail because user2 has no permission to
        // its sub directory.
        fs = DFSTestUtil.login(fs, conf, USER2);
        fs.delete(user1Dir, true);
        fail("User2 should not be allowed to delete user1's dir.");
      } catch (AccessControlException e) {
        e.printStackTrace();
        assertTrue("Permission denied messages must carry the username",
            e.getMessage().contains(USER2_NAME));
      }

      // ensure the /BSS/user1 still exists
      assertTrue(fs.exists(user1Dir));

      try {
        fs = DFSTestUtil.login(fs, conf, SUPERUSER);
        Trash trash = new Trash(fs, conf);
        Path trashRoot = trash.getCurrentTrashDir(user1Dir);
        while(true) {
          trashRoot = trashRoot.getParent();
          if(trashRoot.getParent().isRoot()) {
            break;
          }
        }
        fs.mkdirs(trashRoot);
        fs.setPermission(trashRoot, new FsPermission((short) 0777));

        // login as user2, attempt to move /BSS/user1 to trash
        // this should also fail otherwise the directory will be
        // removed by trash emptier (emptier is running by superuser)
        fs = DFSTestUtil.login(fs, conf, USER2);
        Trash userTrash = new Trash(fs, conf);
        assertTrue(userTrash.isEnabled());
        userTrash.moveToTrash(user1Dir);
        fail("User2 should not be allowed to move"
            + "user1's dir to trash");
      } catch (IOException e) {
        // expect the exception is caused by permission denied
        assertTrue(e.getCause() instanceof AccessControlException);
        e.printStackTrace();
        assertTrue("Permission denied messages must carry the username",
            e.getCause().getMessage().contains(USER2_NAME));
      }

      // ensure /BSS/user1 still exists
      assertEquals(fs.exists(user1Dir), true);
    } finally {
      fs = DFSTestUtil.login(fs, conf, SUPERUSER);
      fs.delete(rootDir, true);
      conf.set(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, "0");
    }
  }

  /* check if the ownership of a file/directory is set correctly */
  @Test
  public void testOwnership() throws Exception {
    testOwnership(OpType.CREATE); // test file creation
    testOwnership(OpType.MKDIRS); // test directory creation
  }

  /* change a file/directory's owner and group.
   * if expectDeny is set, expect an AccessControlException.
   */
  private void setOwner(Path path, String owner, String group,
      boolean expectDeny) throws IOException {
    try {
      String expectedOwner = (owner == null) ? getOwner(path) : owner;
      String expectedGroup = (group == null) ? getGroup(path) : group;
      fs.setOwner(path, owner, group);
      checkOwnership(path, expectedOwner, expectedGroup);
      assertFalse(expectDeny);
    } catch(AccessControlException e) {
      assertTrue(expectDeny);
    }
  }

  /* check ownership is set correctly for a file or directory */
  private void testOwnership(OpType op) throws Exception {
    // case 1: superuser create a file/directory
    fs = FileSystem.get(conf);
    create(op, FILE_DIR_PATH, DEFAULT_UMASK,
        new FsPermission(DEFAULT_PERMISSION));
    checkOwnership(FILE_DIR_PATH, SUPERUSER.getShortUserName(),
        getGroup(FILE_DIR_PATH.getParent()));

    // case 2: superuser changes FILE_DIR_PATH's owner to be <user1, group3>
    setOwner(FILE_DIR_PATH, USER1.getShortUserName(), GROUP3_NAME, false);

    // case 3: user1 changes FILE_DIR_PATH's owner to be user2
    fs = DFSTestUtil.login(fs, conf, USER1);
    setOwner(FILE_DIR_PATH, USER2.getShortUserName(), null, true);

    // case 4: user1 changes FILE_DIR_PATH's group to be group1 which it belongs
    // to
    setOwner(FILE_DIR_PATH, null, GROUP1_NAME, false);

    // case 5: user1 changes FILE_DIR_PATH's group to be group3
    // which it does not belong to
    setOwner(FILE_DIR_PATH, null, GROUP3_NAME, true);

    // case 6: user2 (non-owner) changes FILE_DIR_PATH's group to be group3
    fs = DFSTestUtil.login(fs, conf, USER2);
    setOwner(FILE_DIR_PATH, null, GROUP3_NAME, true);

    // case 7: user2 (non-owner) changes FILE_DIR_PATH's user to be user2
    setOwner(FILE_DIR_PATH, USER2.getShortUserName(), null, true);

    // delete the file/directory
    fs = DFSTestUtil.login(fs, conf, SUPERUSER);
    fs.delete(FILE_DIR_PATH, true);
  }

  /* Return the group owner of the file/directory */
  private String getGroup(Path path) throws IOException {
    return fs.getFileStatus(path).getGroup();
  }

  /* Return the file owner of the file/directory */
  private String getOwner(Path path) throws IOException {
    return fs.getFileStatus(path).getOwner();
  }

  /* check if ownership is set correctly */
  private void checkOwnership(Path name, String expectedOwner,
      String expectedGroup) throws IOException {
    // check its owner and group
    FileStatus status = fs.getFileStatus(name);
    assertEquals(status.getOwner(), expectedOwner);
    assertEquals(status.getGroup(), expectedGroup);
  }

  final static private String ANCESTOR_NAME = "/ancestor";
  final static private String PARENT_NAME = "parent";
  final static private String FILE_NAME = "file";
  final static private String DIR_NAME = "dir";
  final static private String FILE_DIR_NAME = "filedir";

  enum OpType {CREATE, MKDIRS, OPEN, SET_REPLICATION,
    GET_FILEINFO, IS_DIR, EXISTS, GET_CONTENT_LENGTH, LIST, RENAME, DELETE
  };

  /* Check if namenode performs permission checking correctly for
   * superuser, file owner, group owner, and other users */
  @Test
  public void testPermissionChecking() throws Exception {
    try {
      fs = FileSystem.get(conf);

      // set the permission of the root to be world-wide rwx
      fs.setPermission(new Path("/"), new FsPermission((short)0777));
      
      // create a directory hierarchy and sets random permission for each inode
      PermissionGenerator ancestorPermissionGenerator = 
        new PermissionGenerator(r);
      PermissionGenerator dirPermissionGenerator = new PermissionGenerator(r);
      PermissionGenerator filePermissionGenerator = new PermissionGenerator(r);
      short[] ancestorPermissions = new short[NUM_TEST_PERMISSIONS];
      short[] parentPermissions = new short[NUM_TEST_PERMISSIONS];
      short[] permissions = new short[NUM_TEST_PERMISSIONS];
      Path[] ancestorPaths = new Path[NUM_TEST_PERMISSIONS];
      Path[] parentPaths = new Path[NUM_TEST_PERMISSIONS];
      Path[] filePaths = new Path[NUM_TEST_PERMISSIONS];
      Path[] dirPaths = new Path[NUM_TEST_PERMISSIONS];
      for (int i = 0; i < NUM_TEST_PERMISSIONS; i++) {
        // create ancestor directory
        ancestorPaths[i] = new Path(ANCESTOR_NAME + i);
        create(OpType.MKDIRS, ancestorPaths[i]);
        fs.setOwner(ancestorPaths[i], USER1_NAME, GROUP2_NAME);
        // create parent directory
        parentPaths[i] = new Path(ancestorPaths[i], PARENT_NAME + i);
        create(OpType.MKDIRS, parentPaths[i]);
        // change parent directory's ownership to be user1
        fs.setOwner(parentPaths[i], USER1_NAME, GROUP2_NAME);

        filePaths[i] = new Path(parentPaths[i], FILE_NAME + i);
        dirPaths[i] = new Path(parentPaths[i], DIR_NAME + i);

        // makes sure that each inode at the same level 
        // has a different permission
        ancestorPermissions[i] = ancestorPermissionGenerator.next();
        parentPermissions[i] = dirPermissionGenerator.next();
        permissions[i] = filePermissionGenerator.next();
        fs.setPermission(ancestorPaths[i], new FsPermission(
            ancestorPermissions[i]));
        fs.setPermission(parentPaths[i], new FsPermission(
                parentPermissions[i]));
      }

      /* file owner */
      testPermissionCheckingPerUser(USER1, ancestorPermissions,
          parentPermissions, permissions, parentPaths, filePaths, dirPaths);
      /* group owner */
      testPermissionCheckingPerUser(USER2, ancestorPermissions,
          parentPermissions, permissions, parentPaths, filePaths, dirPaths);
      /* other owner */
      testPermissionCheckingPerUser(USER3, ancestorPermissions,
          parentPermissions, permissions, parentPaths, filePaths, dirPaths);
      /* super owner */
      testPermissionCheckingPerUser(SUPERUSER, ancestorPermissions,
          parentPermissions, permissions, parentPaths, filePaths, dirPaths);
    } finally {
      fs.close();
    }
  }

  @Test
  public void testAccessOwner() throws IOException, InterruptedException {
    FileSystem rootFs = FileSystem.get(conf);
    Path p1 = new Path("/p1");
    rootFs.mkdirs(p1);
    rootFs.setOwner(p1, USER1_NAME, GROUP1_NAME);
    fs = USER1.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return FileSystem.get(conf);
      }
    });
    fs.setPermission(p1, new FsPermission((short) 0444));
    fs.access(p1, FsAction.READ);
    try {
      fs.access(p1, FsAction.WRITE);
      fail("The access call should have failed.");
    } catch (AccessControlException e) {
      assertTrue("Permission denied messages must carry the username",
              e.getMessage().contains(USER1_NAME));
      assertTrue("Permission denied messages must carry the path parent",
              e.getMessage().contains(
                  p1.getParent().toUri().getPath()));
    }

    Path badPath = new Path("/bad/bad");
    try {
      fs.access(badPath, FsAction.READ);
      fail("The access call should have failed");
    } catch (FileNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testAccessGroupMember() throws IOException, InterruptedException {
    FileSystem rootFs = FileSystem.get(conf);
    Path p2 = new Path("/p2");
    rootFs.mkdirs(p2);
    rootFs.setOwner(p2, UserGroupInformation.getCurrentUser().getShortUserName(), GROUP1_NAME);
    rootFs.setPermission(p2, new FsPermission((short) 0740));
    fs = USER1.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return FileSystem.get(conf);
      }
    });
    fs.access(p2, FsAction.READ);
    try {
      fs.access(p2, FsAction.EXECUTE);
      fail("The access call should have failed.");
    } catch (AccessControlException e) {
      assertTrue("Permission denied messages must carry the username",
              e.getMessage().contains(USER1_NAME));
      assertTrue("Permission denied messages must carry the path parent",
              e.getMessage().contains(
                  p2.getParent().toUri().getPath()));
    }
  }

  @Test
  public void testAccessOthers() throws IOException, InterruptedException {
    FileSystem rootFs = FileSystem.get(conf);
    Path p3 = new Path("/p3");
    rootFs.mkdirs(p3);
    rootFs.setPermission(p3, new FsPermission((short) 0774));
    fs = USER1.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return FileSystem.get(conf);
      }
    });
    fs.access(p3, FsAction.READ);
    try {
      fs.access(p3, FsAction.READ_WRITE);
      fail("The access call should have failed.");
    } catch (AccessControlException e) {
      assertTrue("Permission denied messages must carry the username",
              e.getMessage().contains(USER1_NAME));
      assertTrue("Permission denied messages must carry the path parent",
              e.getMessage().contains(
                  p3.getParent().toUri().getPath()));
    }
  }

  @Test
  public void testPermissionMessageOnNonDirAncestor()
      throws IOException, InterruptedException {
    FileSystem rootFs = FileSystem.get(conf);
    Path p4 = new Path("/p4");
    rootFs.mkdirs(p4);
    rootFs.setOwner(p4, USER1_NAME, GROUP1_NAME);

    final Path fpath = new Path("/p4/file");
    DataOutputStream out = rootFs.create(fpath);
    out.writeBytes("dhruba: " + fpath);
    out.close();
    rootFs.setOwner(fpath, USER1_NAME, GROUP1_NAME);
    assertTrue(rootFs.exists(fpath));

    fs = USER1.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return FileSystem.get(conf);
      }
    });

    final Path nfpath = new Path("/p4/file/nonexisting");
    assertFalse(rootFs.exists(nfpath));

    try {
      fs.exists(nfpath);
      fail("The exists call should have failed.");
    } catch (AccessControlException e) {
      assertTrue("Permission denied messages must carry file path",
          e.getMessage().contains(fpath.getName()));
      assertTrue("Permission denied messages must specify existing_file is not "
              + "a directory, when checked on /existing_file/non_existing_name",
          e.getMessage().contains("is not a directory"));
    }

    rootFs.setPermission(p4, new FsPermission("600"));
    try {
      fs.exists(nfpath);
      fail("The exists call should have failed.");
    } catch (AccessControlException e) {
      assertFalse("Permission denied messages must not carry full file path,"
              + "since the user does not have permission on /p4: "
              + e.getMessage(),
          e.getMessage().contains(fpath.getName()));
      assertFalse("Permission denied messages must not specify /p4"
          + " is not a directory: " + e.getMessage(),
          e.getMessage().contains("is not a directory"));
    }
  }

  /* Check if namenode performs permission checking correctly
   * for the given user for operations mkdir, open, setReplication,
   * getFileInfo, isDirectory, exists, getContentLength, list, rename,
   * and delete */
  private void testPermissionCheckingPerUser(UserGroupInformation ugi,
      short[] ancestorPermission, short[] parentPermission,
      short[] filePermission, Path[] parentDirs, Path[] files, Path[] dirs)
      throws Exception {
    boolean[] isDirEmpty = new boolean[NUM_TEST_PERMISSIONS];
    fs = DFSTestUtil.login(fs, conf, SUPERUSER);
    for (int i = 0; i < NUM_TEST_PERMISSIONS; i++) {
      create(OpType.CREATE, files[i]);
      create(OpType.MKDIRS, dirs[i]);
      fs.setOwner(files[i], USER1_NAME, GROUP2_NAME);
      fs.setOwner(dirs[i], USER1_NAME, GROUP2_NAME);
      checkOwnership(dirs[i], USER1_NAME, GROUP2_NAME);
      checkOwnership(files[i], USER1_NAME, GROUP2_NAME);

      FsPermission fsPermission = new FsPermission(filePermission[i]);
      fs.setPermission(files[i], fsPermission);
      fs.setPermission(dirs[i], fsPermission);

      isDirEmpty[i] = (fs.listStatus(dirs[i]).length == 0);
    }

    fs = DFSTestUtil.login(fs, conf, ugi);
    for (int i = 0; i < NUM_TEST_PERMISSIONS; i++) {
      testCreateMkdirs(ugi, new Path(parentDirs[i], FILE_DIR_NAME),
          ancestorPermission[i], parentPermission[i]);
      testOpen(ugi, files[i], ancestorPermission[i], parentPermission[i],
          filePermission[i]);
      testSetReplication(ugi, files[i], ancestorPermission[i],
          parentPermission[i], filePermission[i]);
      testSetTimes(ugi, files[i], ancestorPermission[i],
          parentPermission[i], filePermission[i]);
      testStats(ugi, files[i], ancestorPermission[i], parentPermission[i]);
      testList(ugi, files[i], dirs[i], ancestorPermission[i],
          parentPermission[i], filePermission[i]);
      int next = i == NUM_TEST_PERMISSIONS - 1 ? 0 : i + 1;
      testRename(ugi, files[i], files[next], ancestorPermission[i],
          parentPermission[i], ancestorPermission[next], parentPermission[next]);
      testDeleteFile(ugi, files[i], ancestorPermission[i], parentPermission[i]);
      testDeleteDir(ugi, dirs[i], ancestorPermission[i], parentPermission[i],
          filePermission[i], null, isDirEmpty[i]);
    }
    
    // test non existent file
    checkNonExistentFile();
  }

  /* A random permission generator that guarantees that each permission
   * value is generated only once.
   */
  static class PermissionGenerator {
    private final Random r;
    private final short[] permissions = new short[MAX_PERMISSION + 1];
    private int numLeft = MAX_PERMISSION + 1;

    PermissionGenerator(Random r) {
      this.r = r;
      for (int i = 0; i <= MAX_PERMISSION; i++) {
        permissions[i] = (short) i;
      }
    }

    short next() throws IOException {
      if (numLeft == 0) {
        throw new IOException("No more permission is avaialbe");
      }
      int index = r.nextInt(numLeft); // choose which permission to return
      numLeft--; // decrement the counter

      // swap the chosen permission with last available permission in the array
      short temp = permissions[numLeft];
      permissions[numLeft] = permissions[index];
      permissions[index] = temp;

      return permissions[numLeft];
    }
  }

  /* A base class that verifies the permission checking is correct 
   * for an operation */
  abstract class PermissionVerifier {
    protected Path path;
    protected short ancestorPermission;
    protected short parentPermission;
    private short permission;
    protected short requiredAncestorPermission;
    protected short requiredParentPermission;
    protected short requiredPermission;
    final static protected short opAncestorPermission = SEARCH_MASK;
    protected short opParentPermission;
    protected short opPermission;
    protected UserGroupInformation ugi;

    /* initialize */
    protected void set(Path path, short ancestorPermission,
        short parentPermission, short permission) {
      this.path = path;
      this.ancestorPermission = ancestorPermission;
      this.parentPermission = parentPermission;
      this.permission = permission;
      setOpPermission();
      this.ugi = null;
    }

    /* Perform an operation and verify if the permission checking is correct */
    void verifyPermission(UserGroupInformation ugi) throws IOException {
      if (this.ugi != ugi) {
        setRequiredPermissions(ugi);
        this.ugi = ugi;
      }

      try {
        try {
          call();
          assertFalse(expectPermissionDeny());
        } catch(AccessControlException e) {
          assertTrue(expectPermissionDeny());
        }
      } catch (AssertionError ae) {
        logPermissions();
        throw ae;
      }
    }

    /** Log the permissions and required permissions */
    protected void logPermissions() {
      LOG.info("required ancestor permission:"
          + Integer.toOctalString(requiredAncestorPermission));
      LOG.info("ancestor permission: "
          + Integer.toOctalString(ancestorPermission));
      LOG.info("required parent permission:"
          + Integer.toOctalString(requiredParentPermission));
      LOG.info("parent permission: " + Integer.toOctalString(parentPermission));
      LOG.info("required permission:"
          + Integer.toOctalString(requiredPermission));
      LOG.info("permission: " + Integer.toOctalString(permission));
    }

    /* Return true if an AccessControlException is expected */
    protected boolean expectPermissionDeny() {
      return (requiredPermission & permission) != requiredPermission
          || (requiredParentPermission & parentPermission) !=
                            requiredParentPermission
          || (requiredAncestorPermission & ancestorPermission) !=
                            requiredAncestorPermission;
    }

    /* Set the permissions required to pass the permission checking */
    protected void setRequiredPermissions(UserGroupInformation ugi) {
      if (SUPERUSER.equals(ugi)) {
        requiredAncestorPermission = SUPER_MASK;
        requiredParentPermission = SUPER_MASK;
        requiredPermission = SUPER_MASK;
      } else if (USER1.equals(ugi)) {
        requiredAncestorPermission = (short)(opAncestorPermission & OWNER_MASK);
        requiredParentPermission = (short)(opParentPermission & OWNER_MASK);
        requiredPermission = (short)(opPermission & OWNER_MASK);
      } else if (USER2.equals(ugi)) {
        requiredAncestorPermission = (short)(opAncestorPermission & GROUP_MASK);
        requiredParentPermission = (short)(opParentPermission & GROUP_MASK);
        requiredPermission = (short)(opPermission & GROUP_MASK);
      } else if (USER3.equals(ugi)) {
        requiredAncestorPermission = (short)(opAncestorPermission & OTHER_MASK);
        requiredParentPermission = (short)(opParentPermission & OTHER_MASK);
        requiredPermission = (short)(opPermission & OTHER_MASK);
      } else {
        throw new IllegalArgumentException("Non-supported user: " + ugi);
      }
    }

    /* Set the rwx permissions required for the operation */
    abstract void setOpPermission();

    /* Perform the operation */
    abstract void call() throws IOException;
  }

  final static private short SUPER_MASK = 0;
  final static private short READ_MASK = 0444;
  final static private short WRITE_MASK = 0222;
  final static private short SEARCH_MASK = 0111;
  final static private short NULL_MASK = 0;
  final static private short OWNER_MASK = 0700;
  final static private short GROUP_MASK = 0070;
  final static private short OTHER_MASK = 0007;

  /* A class that verifies the permission checking is correct for create/mkdir*/
  private class CreatePermissionVerifier extends PermissionVerifier {
    private OpType opType;
    private boolean cleanup = true;

    /* initialize */
    protected void set(Path path, OpType opType, short ancestorPermission,
        short parentPermission) {
      super.set(path, ancestorPermission, parentPermission, NULL_MASK);
      setOpType(opType);
    }

    void setCleanup(boolean cleanup) {
      this.cleanup = cleanup;
    }
    
    /* set if the operation mkdir/create */
    void setOpType(OpType opType) {
      this.opType = opType;
    }

    @Override
    void setOpPermission() {
      this.opParentPermission = SEARCH_MASK | WRITE_MASK;
    }

    @Override
    void call() throws IOException {
      create(opType, path);
      if (cleanup) {
        fs.delete(path, true);
      }
    }
  }

  private final CreatePermissionVerifier createVerifier =
    new CreatePermissionVerifier();
  /* test if the permission checking of create/mkdir is correct */
  private void testCreateMkdirs(UserGroupInformation ugi, Path path,
      short ancestorPermission, short parentPermission) throws Exception {
    createVerifier.set(path, OpType.MKDIRS, ancestorPermission,
        parentPermission);
    createVerifier.verifyPermission(ugi);
    createVerifier.setOpType(OpType.CREATE);
    createVerifier.setCleanup(false);
    createVerifier.verifyPermission(ugi);
    createVerifier.setCleanup(true);
    createVerifier.verifyPermission(ugi); // test overWritten
  }

  /* A class that verifies the permission checking is correct for open */
  private class OpenPermissionVerifier extends PermissionVerifier {
    @Override
    void setOpPermission() {
      this.opParentPermission = SEARCH_MASK;
      this.opPermission = READ_MASK;
    }

    @Override
    void call() throws IOException {
      FSDataInputStream in = fs.open(path);
      in.close();
    }
  }

  private final OpenPermissionVerifier openVerifier = new OpenPermissionVerifier();
  /* test if the permission checking of open is correct */
  private void testOpen(UserGroupInformation ugi, Path path,
      short ancestorPermission, short parentPermission, short filePermission)
      throws Exception {
    openVerifier
        .set(path, ancestorPermission, parentPermission, filePermission);
    openVerifier.verifyPermission(ugi);
  }

  /* A class that verifies the permission checking is correct for 
   * setReplication */
  private class SetReplicationPermissionVerifier extends PermissionVerifier {
    @Override
    void setOpPermission() {
      this.opParentPermission = SEARCH_MASK;
      this.opPermission = WRITE_MASK;
    }

    @Override
    void call() throws IOException {
      fs.setReplication(path, (short) 1);
    }
  }

  private final SetReplicationPermissionVerifier replicatorVerifier =
    new SetReplicationPermissionVerifier();
  /* test if the permission checking of setReplication is correct */
  private void testSetReplication(UserGroupInformation ugi, Path path,
      short ancestorPermission, short parentPermission, short filePermission)
      throws Exception {
    replicatorVerifier.set(path, ancestorPermission, parentPermission,
        filePermission);
    replicatorVerifier.verifyPermission(ugi);
  }

  /* A class that verifies the permission checking is correct for 
   * setTimes */
  private class SetTimesPermissionVerifier extends PermissionVerifier {
    @Override
    void setOpPermission() {
      this.opParentPermission = SEARCH_MASK;
      this.opPermission = WRITE_MASK;
    }

    @Override
    void call() throws IOException {
      fs.setTimes(path, 100, 100);
      fs.setTimes(path, -1, 100);
      fs.setTimes(path, 100, -1);
    }
  }

  private final SetTimesPermissionVerifier timesVerifier =
    new SetTimesPermissionVerifier();
  /* test if the permission checking of setReplication is correct */
  private void testSetTimes(UserGroupInformation ugi, Path path,
      short ancestorPermission, short parentPermission, short filePermission)
      throws Exception {
    timesVerifier.set(path, ancestorPermission, parentPermission,
        filePermission);
    timesVerifier.verifyPermission(ugi);
  }

  /* A class that verifies the permission checking is correct for isDirectory,
   * exist,  getFileInfo, getContentSummary */
  private class StatsPermissionVerifier extends PermissionVerifier {
    OpType opType;

    /* initialize */
    void set(Path path, OpType opType, short ancestorPermission,
        short parentPermission) {
      super.set(path, ancestorPermission, parentPermission, NULL_MASK);
      setOpType(opType);
    }

    /* set if operation is getFileInfo, isDirectory, exist, getContenSummary */
    void setOpType(OpType opType) {
      this.opType = opType;
    }

    @Override
    void setOpPermission() {
      this.opParentPermission = SEARCH_MASK;
    }

    @Override
    void call() throws IOException {
      switch (opType) {
      case GET_FILEINFO:
        fs.getFileStatus(path);
        break;
      case IS_DIR:
        fs.isDirectory(path);
        break;
      case EXISTS:
        fs.exists(path);
        break;
      case GET_CONTENT_LENGTH:
        fs.getContentSummary(path).getLength();
        break;
      default:
        throw new IllegalArgumentException("Unexpected operation type: "
            + opType);
      }
    }
  }

  private final StatsPermissionVerifier statsVerifier = new StatsPermissionVerifier();
  /* test if the permission checking of isDirectory, exist,
   * getFileInfo, getContentSummary is correct */
  private void testStats(UserGroupInformation ugi, Path path,
      short ancestorPermission, short parentPermission) throws Exception {
    statsVerifier.set(path, OpType.GET_FILEINFO, ancestorPermission,
        parentPermission);
    statsVerifier.verifyPermission(ugi);
    statsVerifier.setOpType(OpType.IS_DIR);
    statsVerifier.verifyPermission(ugi);
    statsVerifier.setOpType(OpType.EXISTS);
    statsVerifier.verifyPermission(ugi);
    statsVerifier.setOpType(OpType.GET_CONTENT_LENGTH);
    statsVerifier.verifyPermission(ugi);
  }

  private enum InodeType {
    FILE, DIR
  };

  /* A class that verifies the permission checking is correct for list */
  private class ListPermissionVerifier extends PermissionVerifier {
    private InodeType inodeType;

    /* initialize */
    void set(Path path, InodeType inodeType, short ancestorPermission,
        short parentPermission, short permission) {
      this.inodeType = inodeType;
      super.set(path, ancestorPermission, parentPermission, permission);
    }

    /* set if the given path is a file/directory */
    void setInodeType(Path path, InodeType inodeType) {
      this.path = path;
      this.inodeType = inodeType;
      setOpPermission();
      this.ugi = null;
    }

    @Override
    void setOpPermission() {
      this.opParentPermission = SEARCH_MASK;
      switch (inodeType) {
      case FILE:
        this.opPermission = 0;
        break;
      case DIR:
        this.opPermission = READ_MASK | SEARCH_MASK;
        break;
      default:
        throw new IllegalArgumentException("Illegal inode type: " + inodeType);
      }
    }

    @Override
    void call() throws IOException {
      fs.listStatus(path);
    }
  }

  final ListPermissionVerifier listVerifier = new ListPermissionVerifier();
  /* test if the permission checking of list is correct */
  private void testList(UserGroupInformation ugi, Path file, Path dir,
      short ancestorPermission, short parentPermission, short filePermission)
      throws Exception {
    listVerifier.set(file, InodeType.FILE, ancestorPermission,
        parentPermission, filePermission);
    listVerifier.verifyPermission(ugi);
    listVerifier.setInodeType(dir, InodeType.DIR);
    listVerifier.verifyPermission(ugi);
  }

  /* A class that verifies the permission checking is correct for rename */
  private class RenamePermissionVerifier extends PermissionVerifier {
    private Path dst;
    private short dstAncestorPermission;
    private short dstParentPermission;

    /* initialize */
    void set(Path src, short srcAncestorPermission, short srcParentPermission,
        Path dst, short dstAncestorPermission, short dstParentPermission) {
      super.set(src, srcAncestorPermission, srcParentPermission, NULL_MASK);
      this.dst = dst;
      this.dstAncestorPermission = dstAncestorPermission;
      this.dstParentPermission = dstParentPermission;
    }

    @Override
    void setOpPermission() {
      opParentPermission = SEARCH_MASK | WRITE_MASK;
    }

    @Override
    void call() throws IOException {
      fs.rename(path, dst);
    }

    @Override
    protected boolean expectPermissionDeny() {
      return super.expectPermissionDeny()
          || (requiredParentPermission & dstParentPermission) != 
                requiredParentPermission
          || (requiredAncestorPermission & dstAncestorPermission) != 
                requiredAncestorPermission;
    }

    @Override
    protected void logPermissions() {
      super.logPermissions();
      LOG.info("dst ancestor permission: "
          + Integer.toOctalString(dstAncestorPermission));
      LOG.info("dst parent permission: "
          + Integer.toOctalString(dstParentPermission));
    }
  }

  final RenamePermissionVerifier renameVerifier = new RenamePermissionVerifier();
  /* test if the permission checking of rename is correct */
  private void testRename(UserGroupInformation ugi, Path src, Path dst,
      short srcAncestorPermission, short srcParentPermission,
      short dstAncestorPermission, short dstParentPermission) throws Exception {
    renameVerifier.set(src, srcAncestorPermission, srcParentPermission, dst,
        dstAncestorPermission, dstParentPermission);
    renameVerifier.verifyPermission(ugi);
  }

  /* A class that verifies the permission checking is correct for delete */
  private class DeletePermissionVerifier extends PermissionVerifier {
    void set(Path path, short ancestorPermission, short parentPermission) {
      super.set(path, ancestorPermission, parentPermission, NULL_MASK);
    }

    @Override
    void setOpPermission() {
      this.opParentPermission = SEARCH_MASK | WRITE_MASK;
    }

    @Override
    void call() throws IOException {
      fs.delete(path, true);
    }
  }

  /* A class that verifies the permission checking is correct for
   * directory deletion
   */
  private class DeleteDirPermissionVerifier extends DeletePermissionVerifier {
    private short[] childPermissions;

    /* initialize */
    void set(Path path, short ancestorPermission, short parentPermission,
        short permission, short[] childPermissions) {
      set(path, ancestorPermission, parentPermission, permission);
      this.childPermissions = childPermissions;
    }

    @Override
    void setOpPermission() {
      this.opParentPermission = SEARCH_MASK | WRITE_MASK;
      this.opPermission = SEARCH_MASK | WRITE_MASK | READ_MASK;
    }

    @Override
    protected boolean expectPermissionDeny() {
      if (super.expectPermissionDeny()) {
        return true;
      } else {
        if (childPermissions != null) {
          for (short childPermission : childPermissions) {
            if ((requiredPermission & childPermission) != requiredPermission) {
              return true;
            }
          }
        }
        return false;
      }
    }
  }

  /* A class that verifies the permission checking is correct for
   * empty-directory deletion
   */
  private class DeleteEmptyDirPermissionVerifier extends DeleteDirPermissionVerifier {
    @Override
    void setOpPermission() {
      this.opParentPermission = SEARCH_MASK | WRITE_MASK;
      this.opPermission = NULL_MASK;
    }
  }

  final DeletePermissionVerifier fileDeletionVerifier =
    new DeletePermissionVerifier();

  /* test if the permission checking of file deletion is correct */
  private void testDeleteFile(UserGroupInformation ugi, Path file,
      short ancestorPermission, short parentPermission) throws Exception {
    fileDeletionVerifier.set(file, ancestorPermission, parentPermission);
    fileDeletionVerifier.verifyPermission(ugi);
  }

  final DeleteDirPermissionVerifier dirDeletionVerifier =
    new DeleteDirPermissionVerifier();

  final DeleteEmptyDirPermissionVerifier emptyDirDeletionVerifier =
      new DeleteEmptyDirPermissionVerifier();

  /* test if the permission checking of directory deletion is correct */
  private void testDeleteDir(UserGroupInformation ugi, Path path,
      short ancestorPermission, short parentPermission, short permission,
      short[] childPermissions,
      final boolean isDirEmpty) throws Exception {
    DeleteDirPermissionVerifier ddpv = isDirEmpty?
        emptyDirDeletionVerifier : dirDeletionVerifier;
    ddpv.set(path, ancestorPermission, parentPermission, permission,
        childPermissions);
    ddpv.verifyPermission(ugi);
  }

  /* test non-existent file */
  private void checkNonExistentFile() {
    try {
      assertFalse(fs.exists(NON_EXISTENT_FILE));
    } catch (IOException e) {
      checkNoPermissionDeny(e);
    }
    try {
      fs.open(NON_EXISTENT_FILE);
    } catch (IOException e) {
      checkNoPermissionDeny(e);
    }
    try {
      fs.setReplication(NON_EXISTENT_FILE, (short)4);
    } catch (IOException e) {
      checkNoPermissionDeny(e);
    }
    try {
      fs.getFileStatus(NON_EXISTENT_FILE);
    } catch (IOException e) {
      checkNoPermissionDeny(e);
    }
    try {      
      fs.getContentSummary(NON_EXISTENT_FILE).getLength();
    } catch (IOException e) {
      checkNoPermissionDeny(e);
    }
    try {
      fs.listStatus(NON_EXISTENT_FILE);
    } catch (IOException e) {
      checkNoPermissionDeny(e);
    }
    try {
      fs.delete(NON_EXISTENT_FILE, true);
    } catch (IOException e) {
      checkNoPermissionDeny(e);
    }
    try {
      fs.rename(NON_EXISTENT_FILE, new Path(NON_EXISTENT_FILE+".txt"));
    } catch (IOException e) {
      checkNoPermissionDeny(e);
    }
  }
  
  private void checkNoPermissionDeny(IOException e) {
    assertFalse(e instanceof AccessControlException);
  }
}
