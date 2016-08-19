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
package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.hadoop.util.Shell;

import java.io.*;
import java.util.*;

import org.junit.Test;
import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

/**
 * This class tests the local file system via the FileSystem abstraction.
 */
public class TestLocalFileSystemPermission {

  public static final Logger LOGGER =
      LoggerFactory.getLogger(TestFcLocalFsPermission.class);

  static final String TEST_PATH_PREFIX = GenericTestUtils.getTempPath(
      TestLocalFileSystemPermission.class.getSimpleName());

  static {
    GenericTestUtils.setLogLevel(FileSystem.LOG, Level.DEBUG);
  }

  private Path writeFile(FileSystem fs, String name) throws IOException {
    Path f = new Path(TEST_PATH_PREFIX + name);
    FSDataOutputStream stm = fs.create(f);
    stm.writeBytes("42\n");
    stm.close();
    return f;
  }

  private Path writeFile(FileSystem fs, String name, FsPermission perm) throws IOException {
    Path f = new Path(TEST_PATH_PREFIX + name);
    FSDataOutputStream stm = fs.create(f, perm, true, 2048, (short)1, 32 * 1024 * 1024, null);
    stm.writeBytes("42\n");
    stm.close();
    return f;
  }

  private void cleanup(FileSystem fs, Path name) throws IOException {
    assertTrue(fs.exists(name));
    fs.delete(name, true);
    assertTrue(!fs.exists(name));
  }

  @Test
  public void testLocalFSDirsetPermission() throws IOException {
    assumeNotWindows();
    LocalFileSystem localfs = FileSystem.getLocal(new Configuration());
    Configuration conf = localfs.getConf();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "044");
    Path dir = new Path(TEST_PATH_PREFIX + "dir");
    localfs.mkdirs(dir);
    try {
      FsPermission initialPermission = getPermission(localfs, dir);
      assertEquals(
          FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(conf)),
          initialPermission);
    } catch(Exception e) {
      LOGGER.error("Cannot run test", e);
      return;
    }

    FsPermission perm = new FsPermission((short)0755);
    Path dir1 = new Path(TEST_PATH_PREFIX + "dir1");
    localfs.mkdirs(dir1, perm);
    try {
      FsPermission initialPermission = getPermission(localfs, dir1);
      assertEquals(perm.applyUMask(FsPermission.getUMask(conf)), initialPermission);
    } catch(Exception e) {
      LOGGER.error("Cannot run test", e);
      return;
    }

    Path dir2 = new Path(TEST_PATH_PREFIX + "dir2");
    localfs.mkdirs(dir2);
    try {
      FsPermission initialPermission = getPermission(localfs, dir2);
      Path copyPath = new Path(TEST_PATH_PREFIX + "dir_copy");
      localfs.rename(dir2, copyPath);
      FsPermission copyPermission = getPermission(localfs, copyPath);
      assertEquals(copyPermission, initialPermission);
      dir2 = copyPath;
    } catch (Exception e) {
      LOGGER.error("Cannot run test", e);
      return;
    } finally {
      cleanup(localfs, dir);
      cleanup(localfs, dir1);
      if (localfs.exists(dir2)) {
        localfs.delete(dir2, true);
      }
    }
  }

  /** Test LocalFileSystem.setPermission */
  @Test
  public void testLocalFSsetPermission() throws IOException {
    assumeNotWindows();
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "044");
    LocalFileSystem localfs = FileSystem.getLocal(conf);
    String filename = "foo";
    Path f = writeFile(localfs, filename);
    try {
      FsPermission initialPermission = getPermission(localfs, f);
      assertEquals(
          FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(conf)),
          initialPermission);
    } catch(Exception e) {
      LOGGER.error("Cannot run test", e);
      return;
    }

    String filename1 = "foo1";
    FsPermission perm = new FsPermission((short)0755);
    Path f1 = writeFile(localfs, filename1, perm);
    try {
      FsPermission initialPermission = getPermission(localfs, f1);
      assertEquals(
          perm.applyUMask(FsPermission.getUMask(conf)), initialPermission);
    } catch(Exception e) {
      LOGGER.error("Cannot run test", e);
      return;
    }

    String filename2 = "foo2";
    Path f2 = writeFile(localfs, filename2);
    try {
      FsPermission initialPermission = getPermission(localfs, f2);
      Path copyPath = new Path(TEST_PATH_PREFIX + "/foo_copy");
      localfs.rename(f2, copyPath);
      FsPermission copyPermission = getPermission(localfs, copyPath);
      assertEquals(copyPermission, initialPermission);
      f2 = copyPath;
    } catch (Exception e) {
      LOGGER.error("Cannot run test", e);
      return;
    }

    try {
      // create files and manipulate them.
      FsPermission all = new FsPermission((short)0777);
      FsPermission none = new FsPermission((short)0);

      localfs.setPermission(f, none);
      assertEquals(none, getPermission(localfs, f));

      localfs.setPermission(f, all);
      assertEquals(all, getPermission(localfs, f));
    }
    finally {
      cleanup(localfs, f);
      cleanup(localfs, f1);
      if (localfs.exists(f2)) {
        localfs.delete(f2, true);
      }
    }
  }

  FsPermission getPermission(LocalFileSystem fs, Path p) throws IOException {
    return fs.getFileStatus(p).getPermission();
  }

  /** Test LocalFileSystem.setOwner. */
  @Test
  public void testLocalFSsetOwner() throws IOException {
    if (Path.WINDOWS) {
      LOGGER.info("Cannot run test for Windows");
      return;
    }

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "044");
    LocalFileSystem localfs = FileSystem.getLocal(conf);
    String filename = "bar";
    Path f = writeFile(localfs, filename);
    List<String> groups = null;
    try {
      groups = getGroups();
      LOGGER.info("{}: {}", filename, getPermission(localfs, f));
    }
    catch(IOException e) {
      LOGGER.error("Cannot run test", e);
      return;
    }
    if (groups == null || groups.size() < 1) {
      LOGGER.error("Cannot run test: need at least one group. groups={}",
          groups);
      return;
    }

    // create files and manipulate them.
    try {
      String g0 = groups.get(0);
      localfs.setOwner(f, null, g0);
      assertEquals(g0, getGroup(localfs, f));

      if (groups.size() > 1) {
        String g1 = groups.get(1);
        localfs.setOwner(f, null, g1);
        assertEquals(g1, getGroup(localfs, f));
      } else {
        LOGGER.info("Not testing changing the group since user " +
            "belongs to only one group.");
      }
    } 
    finally {cleanup(localfs, f);}
  }

  /**
   * Steps:
   * 1. Create a directory with default permissions: 777 with umask 022
   * 2. Check the directory has good permissions: 755
   * 3. Set the umask to 062.
   * 4. Create a new directory with default permissions.
   * 5. For this directory we expect 715 as permission not 755
   * @throws Exception we can throw away all the exception.
   */
  @Test
  public void testSetUmaskInRealTime() throws Exception {
    if (Path.WINDOWS) {
      LOGGER.info("Cannot run test for Windows");
      return;
    }

    LocalFileSystem localfs = FileSystem.getLocal(new Configuration());
    Configuration conf = localfs.getConf();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "022");
    LOGGER.info("Current umask is {}",
        conf.get(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY));
    Path dir = new Path(TEST_PATH_PREFIX + "dir");
    Path dir2 = new Path(TEST_PATH_PREFIX + "dir2");
    try {
      assertTrue(localfs.mkdirs(dir));
      FsPermission initialPermission = getPermission(localfs, dir);
      assertEquals(
          "With umask 022 permission should be 755 since the default " +
              "permission is 777", new FsPermission("755"), initialPermission);

      // Modify umask and create a new directory
      // and check if new umask is applied
      conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "062");
      assertTrue(localfs.mkdirs(dir2));
      FsPermission finalPermission = localfs.getFileStatus(dir2)
          .getPermission();
      assertThat("With umask 062 permission should not be 755 since the " +
          "default permission is 777", new FsPermission("755"),
          is(not(finalPermission)));
      assertEquals(
          "With umask 062 we expect 715 since the default permission is 777",
          new FsPermission("715"), finalPermission);
    } finally {
      conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "022");
      cleanup(localfs, dir);
      cleanup(localfs, dir2);
    }
  }

  static List<String> getGroups() throws IOException {
    List<String> a = new ArrayList<String>();
    String s = Shell.execCommand(Shell.getGroupsCommand());
    for(StringTokenizer t = new StringTokenizer(s); t.hasMoreTokens(); ) {
      a.add(t.nextToken());
    }
    return a;
  }

  String getGroup(LocalFileSystem fs, Path p) throws IOException {
    return fs.getFileStatus(p).getGroup();
  }
}
