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
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.apache.hadoop.util.Shell;

import java.io.*;
import java.util.*;

import junit.framework.*;

/**
 * This class tests the local file system via the FileSystem abstraction.
 */
public class TestLocalFileSystemPermission extends TestCase {
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

  public void testLocalFSDirsetPermission() throws IOException {
    if (Path.WINDOWS) {
      System.out.println("Cannot run test for Windows");
      return;
    }
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "044");
    LocalFileSystem localfs = FileSystem.getLocal(conf);
    Path dir = new Path(TEST_PATH_PREFIX + "dir");
    localfs.mkdirs(dir);
    try {
      FsPermission initialPermission = getPermission(localfs, dir);
      assertEquals(
          FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(conf)),
          initialPermission);
    } catch(Exception e) {
      System.out.println(StringUtils.stringifyException(e));
      System.out.println("Cannot run test");
      return;
    }

    FsPermission perm = new FsPermission((short)0755);
    Path dir1 = new Path(TEST_PATH_PREFIX + "dir1");
    localfs.mkdirs(dir1, perm);
    try {
      FsPermission initialPermission = getPermission(localfs, dir1);
      assertEquals(perm.applyUMask(FsPermission.getUMask(conf)), initialPermission);
    } catch(Exception e) {
      System.out.println(StringUtils.stringifyException(e));
      System.out.println("Cannot run test");
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
      System.out.println(StringUtils.stringifyException(e));
      System.out.println("Cannot run test");
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
  public void testLocalFSsetPermission() throws IOException {
    if (Path.WINDOWS) {
      System.out.println("Cannot run test for Windows");
      return;
    }
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
      System.out.println(StringUtils.stringifyException(e));
      System.out.println("Cannot run test");
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
      System.out.println(StringUtils.stringifyException(e));
      System.out.println("Cannot run test");
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
      System.out.println(StringUtils.stringifyException(e));
      System.out.println("Cannot run test");
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

  /** Test LocalFileSystem.setOwner */
  public void testLocalFSsetOwner() throws IOException {
    if (Path.WINDOWS) {
      System.out.println("Cannot run test for Windows");
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
      System.out.println(filename + ": " + getPermission(localfs, f));
    }
    catch(IOException e) {
      System.out.println(StringUtils.stringifyException(e));
      System.out.println("Cannot run test");
      return;
    }
    if (groups == null || groups.size() < 1) {
      System.out.println("Cannot run test: need at least one group.  groups="
                         + groups);
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
        System.out.println("Not testing changing the group since user " +
                           "belongs to only one group.");
      }
    } 
    finally {cleanup(localfs, f);}
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
