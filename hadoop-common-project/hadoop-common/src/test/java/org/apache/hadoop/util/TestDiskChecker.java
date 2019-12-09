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
package org.apache.hadoop.util;

import java.io.*;
import java.nio.file.Files;

import org.apache.hadoop.util.DiskChecker.FileIoProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDiskChecker {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestDiskChecker.class);

  private final FsPermission defaultPerm = new FsPermission("755");
  private final FsPermission invalidPerm = new FsPermission("000");

  private FileIoProvider fileIoProvider = null;

  @Before
  public void setup() {
    // Some tests replace the static field DiskChecker#fileIoProvider.
    // Cache it so we can restore it after each test completes.
    fileIoProvider = DiskChecker.getFileOutputStreamProvider();
  }

  @After
  public void cleanup() {
    DiskChecker.replaceFileOutputStreamProvider(fileIoProvider);
  }

  @Test(timeout = 30000)
  public void testMkdirs_dirExists() throws Throwable {
    _mkdirs(true, defaultPerm, defaultPerm);
  }

  @Test(timeout = 30000)
  public void testMkdirs_noDir() throws Throwable {
    _mkdirs(false, defaultPerm, defaultPerm);
  }

  @Test(timeout = 30000)
  public void testMkdirs_dirExists_badUmask() throws Throwable {
    _mkdirs(true, defaultPerm, invalidPerm);
  }

  @Test(timeout = 30000)
  public void testMkdirs_noDir_badUmask() throws Throwable {
    _mkdirs(false, defaultPerm, invalidPerm);
  }

  private void _mkdirs(boolean exists, FsPermission before, FsPermission after)
      throws Throwable {
    File localDir = mock(File.class);
    when(localDir.exists()).thenReturn(exists);
    when(localDir.mkdir()).thenReturn(true);
    Path dir = mock(Path.class); // use default stubs
    LocalFileSystem fs = mock(LocalFileSystem.class);
    when(fs.pathToFile(dir)).thenReturn(localDir);
    FileStatus stat = mock(FileStatus.class);
    when(stat.getPermission()).thenReturn(after);
    when(fs.getFileStatus(dir)).thenReturn(stat);

    try {
      DiskChecker.mkdirsWithExistsAndPermissionCheck(fs, dir, before);

      if (!exists)
        verify(fs).setPermission(dir, before);
      else {
        verify(fs).getFileStatus(dir);
        verify(stat).getPermission();
      }
    } catch (DiskErrorException e) {
      if (before != after)
        assertTrue(e.getMessage().startsWith("Incorrect permission"));
    }
  }

  @Test(timeout = 30000)
  public void testCheckDir_normal() throws Throwable {
    _checkDirs(true, new FsPermission("755"), true);
  }

  @Test(timeout = 30000)
  public void testCheckDir_notDir() throws Throwable {
    _checkDirs(false, new FsPermission("000"), false);
  }

  @Test(timeout = 30000)
  public void testCheckDir_notReadable() throws Throwable {
    _checkDirs(true, new FsPermission("000"), false);
  }

  @Test(timeout = 30000)
  public void testCheckDir_notWritable() throws Throwable {
    _checkDirs(true, new FsPermission("444"), false);
  }

  @Test(timeout = 30000)
  public void testCheckDir_notListable() throws Throwable {
    _checkDirs(true, new FsPermission("666"), false);   // not listable
  }

  /**
   * Create an empty file with a random name under test directory.
   * @return the created file
   * @throws java.io.IOException if any
   */
  protected File createTempFile() throws java.io.IOException {
    File testDir =
        new File(System.getProperty("test.build.data", "target/test-dir"));
    return Files.createTempFile(testDir.toPath(), "test", "tmp").toFile();
  }

  /**
   * Create an empty directory with a random name under test directory.
   * @return the created directory
   * @throws java.io.IOException if any
   */
  protected File createTempDir() throws java.io.IOException {
    File testDir =
        new File(System.getProperty("test.build.data", "target/test-dir"));
    return Files.createTempDirectory(testDir.toPath(), "test").toFile();
  }

  private void _checkDirs(boolean isDir, FsPermission perm, boolean success)
      throws Throwable {
    File localDir = isDir ? createTempDir() : createTempFile();
    Shell.execCommand(Shell.getSetPermissionCommand(String.format("%04o",
        perm.toShort()), false, localDir.getAbsolutePath()));
    try {
      DiskChecker.checkDir(FileSystem.getLocal(new Configuration()),
          new Path(localDir.getAbsolutePath()), perm);
      assertTrue("checkDir success, expected failure", success);
    } catch (DiskErrorException e) {
      if (success) {
        throw e; // Unexpected exception!
      }
    }
    localDir.delete();
  }

  /**
   * These test cases test to test the creation of a local folder with correct
   * permission for result of mapper.
   */

  @Test(timeout = 30000)
  public void testCheckDir_normal_local() throws Throwable {
    checkDirs(true, "755", true);
  }

  @Test(timeout = 30000)
  public void testCheckDir_notDir_local() throws Throwable {
    checkDirs(false, "000", false);
  }

  @Test(timeout = 30000)
  public void testCheckDir_notReadable_local() throws Throwable {
    checkDirs(true, "000", false);
  }

  @Test(timeout = 30000)
  public void testCheckDir_notWritable_local() throws Throwable {
    checkDirs(true, "444", false);
  }

  @Test(timeout = 30000)
  public void testCheckDir_notListable_local() throws Throwable {
    checkDirs(true, "666", false);
  }

  protected void checkDirs(boolean isDir, String perm, boolean success)
      throws Throwable {
    File localDir = isDir ? createTempDir() : createTempFile();
    Shell.execCommand(Shell.getSetPermissionCommand(perm, false,
        localDir.getAbsolutePath()));
    try {
      DiskChecker.checkDir(localDir);
      assertTrue("checkDir success, expected failure", success);
    } catch (DiskErrorException e) {
      if (success) {
        throw e; // Unexpected exception!
      }
    }
    localDir.delete();
  }
}
