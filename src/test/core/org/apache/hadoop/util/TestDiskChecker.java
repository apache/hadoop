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

import org.junit.Test;
import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

import static org.apache.hadoop.test.MockitoMaker.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

public class TestDiskChecker {
  final FsPermission defaultPerm = new FsPermission("755");
  final FsPermission invalidPerm = new FsPermission("000");

  @Test public void testMkdirs_dirExists() throws Throwable {
    _mkdirs(true, defaultPerm, defaultPerm);
  }

  @Test public void testMkdirs_noDir() throws Throwable {
    _mkdirs(false, defaultPerm, defaultPerm);
  }

  @Test public void testMkdirs_dirExists_badUmask() throws Throwable {
    _mkdirs(true, defaultPerm, invalidPerm);
  }

  @Test public void testMkdirs_noDir_badUmask() throws Throwable {
    _mkdirs(false, defaultPerm, invalidPerm);
  }

  private void _mkdirs(boolean exists, FsPermission before, FsPermission after)
      throws Throwable {
    File localDir = make(stub(File.class).returning(exists).from.exists());
    when(localDir.mkdir()).thenReturn(true);
    Path dir = mock(Path.class); // use default stubs
    LocalFileSystem fs = make(stub(LocalFileSystem.class)
        .returning(localDir).from.pathToFile(dir));
    FileStatus stat = make(stub(FileStatus.class)
        .returning(after).from.getPermission());
    when(fs.getFileStatus(dir)).thenReturn(stat);

    try {
      DiskChecker.mkdirsWithExistsAndPermissionCheck(fs, dir, before);

      if (!exists)
        verify(fs).setPermission(dir, before);
      else {
        verify(fs).getFileStatus(dir);
        verify(stat).getPermission();
      }
    }
    catch (DiskErrorException e) {
      if (before != after)
        assertTrue(e.getMessage().startsWith("Incorrect permission"));
    }
  }

  @Test public void testCheckDir_normal() throws Throwable {
    _checkDirs(true, new FsPermission("755"), true);
  }

  @Test public void testCheckDir_notDir() throws Throwable {
    _checkDirs(false, new FsPermission("000"), false);
  }

  @Test public void testCheckDir_notReadable() throws Throwable {
    _checkDirs(true, new FsPermission("000"), false);
  }

  @Test public void testCheckDir_notWritable() throws Throwable {
    _checkDirs(true, new FsPermission("444"), false);
  }

  @Test public void testCheckDir_notListable() throws Throwable {
    _checkDirs(true, new FsPermission("666"), false);   // not listable
  }

  private void _checkDirs(boolean isDir, FsPermission perm, boolean success)
      throws Throwable {
    File localDir = make(stub(File.class).returning(true).from.exists());
    when(localDir.mkdir()).thenReturn(true);
    Path dir = mock(Path.class);
    LocalFileSystem fs = make(stub(LocalFileSystem.class)
        .returning(localDir).from.pathToFile(dir));
    FileStatus stat = make(stub(FileStatus.class)
        .returning(perm).from.getPermission());
    when(stat.isDirectory()).thenReturn(isDir);
    when(fs.getFileStatus(dir)).thenReturn(stat);

    try {
      DiskChecker.checkDir(fs, dir, perm);

      verify(stat).isDirectory();
      verify(fs, times(2)).getFileStatus(dir);
      verify(stat, times(2)).getPermission();
      assertTrue("checkDir success", success);
    }
    catch (DiskErrorException e) {
      assertFalse("checkDir success", success);
      e.printStackTrace();
    }
    System.out.println("checkDir success: "+ success);
  }
}
