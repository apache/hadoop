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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.util.DiskChecker.FileIoProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

import static org.apache.hadoop.test.MockitoMaker.*;
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
    File testDir = new File(System.getProperty("test.build.data"));
    return Files.createTempFile(testDir.toPath(), "test", "tmp").toFile();
  }

  /**
   * Create an empty directory with a random name under test directory.
   * @return the created directory
   * @throws java.io.IOException if any
   */
  protected File createTempDir() throws java.io.IOException {
    File testDir = new File(System.getProperty("test.build.data"));
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

  /**
   * Verify DiskChecker ignores at least 2 transient file creation errors.
   */
  @Test(timeout = 30000)
  public void testDiskIoIgnoresTransientCreateErrors() throws Throwable {
    DiskChecker.replaceFileOutputStreamProvider(new TestFileIoProvider(
        DiskChecker.DISK_IO_MAX_ITERATIONS - 1, 0));
    checkDirs(true, "755", true);
  }

  /**
   * Verify DiskChecker bails after 3 file creation errors.
   */
  @Test(timeout = 30000)
  public void testDiskIoDetectsCreateErrors() throws Throwable {
    DiskChecker.replaceFileOutputStreamProvider(new TestFileIoProvider(
        DiskChecker.DISK_IO_MAX_ITERATIONS, 0));
    checkDirs(true, "755", false);
  }

  /**
   * Verify DiskChecker ignores at least 2 transient file write errors.
   */
  @Test(timeout = 30000)
  public void testDiskIoIgnoresTransientWriteErrors() throws Throwable {
    DiskChecker.replaceFileOutputStreamProvider(new TestFileIoProvider(
        0, DiskChecker.DISK_IO_MAX_ITERATIONS - 1));
    checkDirs(true, "755", true);
  }

  /**
   * Verify DiskChecker bails after 3 file write errors.
   */
  @Test(timeout = 30000)
  public void testDiskIoDetectsWriteErrors() throws Throwable {
    DiskChecker.replaceFileOutputStreamProvider(new TestFileIoProvider(
        0, DiskChecker.DISK_IO_MAX_ITERATIONS));
    checkDirs(true, "755", false);
  }

  /**
   * Verify DiskChecker's test file naming scheme.
   */
  @Test(timeout = 30000)
  public void testDiskIoFileNaming() throws Throwable {
    final File rootDir = new File("/");
    assertTrue(".001".matches("\\.00\\d$"));
    for (int i = 1; i < DiskChecker.DISK_IO_MAX_ITERATIONS; ++i) {
      final File file = DiskChecker.getFileNameForDiskIoCheck(rootDir, i);
      assertTrue(
          "File name does not match expected pattern: " + file,
          file.toString().matches("^.*\\.[0-9]+$"));
    }
    final File guidFile = DiskChecker.getFileNameForDiskIoCheck(
        rootDir, DiskChecker.DISK_IO_MAX_ITERATIONS);
    assertTrue(
        "File name does not match expected pattern: " + guidFile,
        guidFile.toString().matches("^.*\\.[A-Za-z0-9-]+$"));
  }

  /**
   * A dummy {@link DiskChecker#FileIoProvider} that can throw a programmable
   * number of times.
   */
  private static class TestFileIoProvider implements FileIoProvider {
    private final AtomicInteger numCreateCalls = new AtomicInteger(0);
    private final AtomicInteger numWriteCalls = new AtomicInteger(0);

    private final int numTimesToThrowOnCreate;
    private final int numTimesToThrowOnWrite;

    public TestFileIoProvider(
        int numTimesToThrowOnCreate, int numTimesToThrowOnWrite) {
      this.numTimesToThrowOnCreate = numTimesToThrowOnCreate;
      this.numTimesToThrowOnWrite = numTimesToThrowOnWrite;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileOutputStream get(File f) throws FileNotFoundException {
      if (numCreateCalls.getAndIncrement() < numTimesToThrowOnCreate) {
        throw new FileNotFoundException("Dummy exception for testing");
      }
      // Can't mock final class FileOutputStream.
      return new FileOutputStream(f);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(FileOutputStream fos, byte[] data) throws IOException {
      if (numWriteCalls.getAndIncrement() < numTimesToThrowOnWrite) {
        throw new IOException("Dummy exception for testing");
      }
      fos.write(data);
    }
  }
}
