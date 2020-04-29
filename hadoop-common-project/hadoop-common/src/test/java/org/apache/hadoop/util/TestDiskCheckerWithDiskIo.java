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

package org.apache.hadoop.util;

import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.FileIoProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;


/**
 * Verify {@link DiskChecker} validation routines that perform
 * Disk IO.
 */
public final class TestDiskCheckerWithDiskIo {
  @Rule
  public Timeout testTimeout = new Timeout(30_000);
  
  /**
   * Verify DiskChecker ignores at least 2 transient file creation errors.
   */
  @Test
  public final void testDiskIoIgnoresTransientCreateErrors() throws Throwable {
    DiskChecker.replaceFileOutputStreamProvider(new TestFileIoProvider(
        DiskChecker.DISK_IO_MAX_ITERATIONS - 1, 0));
    checkDirs(true);
  }

  /**
   * Verify DiskChecker bails after 3 file creation errors.
   */
  @Test(expected = DiskErrorException.class)
  public final void testDiskIoDetectsCreateErrors() throws Throwable {
    DiskChecker.replaceFileOutputStreamProvider(new TestFileIoProvider(
        DiskChecker.DISK_IO_MAX_ITERATIONS, 0));
    checkDirs(false);
  }

  /**
   * Verify DiskChecker ignores at least 2 transient file write errors.
   */
  @Test
  public final void testDiskIoIgnoresTransientWriteErrors() throws Throwable {
    DiskChecker.replaceFileOutputStreamProvider(new TestFileIoProvider(
        0, DiskChecker.DISK_IO_MAX_ITERATIONS - 1));
    checkDirs(true);
  }

  /**
   * Verify DiskChecker bails after 3 file write errors.
   */
  @Test(expected = DiskErrorException.class)
  public final void testDiskIoDetectsWriteErrors() throws Throwable {
    DiskChecker.replaceFileOutputStreamProvider(new TestFileIoProvider(
        0, DiskChecker.DISK_IO_MAX_ITERATIONS));
    checkDirs(false);
  }

  /**
   * Verify DiskChecker's test file naming scheme.
   */
  @Test
  public void testDiskIoFileNaming() {
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

  private void checkDirs(boolean success)
      throws Throwable {
    File localDir = createTempDir();
    try {
      DiskChecker.checkDirWithDiskIo(localDir);
    } finally {
      localDir.delete();
    }
  }

  /**
   * Create an empty directory with a random name under test directory
   * with Posix permissions "0755".
   *
   * @return the created directory
   * @throws java.io.IOException if any
   */
  private File createTempDir() throws java.io.IOException {
    final File testDir = new File(System.getProperty("test.build.data"));
    return Files.createTempDirectory(testDir.toPath(), "test",
        PosixFilePermissions.asFileAttribute(
            PosixFilePermissions.fromString("rwxr-xr-x"))).toFile();
  }  
}
