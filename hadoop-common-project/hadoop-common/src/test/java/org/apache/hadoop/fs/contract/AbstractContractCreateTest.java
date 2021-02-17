/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;

import org.junit.Test;
import org.junit.AssumptionViolatedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertCapabilities;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.getFileStatusEventually;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsSourceToString;

/**
 * Test creating files, overwrite options etc.
 */
public abstract class AbstractContractCreateTest extends
    AbstractFSContractTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractCreateTest.class);

  /**
   * How long to wait for a path to become visible.
   */
  public static final int CREATE_TIMEOUT = 15000;

  protected Path path(String filepath, boolean useBuilder) throws IOException {
    return super.path(filepath  + (useBuilder ? "" : "-builder"));
  }

  private void testCreateNewFile(boolean useBuilder) throws Throwable {
    describe("Foundational 'create a file' test, using builder API=" +
        useBuilder);
    Path path = path("testCreateNewFile", useBuilder);
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024 * 1024, false,
        useBuilder);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data);
  }

  @Test
  public void testCreateNewFile() throws Throwable {
    testCreateNewFile(true);
    testCreateNewFile(false);
  }

  private void testCreateFileOverExistingFileNoOverwrite(boolean useBuilder)
      throws Throwable {
    describe("Verify overwriting an existing file fails, using builder API=" +
        useBuilder);
    Path path = path("testCreateFileOverExistingFileNoOverwrite", useBuilder);
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024, false);
    byte[] data2 = dataset(10 * 1024, 'A', 'Z');
    try {
      writeDataset(getFileSystem(), path, data2, data2.length, 1024, false,
          useBuilder);
      fail("writing without overwrite unexpectedly succeeded");
    } catch (FileAlreadyExistsException expected) {
      //expected
      handleExpectedException(expected);
    } catch (IOException relaxed) {
      handleRelaxedException("Creating a file over a file with overwrite==false",
                             "FileAlreadyExistsException",
                             relaxed);
    }
  }

  @Test
  public void testCreateFileOverExistingFileNoOverwrite() throws Throwable {
    testCreateFileOverExistingFileNoOverwrite(false);
    testCreateFileOverExistingFileNoOverwrite(true);
  }

  private void testOverwriteExistingFile(boolean useBuilder) throws Throwable {
    describe("Overwrite an existing file and verify the new data is there, " +
        "use builder API=" + useBuilder);
    Path path = path("testOverwriteExistingFile", useBuilder);
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024, false,
        useBuilder);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data);
    byte[] data2 = dataset(10 * 1024, 'A', 'Z');
    writeDataset(getFileSystem(), path, data2, data2.length, 1024, true,
        useBuilder);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data2);
  }

  /**
   * This test catches some eventual consistency problems that blobstores exhibit,
   * as we are implicitly verifying that updates are consistent. This
   * is why different file lengths and datasets are used
   */
  @Test
  public void testOverwriteExistingFile() throws Throwable {
    testOverwriteExistingFile(false);
    testOverwriteExistingFile(true);
  }

  private void testOverwriteEmptyDirectory(boolean useBuilder)
      throws Throwable {
    describe("verify trying to create a file over an empty dir fails, " +
        "use builder API=" + useBuilder);
    Path path = path("testOverwriteEmptyDirectory");
    mkdirs(path);
    assertIsDirectory(path);
    byte[] data = dataset(256, 'a', 'z');
    try {
      writeDataset(getFileSystem(), path, data, data.length, 1024, true,
          useBuilder);
      assertIsDirectory(path);
      fail("write of file over empty dir succeeded");
    } catch (FileAlreadyExistsException expected) {
      //expected
      handleExpectedException(expected);
    } catch (IOException e) {
      handleRelaxedException("overwriting a dir with a file ",
                             "FileAlreadyExistsException",
                             e);
    }
    assertIsDirectory(path);
  }

  @Test
  public void testOverwriteEmptyDirectory() throws Throwable {
    testOverwriteEmptyDirectory(false);
    testOverwriteEmptyDirectory(true);
  }

  private void testOverwriteNonEmptyDirectory(boolean useBuilder)
      throws Throwable {
    describe("verify trying to create a file over a non-empty dir fails, " +
        "use builder API=" + useBuilder);
    Path path = path("testOverwriteNonEmptyDirectory");
    mkdirs(path);
    try {
      assertIsDirectory(path);
    } catch (AssertionError failure) {
      if (isSupported(CREATE_OVERWRITES_DIRECTORY)) {
        // file/directory hack surfaces here
        throw new AssumptionViolatedException(failure.toString(), failure);
      }
      // else: rethrow
      throw failure;
    }
    Path child = new Path(path, "child");
    writeTextFile(getFileSystem(), child, "child file", true);
    byte[] data = dataset(256, 'a', 'z');
    try {
      writeDataset(getFileSystem(), path, data, data.length, 1024,
                   true, useBuilder);
      FileStatus status = getFileSystem().getFileStatus(path);

      boolean isDir = status.isDirectory();
      if (!isDir && isSupported(CREATE_OVERWRITES_DIRECTORY)) {
        // For some file systems, downgrade to a skip so that the failure is
        // visible in test results.
        skip("This Filesystem allows a file to overwrite a directory");
      }
      fail("write of file over dir succeeded");
    } catch (FileAlreadyExistsException expected) {
      //expected
      handleExpectedException(expected);
    } catch (IOException e) {
      handleRelaxedException("overwriting a dir with a file ",
                             "FileAlreadyExistsException",
                             e);
    }
    assertIsDirectory(path);
    assertIsFile(child);
  }

  @Test
  public void testOverwriteNonEmptyDirectory() throws Throwable {
    testOverwriteNonEmptyDirectory(false);
    testOverwriteNonEmptyDirectory(true);
  }

  @Test
  public void testCreatedFileIsImmediatelyVisible() throws Throwable {
    describe("verify that a newly created file exists as soon as open returns");
    Path path = path("testCreatedFileIsImmediatelyVisible");
    try(FSDataOutputStream out = getFileSystem().create(path,
                                   false,
                                   4096,
                                   (short) 1,
                                   1024)) {
      if (!getFileSystem().exists(path)) {

        if (isSupported(CREATE_VISIBILITY_DELAYED)) {
          // For some file systems, downgrade to a skip so that the failure is
          // visible in test results.
          skip("This Filesystem delays visibility of newly created files");
        }
        assertPathExists("expected path to be visible before anything written",
                         path);
      }
    }
  }

  @Test
  public void testCreatedFileIsVisibleOnFlush() throws Throwable {
    describe("verify that a newly created file exists once a flush has taken "
        + "place");
    Path path = path("testCreatedFileIsVisibleOnFlush");
    FileSystem fs = getFileSystem();
    try(FSDataOutputStream out = fs.create(path,
          false,
          4096,
          (short) 1,
          1024)) {
      out.write('a');
      out.flush();
      if (!fs.exists(path)) {
        if (isSupported(IS_BLOBSTORE) ||
            isSupported(CREATE_VISIBILITY_DELAYED)) {
          // object store or some file systems: downgrade to a skip so that the
          // failure is visible in test results
          skip("For object store or some file systems, newly created files are"
              + " not immediately visible");
        }
        assertPathExists("expected path to be visible before file closed",
            path);
      }
    }
  }

  @Test
  public void testCreatedFileIsEventuallyVisible() throws Throwable {
    describe("verify a written to file is visible after the stream is closed");
    Path path = path("testCreatedFileIsEventuallyVisible");
    FileSystem fs = getFileSystem();
    try(
      FSDataOutputStream out = fs.create(path,
          false,
          4096,
          (short) 1,
          1024)
      ) {
      out.write(0x01);
      out.close();
      getFileStatusEventually(fs, path, CREATE_TIMEOUT);
    }
  }

  @Test
  public void testFileStatusBlocksizeNonEmptyFile() throws Throwable {
    describe("validate the block size of a filesystem and files within it");
    FileSystem fs = getFileSystem();

    long rootPath = fs.getDefaultBlockSize(path("/"));
    assertTrue("Root block size is invalid " + rootPath,
        rootPath > 0);

    Path path = path("testFileStatusBlocksizeNonEmptyFile");
    byte[] data = dataset(256, 'a', 'z');

    writeDataset(fs, path, data, data.length, 1024 * 1024, false);

    validateBlockSize(fs, path, 1);
  }

  @Test
  public void testFileStatusBlocksizeEmptyFile() throws Throwable {
    describe("check that an empty file may return a 0-byte blocksize");
    FileSystem fs = getFileSystem();
    Path path = path("testFileStatusBlocksizeEmptyFile");
    ContractTestUtils.touch(fs, path);
    validateBlockSize(fs, path, 0);
  }

  private void validateBlockSize(FileSystem fs, Path path, int minValue)
      throws IOException, InterruptedException {
    FileStatus status =
        getFileStatusEventually(fs, path, CREATE_TIMEOUT);
    String statusDetails = status.toString();
    assertTrue("File status block size too low:  " + statusDetails
            + " min value: " + minValue,
        status.getBlockSize() >= minValue);
    long defaultBlockSize = fs.getDefaultBlockSize(path);
    assertTrue("fs.getDefaultBlockSize(" + path + ") size " +
            defaultBlockSize + " is below the minimum of " + minValue,
        defaultBlockSize >= minValue);
  }

  @Test
  public void testCreateMakesParentDirs() throws Throwable {
    describe("check that after creating a file its parent directories exist");
    FileSystem fs = getFileSystem();
    Path grandparent = path("testCreateCreatesAndPopulatesParents");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    touch(fs, child);
    assertEquals("List status of parent should include the 1 child file",
        1, fs.listStatus(parent).length);
    assertTrue("Parent directory does not appear to be a directory",
        fs.getFileStatus(parent).isDirectory());
    assertEquals("List status of grandparent should include the 1 parent dir",
        1, fs.listStatus(grandparent).length);
    assertTrue("Grandparent directory does not appear to be a directory",
        fs.getFileStatus(grandparent).isDirectory());
  }

  @Test
  public void testCreateFileUnderFile() throws Throwable {
    describe("Verify that it is forbidden to create file/file");
    if (isSupported(CREATE_FILE_UNDER_FILE_ALLOWED)) {
      // object store or some file systems: downgrade to a skip so that the
      // failure is visible in test results
      skip("This filesystem supports creating files under files");
    }
    Path grandparent = methodPath();
    Path parent = new Path(grandparent, "parent");
    expectCreateUnderFileFails(
        "creating a file under a file",
        grandparent,
        parent);
  }

  @Test
  public void testCreateUnderFileSubdir() throws Throwable {
    describe("Verify that it is forbidden to create file/dir/file");
    if (isSupported(CREATE_FILE_UNDER_FILE_ALLOWED)) {
      // object store or some file systems: downgrade to a skip so that the
      // failure is visible in test results
      skip("This filesystem supports creating files under files");
    }
    Path grandparent = methodPath();
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    expectCreateUnderFileFails(
        "creating a file under a subdirectory of a file",
        grandparent,
        child);
  }


  @Test
  public void testMkdirUnderFile() throws Throwable {
    describe("Verify that it is forbidden to create file/dir");
    Path grandparent = methodPath();
    Path parent = new Path(grandparent, "parent");
    expectMkdirsUnderFileFails("mkdirs() under a file",
        grandparent, parent);
  }

  @Test
  public void testMkdirUnderFileSubdir() throws Throwable {
    describe("Verify that it is forbidden to create file/dir/dir");
    Path grandparent = methodPath();
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    expectMkdirsUnderFileFails("mkdirs() file/dir",
        grandparent, child);

    try {
      // create the child
      mkdirs(child);
    } catch (FileAlreadyExistsException | ParentNotDirectoryException ex) {
      // either of these may be raised.
      handleExpectedException(ex);
    } catch (IOException e) {
      handleRelaxedException("creating a file under a subdirectory of a file ",
          "FileAlreadyExistsException",
          e);
    }
  }

  /**
   * Expect that touch() will fail because the parent is a file.
   * @param action action for message
   * @param file filename to create
   * @param descendant path under file
   * @throws Exception failure
   */
  protected void expectCreateUnderFileFails(String action,
      Path file, Path descendant)
      throws Exception {
    createFile(file);
    try {
      // create the child
      createFile(descendant);
    } catch (FileAlreadyExistsException | ParentNotDirectoryException ex) {
      //expected
      handleExpectedException(ex);
    } catch (IOException e) {
      handleRelaxedException(action,
          "ParentNotDirectoryException",
          e);
    }
  }

  protected void expectMkdirsUnderFileFails(String action,
      Path file, Path descendant)
      throws Exception {
    createFile(file);
    try {
      // now mkdirs
      mkdirs(descendant);
    } catch (FileAlreadyExistsException | ParentNotDirectoryException ex) {
      //expected
      handleExpectedException(ex);
    } catch (IOException e) {
      handleRelaxedException(action,
          "ParentNotDirectoryException",
          e);
    }
  }

  private void createFile(Path path) throws IOException {
    byte[] data = dataset(256, 'a', 'z');
    FileSystem fs = getFileSystem();
    writeDataset(fs, path, data, data.length, 1024 * 1024,
        true);
  }

  @Test
  public void testSyncable() throws Throwable {
    describe("test declared and actual Syncable behaviors");
    FileSystem fs = getFileSystem();
    boolean supportsFlush = isSupported(SUPPORTS_HFLUSH);
    boolean supportsSync = isSupported(SUPPORTS_HSYNC);
    boolean metadataUpdatedOnHSync = isSupported(METADATA_UPDATED_ON_HSYNC);

    validateSyncableSemantics(fs,
        supportsSync,
        supportsFlush,
        metadataUpdatedOnHSync);
  }

  /**
   * Validate the semantics of syncable.
   * @param fs filesystem
   * @param supportsSync sync is present
   * @param supportsFlush flush is present.
   * @param metadataUpdatedOnHSync  Is the metadata updated after an hsync?
   * @throws IOException failure
   */
  protected void validateSyncableSemantics(final FileSystem fs,
      final boolean supportsSync,
      final boolean supportsFlush,
      final boolean metadataUpdatedOnHSync)
      throws IOException {
    Path path = methodPath();
    LOG.info("Expecting files under {} to have supportsSync={}"
            + " and supportsFlush={}; metadataUpdatedOnHSync={}",
        path, supportsSync, supportsFlush, metadataUpdatedOnHSync);

    try (FSDataOutputStream out = fs.create(path, true)) {
      LOG.info("Created output stream {}", out);

      // probe stream for support for flush/sync, whose capabilities
      // of supports/does not support must match what is expected
      String[] hflushCapabilities = {
          StreamCapabilities.HFLUSH
      };
      String[] hsyncCapabilities = {
          StreamCapabilities.HSYNC
      };
      if (supportsFlush) {
        assertCapabilities(out, hflushCapabilities, null);
      } else {
        assertCapabilities(out, null, hflushCapabilities);
      }
      if (supportsSync) {
        assertCapabilities(out, hsyncCapabilities, null);
      } else {
        assertCapabilities(out, null, hsyncCapabilities);
      }

      // write one byte, then hflush it
      out.write('a');
      try {
        out.hflush();
        if (!supportsFlush) {
          // FSDataOutputStream silently downgrades to flush() here.
          // This is not good, but if changed some applications
          // break writing to some stores.
          LOG.warn("FS doesn't support Syncable.hflush(),"
              + " but doesn't reject it either.");
        }
      } catch (UnsupportedOperationException e) {
        if (supportsFlush) {
          throw new AssertionError("hflush not supported", e);
        }
      }

      // write a second byte, then hsync it.
      out.write('b');
      try {
        out.hsync();
      } catch (UnsupportedOperationException e) {
        if (supportsSync) {
          throw new AssertionError("HSync not supported", e);
        }
      }

      if (supportsSync) {
        // if sync really worked, data MUST be visible here

        // first the metadata which MUST be present
        final FileStatus st = fs.getFileStatus(path);
        if (metadataUpdatedOnHSync) {
          // not all stores reliably update it, HDFS/webHDFS in particular
          assertEquals("Metadata not updated during write " + st,
              2, st.getLen());
        }

        // there's no way to verify durability, but we can
        // at least verify a new file input stream reads
        // the data
        try (FSDataInputStream in = fs.open(path)) {
          assertEquals('a', in.read());
          assertEquals('b', in.read());
          assertEquals(-1, in.read());
          LOG.info("Successfully read synced data on a new reader {}", in);
        }
      } else {
        // no sync. Let's do a flush and see what happens.
        out.flush();
        // Now look at the filesystem.
        try (FSDataInputStream in = fs.open(path)) {
          int c = in.read();
          if (c == -1) {
            // nothing was synced; sync and flush really aren't there.
            LOG.info("sync and flush are declared unsupported"
                + " -flushed changes were not saved");

          } else {
            LOG.info("sync and flush are declared unsupported"
                + " - but the stream does offer some sync/flush semantics");
          }
          // close outside a finally as we do want to see any exception raised.
          in.close();

        } catch (FileNotFoundException e) {
          // that's OK if it's an object store, but not if its a real
          // FS
          if (!isSupported(IS_BLOBSTORE)) {
            throw e;
          } else {
            LOG.warn(
                "Output file was not created; this is an object store with different"
                    + " visibility semantics");
          }
        }
      }
      // close the output stream
      out.close();

      final String stats = ioStatisticsSourceToString(out);
      if (!stats.isEmpty()) {
        LOG.info("IOStatistics {}", stats);
      }
    }
  }
}
