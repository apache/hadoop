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

package org.apache.hadoop.fs.azure;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.integration.AbstractAzureScaleTest;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.azure.integration.AzureTestUtils .*;

/**
 * Write data into a page blob and verify you can read back all of it
 * or just a part of it.
 */
public class ITestReadAndSeekPageBlobAfterWrite extends AbstractAzureScaleTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestReadAndSeekPageBlobAfterWrite.class);

  private FileSystem fs;
  private byte[] randomData;

  // Page blob physical page size
  private static final int PAGE_SIZE = PageBlobFormatHelpers.PAGE_SIZE;

  // Size of data on page (excluding header)
  private static final int PAGE_DATA_SIZE = PAGE_SIZE - PageBlobFormatHelpers.PAGE_HEADER_SIZE;
  private static final int MAX_BYTES = 33554432; // maximum bytes in a file that we'll test
  private static final int MAX_PAGES = MAX_BYTES / PAGE_SIZE; // maximum number of pages we'll test
  private Random rand = new Random();

  // A key with a prefix under /pageBlobs, which for the test file system will
  // force use of a page blob.
  private static final String KEY = "/pageBlobs/file.dat";

  // path of page blob file to read and write
  private Path blobPath;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    fs = getTestAccount().getFileSystem();
    // Make sure we are using an integral number of pages.
    assertEquals(0, MAX_BYTES % PAGE_SIZE);

    // load an in-memory array of random data
    randomData = new byte[PAGE_SIZE * MAX_PAGES];
    rand.nextBytes(randomData);

    blobPath = blobPath("ITestReadAndSeekPageBlobAfterWrite");
  }

  @Override
  public void tearDown() throws Exception {
    deleteQuietly(fs, blobPath, true);
    super.tearDown();
  }

  /**
   * Make sure the file name (key) is a page blob file name. If anybody changes that,
   * we need to come back and update this test class.
   */
  @Test
  public void testIsPageBlobFileName() {
    AzureNativeFileSystemStore store = ((NativeAzureFileSystem) fs).getStore();
    String[] a = blobPath.toUri().getPath().split("/");
    String key2 = a[1] + "/";
    assertTrue("Not a page blob: " + blobPath, store.isPageBlobKey(key2));
  }

  /**
   * For a set of different file sizes, write some random data to a page blob,
   * read it back, and compare that what was read is the same as what was written.
   */
  @Test
  public void testReadAfterWriteRandomData() throws IOException {

    // local shorthand
    final int pds = PAGE_DATA_SIZE;

    // Test for sizes at and near page boundaries
    int[] dataSizes = {

        // on first page
        0, 1, 2, 3,

        // Near first physical page boundary (because the implementation
        // stores PDS + the page header size bytes on each page).
        pds - 1, pds, pds + 1, pds + 2, pds + 3,

        // near second physical page boundary
        (2 * pds) - 1, (2 * pds), (2 * pds) + 1, (2 * pds) + 2, (2 * pds) + 3,

        // near tenth physical page boundary
        (10 * pds) - 1, (10 * pds), (10 * pds) + 1, (10 * pds) + 2, (10 * pds) + 3,

        // test one big size, >> 4MB (an internal buffer size in the code)
        MAX_BYTES
    };

    for (int i : dataSizes) {
      testReadAfterWriteRandomData(i);
    }
  }

  private void testReadAfterWriteRandomData(int size) throws IOException {
    writeRandomData(size);
    readRandomDataAndVerify(size);
  }

  /**
   * Read "size" bytes of data and verify that what was read and what was written
   * are the same.
   */
  private void readRandomDataAndVerify(int size) throws AzureException, IOException {
    byte[] b = new byte[size];
    FSDataInputStream stream = fs.open(blobPath);
    int bytesRead = stream.read(b);
    stream.close();
    assertEquals(bytesRead, size);

    // compare the data read to the data written
    assertTrue(comparePrefix(randomData, b, size));
  }

  // return true if the beginning "size" values of the arrays are the same
  private boolean comparePrefix(byte[] a, byte[] b, int size) {
    if (a.length < size || b.length < size) {
      return false;
    }
    for (int i = 0; i < size; i++) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  }

  // Write a specified amount of random data to the file path for this test class.
  private void writeRandomData(int size) throws IOException {
    OutputStream output = fs.create(blobPath);
    output.write(randomData, 0, size);
    output.close();
  }

  /**
   * Write data to a page blob, open it, seek, and then read a range of data.
   * Then compare that the data read from that range is the same as the data originally written.
   */
  @Test
  public void testPageBlobSeekAndReadAfterWrite() throws IOException {
    writeRandomData(PAGE_SIZE * MAX_PAGES);
    int recordSize = 100;
    byte[] b = new byte[recordSize];


    try(FSDataInputStream stream = fs.open(blobPath)) {
      // Seek to a boundary around the middle of the 6th page
      int seekPosition = 5 * PAGE_SIZE + 250;
      stream.seek(seekPosition);

      // Read a record's worth of bytes and verify results
      int bytesRead = stream.read(b);
      verifyReadRandomData(b, bytesRead, seekPosition, recordSize);

      // Seek to another spot and read a record greater than a page
      seekPosition = 10 * PAGE_SIZE + 250;
      stream.seek(seekPosition);
      recordSize = 1000;
      b = new byte[recordSize];
      bytesRead = stream.read(b);
      verifyReadRandomData(b, bytesRead, seekPosition, recordSize);

      // Read the last 100 bytes of the file
      recordSize = 100;
      seekPosition = PAGE_SIZE * MAX_PAGES - recordSize;
      stream.seek(seekPosition);
      b = new byte[recordSize];
      bytesRead = stream.read(b);
      verifyReadRandomData(b, bytesRead, seekPosition, recordSize);

      // Read past the end of the file and we should get only partial data.
      recordSize = 100;
      seekPosition = PAGE_SIZE * MAX_PAGES - recordSize + 50;
      stream.seek(seekPosition);
      b = new byte[recordSize];
      bytesRead = stream.read(b);
      assertEquals(50, bytesRead);

      // compare last 50 bytes written with those read
      byte[] tail = Arrays.copyOfRange(randomData, seekPosition, randomData.length);
      assertTrue(comparePrefix(tail, b, 50));
    }
  }

  // Verify that reading a record of data after seeking gives the expected data.
  private void verifyReadRandomData(byte[] b, int bytesRead, int seekPosition, int recordSize) {
    byte[] originalRecordData =
        Arrays.copyOfRange(randomData, seekPosition, seekPosition + recordSize + 1);
    assertEquals(recordSize, bytesRead);
    assertTrue(comparePrefix(originalRecordData, b, recordSize));
  }

  // Test many small flushed writes interspersed with periodic hflush calls.
  // For manual testing, increase NUM_WRITES to a large number.
  // The goal for a long-running manual test is to make sure that it finishes
  // and the close() call does not time out. It also facilitates debugging into
  // hflush/hsync.
  @Test
  public void testManySmallWritesWithHFlush() throws IOException {
    writeAndReadOneFile(50, 100, 20);
  }

  /**
   * Write a total of numWrites * recordLength data to a file, read it back,
   * and check to make sure what was read is the same as what was written.
   * The syncInterval is the number of writes after which to call hflush to
   * force the data to storage.
   */
  private void writeAndReadOneFile(int numWrites,
      int recordLength, int syncInterval) throws IOException {

    // A lower bound on the minimum time we think it will take to do
    // a write to Azure storage.
    final long MINIMUM_EXPECTED_TIME = 20;
    LOG.info("Writing " + numWrites * recordLength + " bytes to " + blobPath.getName());
    FSDataOutputStream output = fs.create(blobPath);
    int writesSinceHFlush = 0;
    try {

      // Do a flush and hflush to exercise case for empty write queue in PageBlobOutputStream,
      // to test concurrent execution gates.
      output.flush();
      output.hflush();
      for (int i = 0; i < numWrites; i++) {
        output.write(randomData, i * recordLength, recordLength);
        writesSinceHFlush++;
        output.flush();
        if ((i % syncInterval) == 0) {
          output.hflush();
          writesSinceHFlush = 0;
        }
      }
    } finally {
      long start = Time.monotonicNow();
      output.close();
      long end = Time.monotonicNow();
      LOG.debug("close duration = " + (end - start) + " msec.");
      if (writesSinceHFlush > 0) {
        assertTrue(String.format(
            "close duration with >= 1 pending write is %d, less than minimum expected of %d",
            end - start, MINIMUM_EXPECTED_TIME),
            end - start >= MINIMUM_EXPECTED_TIME);
        }
    }

    // Read the data back and check it.
    FSDataInputStream stream = fs.open(blobPath);
    int SIZE = numWrites * recordLength;
    byte[] b = new byte[SIZE];
    try {
      stream.seek(0);
      stream.read(b, 0, SIZE);
      verifyReadRandomData(b, SIZE, 0, SIZE);
    } finally {
      stream.close();
    }

    // delete the file
    fs.delete(blobPath, false);
  }

  // Test writing to a large file repeatedly as a stress test.
  // Set the repetitions to a larger number for manual testing
  // for a longer stress run.
  @Test
  public void testLargeFileStress() throws IOException {
    int numWrites = 32;
    int recordSize = 1024 * 1024;
    int syncInterval = 10;
    int repetitions = 1;
    for (int i = 0; i < repetitions; i++) {
      writeAndReadOneFile(numWrites, recordSize, syncInterval);
    }
  }
  
  // Write to a file repeatedly to verify that it extends.
  // The page blob file should start out at 128MB and finish at 256MB.
  public void testFileSizeExtension() throws IOException {
    final int writeSize = 1024 * 1024;
    final int numWrites = 129;
    final byte dataByte = 5;
    byte[] data = new byte[writeSize];
    Arrays.fill(data, dataByte);
    try (FSDataOutputStream output = fs.create(blobPath)) {
      for (int i = 0; i < numWrites; i++) {
        output.write(data);
        output.hflush();
        LOG.debug("total writes = " + (i + 1));
      }
    }

    // Show that we wrote more than the default page blob file size.
    assertTrue(numWrites * writeSize > PageBlobOutputStream.PAGE_BLOB_MIN_SIZE);

    // Verify we can list the new size. That will prove we expanded the file.
    FileStatus[] status = fs.listStatus(blobPath);
    assertEquals("File size hasn't changed " + status,
        numWrites * writeSize, status[0].getLen());
    LOG.debug("Total bytes written to " + blobPath + " = " + status[0].getLen());
    fs.delete(blobPath, false);
  }

}
