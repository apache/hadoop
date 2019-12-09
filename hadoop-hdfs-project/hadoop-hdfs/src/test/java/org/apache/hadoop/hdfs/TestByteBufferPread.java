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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.fs.FSExceptionMessages.EOF_IN_READ_FULLY;
import static org.apache.hadoop.fs.FSExceptionMessages.NEGATIVE_POSITION_READ;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the DFS positional read functionality on a single node
 * mini-cluster. These tests are inspired from {@link TestPread}. The tests
 * are much less comprehensive than other pread tests because pread already
 * internally uses ByteBuffers.
 */
@SuppressWarnings({"NestedAssignment", "StaticNonFinalField"})
@RunWith(Parameterized.class)
public class TestByteBufferPread {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestByteBufferPread.class);

  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  private static byte[] fileContents;
  private static Path testFile;
  private static Path emptyFile;
  private static Random rand;

  private static final long SEED = 0xDEADBEEFL;
  private static final int BLOCK_SIZE = 4096;
  private static final int FILE_SIZE = 12 * BLOCK_SIZE;

  public static final int HALF_SIZE = FILE_SIZE / 2;
  public static final int QUARTER_SIZE = FILE_SIZE / 4;
  public static final int EOF_POS = FILE_SIZE -1;

  private final boolean useHeap;

  private ByteBuffer buffer;

  private ByteBuffer emptyBuffer;

  /**
   * This test suite is parameterized for the different heap allocation
   * options.
   * @return a list of allocation policies to test.
   */
  @Parameterized.Parameters(name = "heap={0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {true},
        {false},
    });
  }

  public TestByteBufferPread(final boolean useHeap) {
    this.useHeap = useHeap;
  }

  @BeforeClass
  public static void setupClass() throws IOException {
    // Setup the cluster with a small block size so we can create small files
    // that span multiple blocks
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    fs = cluster.getFileSystem();

    // Create a test file that spans 12 blocks, and contains a bunch of random
    // bytes
    fileContents = new byte[FILE_SIZE];
    rand = new Random(SEED);
    rand.nextBytes(fileContents);
    testFile = new Path("/byte-buffer-pread-test.dat");
    try (FSDataOutputStream out = fs.create(testFile, (short) 3)) {
      out.write(fileContents);
    }
    emptyFile = new Path("/byte-buffer-pread-emptyfile.dat");
    ContractTestUtils.touch(fs, emptyFile);
  }

  @Before
  public void setup() throws IOException {
    if (useHeap) {
      buffer = ByteBuffer.allocate(FILE_SIZE);
      emptyBuffer = ByteBuffer.allocate(0);
    } else {
      buffer = ByteBuffer.allocateDirect(FILE_SIZE);
      emptyBuffer = ByteBuffer.allocateDirect(0);
    }
  }

  /**
   * Reads the entire testFile using the pread API and validates that its
   * contents are properly loaded into the {@link ByteBuffer}.
   */
  @Test
  public void testPreadWithByteBuffer() throws IOException {
    int bytesRead;
    int totalBytesRead = 0;
    try (FSDataInputStream in = fs.open(testFile)) {
      while ((bytesRead = in.read(totalBytesRead, buffer)) > 0) {
        totalBytesRead += bytesRead;
        // Check that each call to read changes the position of the ByteBuffer
        // correctly
        assertBufferPosition(totalBytesRead);
      }

      // Make sure the buffer is full
      assertBufferIsFull();
      // Make sure the contents of the read buffer equal the contents of the
      // file
      assertBufferEqualsFileContents(0, FILE_SIZE, 0);
    }
  }

  /**
   * Assert that the value of {@code buffer.position()} equals
   * the expected value.
   * @param pos required position.
   */
  private void assertBufferPosition(final int pos) {
    assertEquals("Buffer position",
        pos, buffer.position());
  }

  /**
   * Assert the stream is at the given position.
   * @param in stream
   * @param pos required position
   * @throws IOException seek() failure.
   */
  private void assertStreamPosition(final Seekable in, long pos)
      throws IOException {
    assertEquals("Buffer position",
        pos, in.getPos());
  }

  /**
   * Assert that the buffer is full.
   */
  private void assertBufferIsFull() {
    assertFalse("Buffer is not full", buffer.hasRemaining());
  }

  /**
   * Assert that the buffer is not full full.
   */
  private void assertBufferIsNotFull() {
    assertTrue("Buffer is full", buffer.hasRemaining());
  }

  /**
   * Attempts to read the testFile into a {@link ByteBuffer} that is already
   * full, and validates that doing so does not change the contents of the
   * supplied {@link ByteBuffer}.
   */
  @Test
  public void testPreadWithFullByteBuffer()
          throws IOException {
    // Load some dummy data into the buffer
    byte[] existingBufferBytes = new byte[FILE_SIZE];
    rand.nextBytes(existingBufferBytes);
    buffer.put(existingBufferBytes);
    // Make sure the buffer is full
    assertBufferIsFull();

    try (FSDataInputStream in = fs.open(testFile)) {
      // Attempt to read into the buffer, 0 bytes should be read since the
      // buffer is full
      assertEquals(0, in.read(buffer));

      // Double check the buffer is still full and its contents have not
      // changed
      assertBufferIsFull();
      buffer.position(0);
      byte[] bufferContents = new byte[FILE_SIZE];
      buffer.get(bufferContents);
      assertArrayEquals(bufferContents, existingBufferBytes);
    }
  }

  /**
   * Reads half of the testFile into the {@link ByteBuffer} by setting a
   * {@link ByteBuffer#limit()} on the buffer. Validates that only half of the
   * testFile is loaded into the buffer.
   */
  @Test
  public void testPreadWithLimitedByteBuffer() throws IOException {
    int bytesRead;
    int totalBytesRead = 0;
    // Set the buffer limit to half the size of the file
    buffer.limit(HALF_SIZE);

    try (FSDataInputStream in = fs.open(testFile)) {
      in.seek(EOF_POS);
      while ((bytesRead = in.read(totalBytesRead, buffer)) > 0) {
        totalBytesRead += bytesRead;
        // Check that each call to read changes the position of the ByteBuffer
        // correctly
        assertBufferPosition(totalBytesRead);
      }

      // Since we set the buffer limit to half the size of the file, we should
      // have only read half of the file into the buffer
      assertEquals(HALF_SIZE, totalBytesRead);
      // Check that the buffer is full and the contents equal the first half of
      // the file
      assertBufferIsFull();
      assertBufferEqualsFileContents(0, HALF_SIZE, 0);

      // position hasn't changed
      assertStreamPosition(in, EOF_POS);
    }
  }

  /**
   * Reads half of the testFile into the {@link ByteBuffer} by setting the
   * {@link ByteBuffer#position()} the half the size of the file. Validates that
   * only half of the testFile is loaded into the buffer.
   * <p>
   * This test interleaves reading from the stream by the classic input
   * stream API, verifying those bytes are also as expected.
   * This lets us validate the requirement that these positions reads must
   * not interfere with the conventional read sequence.
   */
  @Test
  public void testPreadWithPositionedByteBuffer() throws IOException {
    int bytesRead;
    int totalBytesRead = 0;
    // Set the buffer position to half the size of the file
    buffer.position(HALF_SIZE);
    int counter = 0;

    try (FSDataInputStream in = fs.open(testFile)) {
      assertEquals("Byte read from stream",
          fileContents[counter++], in.read());
      while ((bytesRead = in.read(totalBytesRead, buffer)) > 0) {
        totalBytesRead += bytesRead;
        // Check that each call to read changes the position of the ByteBuffer
        // correctly
        assertBufferPosition(totalBytesRead + HALF_SIZE);
        // read the next byte.
        assertEquals("Byte read from stream",
            fileContents[counter++], in.read());
      }

      // Since we set the buffer position to half the size of the file, we
      // should have only read half of the file into the buffer
      assertEquals("bytes read",
          HALF_SIZE, totalBytesRead);
      // Check that the buffer is full and the contents equal the first half of
      // the file
      assertBufferIsFull();
      assertBufferEqualsFileContents(HALF_SIZE, HALF_SIZE, 0);
    }
  }

  /**
   * Assert the buffer ranges matches that in the file.
   * @param bufferPosition buffer position
   * @param length length of data to check
   * @param fileOffset offset in file.
   */
  private void assertBufferEqualsFileContents(int bufferPosition,
      int length,
      int fileOffset) {
    buffer.position(bufferPosition);
    byte[] bufferContents = new byte[length];
    buffer.get(bufferContents);
    assertArrayEquals(
        "Buffer data from [" + bufferPosition + "-" + length + "]",
        bufferContents,
        Arrays.copyOfRange(fileContents, fileOffset, fileOffset + length));
  }

  /**
   * Reads half of the testFile into the {@link ByteBuffer} by specifying a
   * position for the pread API that is half of the file size. Validates that
   * only half of the testFile is loaded into the buffer.
   */
  @Test
  public void testPositionedPreadWithByteBuffer() throws IOException {
    int bytesRead;
    int totalBytesRead = 0;

    try (FSDataInputStream in = fs.open(testFile)) {
      // Start reading from halfway through the file
      while ((bytesRead = in.read(totalBytesRead + HALF_SIZE,
              buffer)) > 0) {
        totalBytesRead += bytesRead;
        // Check that each call to read changes the position of the ByteBuffer
        // correctly
        assertBufferPosition(totalBytesRead);
      }

      // Since we starting reading halfway through the file, the buffer should
      // only be half full
      assertEquals("bytes read", HALF_SIZE, totalBytesRead);
      assertBufferPosition(HALF_SIZE);
      assertBufferIsNotFull();
      // Check that the buffer contents equal the second half of the file
      assertBufferEqualsFileContents(0, HALF_SIZE, HALF_SIZE);
    }
  }

  /**
   * Reads the entire testFile using the preadFully API and validates that its
   * contents are properly loaded into the {@link ByteBuffer}.
   */
  @Test
  public void testPreadFullyWithByteBuffer() throws IOException {
    int totalBytesRead = 0;
    try (FSDataInputStream in = fs.open(testFile)) {
      in.readFully(totalBytesRead, buffer);
      // Make sure the buffer is full
      assertBufferIsFull();
      // Make sure the contents of the read buffer equal the contents of the
      // file
      assertBufferEqualsFileContents(0, FILE_SIZE, 0);
    }
  }

  /**
   * readFully past the end of the file into an empty buffer; expect this
   * to be a no-op.
   */
  @Test
  public void testPreadFullyPastEOFEmptyByteBuffer() throws IOException {
    try (FSDataInputStream in = fs.open(testFile)) {
      in.readFully(FILE_SIZE + 10, emptyBuffer);
    }
  }

  /**
   * Reads from a negative position -expects a failure.
   * Also uses the new openFile() API to improve its coverage.
   */
  @Test
  public void testPreadFullyNegativeOffset() throws Exception {
    try (FSDataInputStream in = fs.openFile(testFile).build().get()) {
      in.seek(QUARTER_SIZE);
      intercept(EOFException.class, NEGATIVE_POSITION_READ,
          () -> in.readFully(-1, buffer));
      // the stream position has not changed.
      assertStreamPosition(in, QUARTER_SIZE);
    }
  }

  /**
   * Read fully with a start position past the EOF -expects a failure.
   */
  @Test
  public void testPreadFullyPositionPastEOF() throws Exception {
    try (FSDataInputStream in = fs.openFile(testFile).build().get()) {
      in.seek(QUARTER_SIZE);
      intercept(EOFException.class, EOF_IN_READ_FULLY,
          () -> in.readFully(FILE_SIZE * 2, buffer));
      // the stream position has not changed.
      assertStreamPosition(in, QUARTER_SIZE);
    }
  }

  /**
   * Read which goes past the EOF; expects a failure.
   * The final state of the buffer is undefined; it may fail fast or fail late.
   * Also uses the new openFile() API to improve its coverage.
   */
  @Test
  public void testPreadFullySpansEOF() throws Exception {
    try (FSDataInputStream in = fs.openFile(testFile).build().get()) {
      intercept(EOFException.class, EOF_IN_READ_FULLY,
          () -> in.readFully(FILE_SIZE - 10, buffer));
      if (buffer.position() > 0) {
        // this implementation does not do a range check before the read;
        // it got partway through before failing.
        // this is not an error -just inefficient.
        LOG.warn("Buffer reads began before range checks with {}", in);
      }
    }
  }

  /**
   * Reads from a negative position into an empty buffer -expects a failure.
   * That is: position checks happen before any probes for an empty buffer.
   */
  @Test
  public void testPreadFullyEmptyBufferNegativeOffset() throws Exception {
    try (FSDataInputStream in = fs.openFile(testFile).build().get()) {
      intercept(EOFException.class, NEGATIVE_POSITION_READ,
          () -> in.readFully(-1, emptyBuffer));
    }
  }

  /**
   * Reads from position 0 on an empty file into a non-empty buffer
   * will fail as there is not enough data.
   */
  @Test
  public void testPreadFullyEmptyFile() throws Exception {
    try (FSDataInputStream in = fs.openFile(emptyFile).build().get()) {
      intercept(EOFException.class, EOF_IN_READ_FULLY,
          () -> in.readFully(0, buffer));
    }
  }

  /**
   * Reads from position 0 on an empty file into an empty buffer,
   * This MUST succeed.
   */
  @Test
  public void testPreadFullyEmptyFileEmptyBuffer() throws Exception {
    try (FSDataInputStream in = fs.openFile(emptyFile).build().get()) {
      in.readFully(0, emptyBuffer);
      assertStreamPosition(in, 0);
    }
  }

  /**
   * Pread from position 0 of an empty file.
   * This MUST succeed, but 0 bytes will be read.
   */
  @Test
  public void testPreadEmptyFile() throws Exception {
    try (FSDataInputStream in = fs.openFile(emptyFile).build().get()) {
      int bytesRead = in.read(0, buffer);
      assertEquals("bytes read from empty file", -1, bytesRead);
      assertStreamPosition(in, 0);
    }
  }

  @AfterClass
  public static void shutdown() throws IOException {
    try {
      if (fs != null) {
        fs.delete(testFile, false);
        fs.delete(emptyFile, false);
        fs.close();
      }
    } finally {
      fs = null;
      if (cluster != null) {
        cluster.shutdown(true);
        cluster = null;
      }
    }
  }
}
