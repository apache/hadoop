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

package org.apache.hadoop.fs.swift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftConnectionClosedException;
import org.apache.hadoop.fs.swift.http.SwiftProtocolConstants;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;

/**
 * Seek tests verify that
 * <ol>
 *   <li>When you seek on a 0 byte file to byte (0), it's not an error.</li>
 *   <li>When you seek past the end of a file, it's an error that should
 *   raise -what- EOFException?</li>
 *   <li>when you seek forwards, you get new data</li>
 *   <li>when you seek backwards, you get the previous data</li>
 *   <li>That this works for big multi-MB files as well as small ones.</li>
 * </ol>
 * These may seem "obvious", but the more the input streams try to be clever
 * about offsets and buffering, the more likely it is that seek() will start
 * to get confused.
 */
public class TestSeek extends SwiftFileSystemBaseTest {
  protected static final Log LOG =
    LogFactory.getLog(TestSeek.class);
  public static final int SMALL_SEEK_FILE_LEN = 256;

  private Path testPath;
  private Path smallSeekFile;
  private Path zeroByteFile;
  private FSDataInputStream instream;

  /**
   * Setup creates dirs under test/hadoop
   *
   * @throws Exception
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    //delete the test directory
    testPath = path("/test");
    smallSeekFile = new Path(testPath, "seekfile.txt");
    zeroByteFile = new Path(testPath, "zero.txt");
    byte[] block = SwiftTestUtils.dataset(SMALL_SEEK_FILE_LEN, 0, 255);
    //this file now has a simple rule: offset => value
    createFile(smallSeekFile, block);
    createEmptyFile(zeroByteFile);
  }

  @After
  public void cleanFile() {
    IOUtils.closeStream(instream);
    instream = null;
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekZeroByteFile() throws Throwable {
    instream = fs.open(zeroByteFile);
    assertEquals(0, instream.getPos());
    //expect initial read to fai;
    int result = instream.read();
    assertMinusOne("initial byte read", result);
    byte[] buffer = new byte[1];
    //expect that seek to 0 works
    instream.seek(0);
    //reread, expect same exception
    result = instream.read();
    assertMinusOne("post-seek byte read", result);
    result = instream.read(buffer, 0, 1);
    assertMinusOne("post-seek buffer read", result);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testBlockReadZeroByteFile() throws Throwable {
    instream = fs.open(zeroByteFile);
    assertEquals(0, instream.getPos());
    //expect that seek to 0 works
    byte[] buffer = new byte[1];
    int result = instream.read(buffer, 0, 1);
    assertMinusOne("block read zero byte file", result);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekReadClosedFile() throws Throwable {
    instream = fs.open(smallSeekFile);
    instream.close();
    try {
      instream.seek(0);
    } catch (SwiftConnectionClosedException e) {
      //expected a closed file
    }
    try {
      instream.read();
    } catch (IOException e) {
      //expected a closed file
    }
    try {
      byte[] buffer = new byte[1];
      int result = instream.read(buffer, 0, 1);
    } catch (IOException e) {
      //expected a closed file
    }
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testNegativeSeek() throws Throwable {
    instream = fs.open(smallSeekFile);
    assertEquals(0, instream.getPos());
    try {
      instream.seek(-1);
      long p = instream.getPos();
      LOG.warn("Seek to -1 returned a position of " + p);
      int result = instream.read();
      fail(
        "expected an exception, got data " + result + " at a position of " + p);
    } catch (IOException e) {
      //bad seek -expected
    }
    assertEquals(0, instream.getPos());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekFile() throws Throwable {
    instream = fs.open(smallSeekFile);
    assertEquals(0, instream.getPos());
    //expect that seek to 0 works
    instream.seek(0);
    int result = instream.read();
    assertEquals(0, result);
    assertEquals(1, instream.read());
    assertEquals(2, instream.getPos());
    assertEquals(2, instream.read());
    assertEquals(3, instream.getPos());
    instream.seek(128);
    assertEquals(128, instream.getPos());
    assertEquals(128, instream.read());
    instream.seek(63);
    assertEquals(63, instream.read());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekAndReadPastEndOfFile() throws Throwable {
    instream = fs.open(smallSeekFile);
    assertEquals(0, instream.getPos());
    //expect that seek to 0 works
    //go just before the end
    instream.seek(SMALL_SEEK_FILE_LEN - 2);
    assertTrue("Premature EOF", instream.read() != -1);
    assertTrue("Premature EOF", instream.read() != -1);
    assertMinusOne("read past end of file", instream.read());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekAndPastEndOfFileThenReseekAndRead() throws Throwable {
    instream = fs.open(smallSeekFile);
    //go just before the end. This may or may not fail; it may be delayed until the
    //read
    try {
      instream.seek(SMALL_SEEK_FILE_LEN);
      //if this doesn't trigger, then read() is expected to fail
      assertMinusOne("read after seeking past EOF", instream.read());
    } catch (EOFException expected) {
      //here an exception was raised in seek
    }
    instream.seek(1);
    assertTrue("Premature EOF", instream.read() != -1);
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.set(SwiftProtocolConstants.SWIFT_REQUEST_SIZE, "1");
    return conf;
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekBigFile() throws Throwable {
    Path testSeekFile = new Path(testPath, "bigseekfile.txt");
    byte[] block = SwiftTestUtils.dataset(65536, 0, 255);
    createFile(testSeekFile, block);
    instream = fs.open(testSeekFile);
    assertEquals(0, instream.getPos());
    //expect that seek to 0 works
    instream.seek(0);
    int result = instream.read();
    assertEquals(0, result);
    assertEquals(1, instream.read());
    assertEquals(2, instream.read());

    //do seek 32KB ahead
    instream.seek(32768);
    assertEquals("@32768", block[32768], (byte) instream.read());
    instream.seek(40000);
    assertEquals("@40000", block[40000], (byte) instream.read());
    instream.seek(8191);
    assertEquals("@8191", block[8191], (byte) instream.read());
    instream.seek(0);
    assertEquals("@0", 0, (byte) instream.read());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testPositionedBulkReadDoesntChangePosition() throws Throwable {
    Path testSeekFile = new Path(testPath, "bigseekfile.txt");
    byte[] block = SwiftTestUtils.dataset(65536, 0, 255);
    createFile(testSeekFile, block);
    instream = fs.open(testSeekFile);
    instream.seek(39999);
    assertTrue(-1 != instream.read());
    assertEquals (40000, instream.getPos());

    byte[] readBuffer = new byte[256];
    instream.read(128, readBuffer, 0, readBuffer.length);
    //have gone back
    assertEquals(40000, instream.getPos());
    //content is the same too
    assertEquals("@40000", block[40000], (byte) instream.read());
    //now verify the picked up data
    for (int i = 0; i < 256; i++) {
      assertEquals("@" + i, block[i + 128], readBuffer[i]);
    }
  }

  /**
   * work out the expected byte from a specific offset
   * @param offset offset in the file
   * @return the value
   */
  int expectedByte(int offset) {
    return offset & 0xff;
  }
}

