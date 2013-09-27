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
import org.apache.hadoop.fs.swift.http.SwiftProtocolConstants;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Test;

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
public class TestReadPastBuffer extends SwiftFileSystemBaseTest {
  protected static final Log LOG =
    LogFactory.getLog(TestReadPastBuffer.class);
  public static final int SWIFT_READ_BLOCKSIZE = 4096;
  public static final int SEEK_FILE_LEN = SWIFT_READ_BLOCKSIZE * 2;

  private Path testPath;
  private Path readFile;
  private Path zeroByteFile;
  private FSDataInputStream instream;


  /**
   * Get a configuration which a small blocksize reported to callers
   * @return a configuration for this test
   */
  @Override
  public Configuration getConf() {
    Configuration conf = super.getConf();
    /*
     * set to 4KB
     */
    conf.setInt(SwiftProtocolConstants.SWIFT_BLOCKSIZE, SWIFT_READ_BLOCKSIZE);
    return conf;
  }

  /**
   * Setup creates dirs under test/hadoop
   *
   * @throws Exception
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    byte[] block = SwiftTestUtils.dataset(SEEK_FILE_LEN, 0, 255);

    //delete the test directory
    testPath = path("/test");
    readFile = new Path(testPath, "TestReadPastBuffer.txt");
    createFile(readFile, block);
  }

  @After
  public void cleanFile() {
    IOUtils.closeStream(instream);
    instream = null;
  }

  /**
   * Create a config with a 1KB request size
   * @return a config
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.set(SwiftProtocolConstants.SWIFT_REQUEST_SIZE, "1");
    return conf;
  }

  /**
   * Seek past the buffer then read
   * @throws Throwable problems
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekAndReadPastEndOfFile() throws Throwable {
    instream = fs.open(readFile);
    assertEquals(0, instream.getPos());
    //expect that seek to 0 works
    //go just before the end
    instream.seek(SEEK_FILE_LEN - 2);
    assertTrue("Premature EOF", instream.read() != -1);
    assertTrue("Premature EOF", instream.read() != -1);
    assertMinusOne("read past end of file", instream.read());
  }

  /**
   * Seek past the buffer and attempt a read(buffer)
   * @throws Throwable failures
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekBulkReadPastEndOfFile() throws Throwable {
    instream = fs.open(readFile);
    assertEquals(0, instream.getPos());
    //go just before the end
    instream.seek(SEEK_FILE_LEN - 1);
    byte[] buffer = new byte[1];
    int result = instream.read(buffer, 0, 1);
    //next byte is expected to fail
    result = instream.read(buffer, 0, 1);
    assertMinusOne("read past end of file", result);
    //and this one
    result = instream.read(buffer, 0, 1);
    assertMinusOne("read past end of file", result);

    //now do an 0-byte read and expect it to
    //to be checked first
    result = instream.read(buffer, 0, 0);
    assertEquals("EOF checks coming before read range check", 0, result);

  }



  /**
   * Read past the buffer size byte by byte and verify that it refreshed
   * @throws Throwable
   */
  @Test
  public void testReadPastBufferSize() throws Throwable {
    instream = fs.open(readFile);

    while (instream.read() != -1);
    //here we have gone past the end of a file and its buffer. Now try again
    assertMinusOne("reading after the (large) file was read: "+ instream,
                   instream.read());
  }
}

