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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.LambdaTestUtils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.util.concurrent.Callable;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;

/**
 * Test S3A Failure translation, including a functional test
 * generating errors during stream IO.
 */
public class ITestS3AFailureHandling extends AbstractS3ATestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AFailureHandling.class);

  @Test
  public void testReadFileChanged() throws Throwable {
    describe("overwrite a file with a shorter one during a read, seek");
    final int fullLength = 8192;
    final byte[] fullDataset = dataset(fullLength, 'a', 32);
    final int shortLen = 4096;
    final byte[] shortDataset = dataset(shortLen, 'A', 32);
    final FileSystem fs = getFileSystem();
    final Path testpath = path("readFileToChange.txt");
    // initial write
    writeDataset(fs, testpath, fullDataset, fullDataset.length, 1024, false);
    try(FSDataInputStream instream = fs.open(testpath)) {
      instream.seek(fullLength - 16);
      assertTrue("no data to read", instream.read() >= 0);
      // overwrite
      writeDataset(fs, testpath, shortDataset, shortDataset.length, 1024, true);
      // here the file length is less. Probe the file to see if this is true,
      // with a spin and wait
      LambdaTestUtils.eventually(30 * 1000, 1000,
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              assertEquals(shortLen, fs.getFileStatus(testpath).getLen());
              return null;
            }
          });

      // here length is shorter. Assuming it has propagated to all replicas,
      // the position of the input stream is now beyond the EOF.
      // An attempt to seek backwards to a position greater than the
      // short length will raise an exception from AWS S3, which must be
      // translated into an EOF

      instream.seek(shortLen + 1024);
      int c = instream.read();
      assertIsEOF("read()", c);

      byte[] buf = new byte[256];

      assertIsEOF("read(buffer)", instream.read(buf));
      assertIsEOF("read(offset)",
          instream.read(instream.getPos(), buf, 0, buf.length));

      // now do a block read fully, again, backwards from the current pos
      try {
        instream.readFully(shortLen + 512, buf);
        fail("Expected readFully to fail");
      } catch (EOFException expected) {
        LOG.debug("Expected EOF: ", expected);
      }

      assertIsEOF("read(offset)",
          instream.read(shortLen + 510, buf, 0, buf.length));

      // seek somewhere useful
      instream.seek(shortLen - 256);

      // delete the file. Reads must fail
      fs.delete(testpath, false);

      try {
        int r = instream.read();
        fail("Expected an exception, got " + r);
      } catch (FileNotFoundException e) {
        // expected
      }

      try {
        instream.readFully(2048, buf);
        fail("Expected readFully to fail");
      } catch (FileNotFoundException e) {
        // expected
      }

    }
  }

  /**
   * Assert that a read operation returned an EOF value.
   * @param operation specific operation
   * @param readResult result
   */
  private void assertIsEOF(String operation, int readResult) {
    assertEquals("Expected EOF from "+ operation
        + "; got char " + (char) readResult, -1, readResult);
  }
}
