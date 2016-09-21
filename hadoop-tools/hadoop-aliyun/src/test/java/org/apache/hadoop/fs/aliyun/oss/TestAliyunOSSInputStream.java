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

package org.apache.hadoop.fs.aliyun.oss;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Tests basic functionality for AliyunOSSInputStream, including seeking and
 * reading files.
 */
public class TestAliyunOSSInputStream {

  private FileSystem fs;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAliyunOSSInputStream.class);

  private static String testRootPath =
      AliyunOSSTestUtils.generateUniqueTestPath();

  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    fs = AliyunOSSTestUtils.createTestFileSystem(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(new Path(testRootPath), true);
    }
  }

  private Path setPath(String path) {
    if (path.startsWith("/")) {
      return new Path(testRootPath + path);
    } else {
      return new Path(testRootPath + "/" + path);
    }
  }

  @Test
  public void testSeekFile() throws Exception {
    Path smallSeekFile = setPath("/test/smallSeekFile.txt");
    long size = 5 * 1024 * 1024;

    ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256, 255);
    LOG.info("5MB file created: smallSeekFile.txt");

    FSDataInputStream instream = this.fs.open(smallSeekFile);
    int seekTimes = 5;
    LOG.info("multiple fold position seeking test...:");
    for (int i = 0; i < seekTimes; i++) {
      long pos = size / (seekTimes - i) - 1;
      LOG.info("begin seeking for pos: " + pos);
      instream.seek(pos);
      assertTrue("expected position at:" + pos + ", but got:"
          + instream.getPos(), instream.getPos() == pos);
      LOG.info("completed seeking at pos: " + instream.getPos());
    }
    LOG.info("random position seeking test...:");
    Random rand = new Random();
    for (int i = 0; i < seekTimes; i++) {
      long pos = Math.abs(rand.nextLong()) % size;
      LOG.info("begin seeking for pos: " + pos);
      instream.seek(pos);
      assertTrue("expected position at:" + pos + ", but got:"
          + instream.getPos(), instream.getPos() == pos);
      LOG.info("completed seeking at pos: " + instream.getPos());
    }
    IOUtils.closeStream(instream);
  }

  @Test
  public void testReadFile() throws Exception {
    final int bufLen = 256;
    final int sizeFlag = 5;
    String filename = "readTestFile_" + sizeFlag + ".txt";
    Path readTestFile = setPath("/test/" + filename);
    long size = sizeFlag * 1024 * 1024;

    ContractTestUtils.generateTestFile(this.fs, readTestFile, size, 256, 255);
    LOG.info(sizeFlag + "MB file created: /test/" + filename);

    FSDataInputStream instream = this.fs.open(readTestFile);
    byte[] buf = new byte[bufLen];
    long bytesRead = 0;
    while (bytesRead < size) {
      int bytes;
      if (size - bytesRead < bufLen) {
        int remaining = (int)(size - bytesRead);
        bytes = instream.read(buf, 0, remaining);
      } else {
        bytes = instream.read(buf, 0, bufLen);
      }
      bytesRead += bytes;

      if (bytesRead % (1024 * 1024) == 0) {
        int available = instream.available();
        int remaining = (int)(size - bytesRead);
        assertTrue("expected remaining:" + remaining + ", but got:" + available,
            remaining == available);
        LOG.info("Bytes read: " + Math.round((double)bytesRead / (1024 * 1024))
            + " MB");
      }
    }
    assertTrue(instream.available() == 0);
    IOUtils.closeStream(instream);
  }
}
