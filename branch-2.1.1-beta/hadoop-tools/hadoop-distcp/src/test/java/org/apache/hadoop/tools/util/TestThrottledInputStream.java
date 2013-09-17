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

package org.apache.hadoop.tools.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;

public class TestThrottledInputStream {
  private static final Log LOG = LogFactory.getLog(TestThrottledInputStream.class);
  private static final int BUFF_SIZE = 1024;

  private enum CB {ONE_C, BUFFER, BUFF_OFFSET}

  @Test
  public void testRead() {
    File tmpFile;
    File outFile;
    try {
      tmpFile = createFile(1024);
      outFile = createFile();

      tmpFile.deleteOnExit();
      outFile.deleteOnExit();

      long maxBandwidth = copyAndAssert(tmpFile, outFile, 0, 1, -1, CB.BUFFER);

      copyAndAssert(tmpFile, outFile, maxBandwidth, 20, 0, CB.BUFFER);
/*
      copyAndAssert(tmpFile, outFile, maxBandwidth, 10, 0, CB.BUFFER);
      copyAndAssert(tmpFile, outFile, maxBandwidth, 50, 0, CB.BUFFER);
*/

      copyAndAssert(tmpFile, outFile, maxBandwidth, 20, 0, CB.BUFF_OFFSET);
/*
      copyAndAssert(tmpFile, outFile, maxBandwidth, 10, 0, CB.BUFF_OFFSET);
      copyAndAssert(tmpFile, outFile, maxBandwidth, 50, 0, CB.BUFF_OFFSET);
*/

      copyAndAssert(tmpFile, outFile, maxBandwidth, 20, 0, CB.ONE_C);
/*
      copyAndAssert(tmpFile, outFile, maxBandwidth, 10, 0, CB.ONE_C);
      copyAndAssert(tmpFile, outFile, maxBandwidth, 50, 0, CB.ONE_C);
*/
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
    }
  }

  private long copyAndAssert(File tmpFile, File outFile,
                             long maxBandwidth, float factor,
                             int sleepTime, CB flag) throws IOException {
    long bandwidth;
    ThrottledInputStream in;
    long maxBPS = (long) (maxBandwidth / factor);

    if (maxBandwidth == 0) {
      in = new ThrottledInputStream(new FileInputStream(tmpFile));
    } else {
      in = new ThrottledInputStream(new FileInputStream(tmpFile), maxBPS);
    }
    OutputStream out = new FileOutputStream(outFile);
    try {
      if (flag == CB.BUFFER) {
        copyBytes(in, out, BUFF_SIZE);
      } else if (flag == CB.BUFF_OFFSET){
        copyBytesWithOffset(in, out, BUFF_SIZE);
      } else {
        copyByteByByte(in, out);
      }

      LOG.info(in);
      bandwidth = in.getBytesPerSec();
      Assert.assertEquals(in.getTotalBytesRead(), tmpFile.length());
      Assert.assertTrue(in.getBytesPerSec() > maxBandwidth / (factor * 1.2));
      Assert.assertTrue(in.getTotalSleepTime() >  sleepTime || in.getBytesPerSec() <= maxBPS);
    } finally {
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
    }
    return bandwidth;
  }

  private static void copyBytesWithOffset(InputStream in, OutputStream out, int buffSize)
    throws IOException {

    byte buf[] = new byte[buffSize];
    int bytesRead = in.read(buf, 0, buffSize);
    while (bytesRead >= 0) {
      out.write(buf, 0, bytesRead);
      bytesRead = in.read(buf);
    }
  }

  private static void copyByteByByte(InputStream in, OutputStream out)
    throws IOException {

    int ch = in.read();
    while (ch >= 0) {
      out.write(ch);
      ch = in.read();
    }
  }

  private static void copyBytes(InputStream in, OutputStream out, int buffSize)
    throws IOException {

    byte buf[] = new byte[buffSize];
    int bytesRead = in.read(buf);
    while (bytesRead >= 0) {
      out.write(buf, 0, bytesRead);
      bytesRead = in.read(buf);
    }
  }

  private File createFile(long sizeInKB) throws IOException {
    File tmpFile = createFile();
    writeToFile(tmpFile, sizeInKB);
    return tmpFile;
  }

  private File createFile() throws IOException {
    return File.createTempFile("tmp", "dat");
  }

  private void writeToFile(File tmpFile, long sizeInKB) throws IOException {
    OutputStream out = new FileOutputStream(tmpFile);
    try {
      byte[] buffer = new byte [1024];
      for (long index = 0; index < sizeInKB; index++) {
        out.write(buffer);
      }
    } finally {
      IOUtils.closeStream(out);
    }
  }
}
