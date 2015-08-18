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
package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.io.AltFileInputStream;
import org.apache.hadoop.test.PathUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestAltFileInputStream {
  private static final File TEST_DIR = PathUtils.getTestDir(TestAltFileInputStream.class);
  private static final File TEST_FILE = new File(TEST_DIR,
      "testAltFileInputStream.dat");

  private static final int TEST_DATA_LEN = 7 * 1024; // 7 KB test data
  private static final byte[] TEST_DATA = DFSTestUtil.generateSequentialBytes(0, TEST_DATA_LEN);

  @Before
  public void setup() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
    assertTrue(TEST_DIR.mkdirs());
    FileOutputStream fos = new FileOutputStream(TEST_FILE);
    fos.write(TEST_DATA);
    fos.close();
  }

  @Test
  public void readWithFileName() throws Exception {
    AltFileInputStream inputStream = new AltFileInputStream(TEST_FILE);
    assertNotNull(inputStream.getFD());
    assertNotNull(inputStream.getChannel());
    calculate(inputStream);
  }

  @Test
  public void readWithFdAndChannel() throws Exception {
    RandomAccessFile raf = new RandomAccessFile(TEST_FILE, "r");
    AltFileInputStream inputStream = new AltFileInputStream(raf.getFD(), raf.getChannel());
    assertNotNull(inputStream.getFD());
    assertNotNull(inputStream.getChannel());
    calculate(inputStream);
  }

  @Test
  public void readWithFileByFileInputStream() throws Exception {
    FileInputStream fileInputStream = new FileInputStream(TEST_FILE);
    assertNotNull(fileInputStream.getChannel());
    assertNotNull(fileInputStream.getFD());
    calculate(fileInputStream);
  }

  public void calculate(InputStream inputStream) throws Exception {
   long numberOfTheFileByte = 0;
   while (inputStream.read() != -1) {
     numberOfTheFileByte++;
   }
   assertEquals(TEST_DATA_LEN, numberOfTheFileByte);
   inputStream.close();
  }
}