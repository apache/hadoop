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
import java.nio.channels.ClosedChannelException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestAltFileInputStream {

  private static final File TEST_DIR = PathUtils.getTestDir(TestAltFileInputStream.class);
  private static final File TEST_FILE = new File(TEST_DIR,
      "testAltFileInputStream.dat");

  private static final int TEST_DATA_LEN = 7 * 1024; // 7KB test data
  private static final byte[] TEST_DATA =
      DFSTestUtil.generateSequentialBytes(0, TEST_DATA_LEN);
  private AltFileInputStream altFileInputStream = null;
  private FileInputStream fileInputStream = null;
  private int FLAG = 0;
  @Before
  public void setup() throws IOException {
      FileUtil.fullyDelete(TEST_DIR);
      assertTrue(TEST_DIR.mkdirs());
    // Write a file out
    FileOutputStream fos = new FileOutputStream(TEST_FILE);
    fos.write(TEST_DATA);
    fos.close();
  }

  @Test
  public void read0() throws Exception {
    FLAG = 0;
    altFileInputStream = new AltFileInputStream(TEST_FILE);
    assertNotNull(altFileInputStream.getFD());
    assertNotNull(altFileInputStream.getChannel());
    long numberOfTheFileByte = 0;
    while (altFileInputStream.read() != -1) {
      numberOfTheFileByte++;
    }
    assertEquals(TEST_DATA_LEN,numberOfTheFileByte);
    close();
  }

  @Test
  public void read1() throws Exception {
    FLAG = 1;
    altFileInputStream = new AltFileInputStream(TEST_FILE);
    altFileInputStream = new AltFileInputStream(altFileInputStream.getFD(),altFileInputStream.getChannel());
    assertNotNull(altFileInputStream.getFD());
    assertNotNull(altFileInputStream.getChannel());
    long numberOfTheFileByte = 0;
    while (altFileInputStream.read() != -1) {
      numberOfTheFileByte++;
    }
    assertEquals(TEST_DATA_LEN, numberOfTheFileByte);
    close();
  }

  @Test
  public void read2() throws Exception {
    FLAG = 2;
    fileInputStream = new FileInputStream(TEST_FILE);
    assertNotNull(fileInputStream.getChannel());
    assertNotNull(fileInputStream.getFD());
    long numberOfTheFileByte = 0;
    while (fileInputStream.read() != -1) {
      numberOfTheFileByte++;
    }
    assertEquals(TEST_DATA_LEN, numberOfTheFileByte);
    close();
  }

  public void close() throws Exception {
    if (FLAG == 0) {
      altFileInputStream.close();
    } else if (FLAG == 1){
      altFileInputStream.close();
    } else {
      fileInputStream.close();
    }
    try
    {
      if (FLAG == 0) {
        altFileInputStream.read();
      } else if (FLAG == 1) {
        altFileInputStream.read();
      } else {
        fileInputStream.read();
      }
    } catch (IOException ex) {
      if ( FLAG!=1 ){
        assert (ex.getMessage().equals("Stream Closed"));
      }
      return;
    }
    throw new Exception("close failure");
  }

}