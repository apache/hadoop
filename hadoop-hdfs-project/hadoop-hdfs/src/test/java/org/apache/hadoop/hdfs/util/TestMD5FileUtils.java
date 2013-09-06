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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.test.PathUtils;
import org.junit.Before;
import org.junit.Test;

public class TestMD5FileUtils {
  private static final File TEST_DIR = PathUtils.getTestDir(TestMD5FileUtils.class);
  private static final File TEST_FILE = new File(TEST_DIR,
      "testMd5File.dat");
  
  private static final int TEST_DATA_LEN = 128 * 1024; // 128KB test data
  private static final byte[] TEST_DATA =
    DFSTestUtil.generateSequentialBytes(0, TEST_DATA_LEN);
  private static final MD5Hash TEST_MD5 = MD5Hash.digest(TEST_DATA);
  
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
  public void testComputeMd5ForFile() throws Exception {
    MD5Hash computedDigest = MD5FileUtils.computeMd5ForFile(TEST_FILE);
    assertEquals(TEST_MD5, computedDigest);    
  }

  @Test
  public void testVerifyMD5FileGood() throws Exception {
    MD5FileUtils.saveMD5File(TEST_FILE, TEST_MD5);
    MD5FileUtils.verifySavedMD5(TEST_FILE, TEST_MD5);
  }

  /**
   * Test when .md5 file does not exist at all
   */
  @Test(expected=IOException.class)
  public void testVerifyMD5FileMissing() throws Exception {
    MD5FileUtils.verifySavedMD5(TEST_FILE, TEST_MD5);
  }

  /**
   * Test when .md5 file exists but incorrect checksum
   */
  @Test
  public void testVerifyMD5FileBadDigest() throws Exception {
    MD5FileUtils.saveMD5File(TEST_FILE, MD5Hash.digest(new byte[0]));
    try {
      MD5FileUtils.verifySavedMD5(TEST_FILE, TEST_MD5);
      fail("Did not throw");
    } catch (IOException ioe) {
      // Expected
    }
  }
  
  /**
   * Test when .md5 file exists but has a bad format
   */
  @Test
  public void testVerifyMD5FileBadFormat() throws Exception {
    FileWriter writer = new FileWriter(MD5FileUtils.getDigestFileForFile(TEST_FILE));
    try {
      writer.write("this is not an md5 file");
    } finally {
      writer.close();
    }
    
    try {
      MD5FileUtils.verifySavedMD5(TEST_FILE, TEST_MD5);
      fail("Did not throw");
    } catch (IOException ioe) {
      // expected
    }
  }  
}
