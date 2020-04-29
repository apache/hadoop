/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;


import static org.junit.Assert.assertTrue;

/**
 * Test to make sure df can run and work.
 */
public class TestDFCachingGetSpaceUsed {
  final static private File DF_DIR = GenericTestUtils.getTestDir("testdfspace");
  public static final int FILE_SIZE = 1024;

  @Before
  public void setUp() {
    FileUtil.fullyDelete(DF_DIR);
    assertTrue(DF_DIR.mkdirs());
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(DF_DIR);
  }

  @Test
  public void testCanBuildRun() throws Exception {
    File file = writeFile("testCanBuild");

    GetSpaceUsed instance = new CachingGetSpaceUsed.Builder()
        .setPath(file)
        .setInterval(50060)
        .setKlass(DFCachingGetSpaceUsed.class)
        .build();
    assertTrue(instance instanceof DFCachingGetSpaceUsed);
    assertTrue(instance.getUsed() >= FILE_SIZE - 20);
    ((DFCachingGetSpaceUsed) instance).close();
  }

  private File writeFile(String fileName) throws IOException {
    File f = new File(DF_DIR, fileName);
    assertTrue(f.createNewFile());
    RandomAccessFile randomAccessFile = new RandomAccessFile(f, "rws");
    randomAccessFile.writeUTF(RandomStringUtils.randomAlphabetic(FILE_SIZE));
    randomAccessFile.getFD().sync();
    randomAccessFile.close();
    return f;
  }

}