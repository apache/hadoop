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
package org.apache.hadoop.fs;

import org.apache.hadoop.util.Shell;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.test.GenericTestUtils;

/** This test makes sure that "DU" does not get to run on each call to getUsed */
public class TestDU {
  final static private File DU_DIR = GenericTestUtils.getTestDir("dutmp");

  @Before
  public void setUp() {
    assumeFalse(Shell.WINDOWS);
    FileUtil.fullyDelete(DU_DIR);
    assertTrue(DU_DIR.mkdirs());
  }

  @After
  public void tearDown() throws IOException {
      FileUtil.fullyDelete(DU_DIR);
  }

  private void createFile(File newFile, int size) throws IOException {
    // write random data so that filesystems with compression enabled (e.g., ZFS)
    // can't compress the file
    Random random = new Random();
    byte[] data = new byte[size];
    random.nextBytes(data);

    newFile.createNewFile();
    RandomAccessFile file = new RandomAccessFile(newFile, "rws");

    file.write(data);

    file.getFD().sync();
    file.close();
  }

  /**
   * Verify that du returns expected used space for a file.
   * We assume here that if a file system crates a file of size
   * that is a multiple of the block size in this file system,
   * then the used size for the file will be exactly that size.
   * This is true for most file systems.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testDU() throws IOException, InterruptedException {
    final int writtenSize = 32*1024;   // writing 32K
    // Allow for extra 4K on-disk slack for local file systems
    // that may store additional file metadata (eg ext attrs).
    final int slack = 4*1024;
    File file = new File(DU_DIR, "data");
    createFile(file, writtenSize);

    Thread.sleep(5000); // let the metadata updater catch up

    DU du = new DU(file, 10000, 0, -1);
    du.init();
    long duSize = du.getUsed();
    du.close();

    assertTrue("Invalid on-disk size",
        duSize >= writtenSize &&
        writtenSize <= (duSize + slack));

    //test with 0 interval, will not launch thread
    du = new DU(file, 0, 1, -1);
    du.init();
    duSize = du.getUsed();
    du.close();

    assertTrue("Invalid on-disk size",
        duSize >= writtenSize &&
        writtenSize <= (duSize + slack));

    //test without launching thread
    du = new DU(file, 10000, 0, -1);
    du.init();
    duSize = du.getUsed();

    assertTrue("Invalid on-disk size",
        duSize >= writtenSize &&
        writtenSize <= (duSize + slack));
  }

  @Test
  public void testDUGetUsedWillNotReturnNegative() throws IOException {
    File file = new File(DU_DIR, "data");
    assertTrue(file.createNewFile());
    Configuration conf = new Configuration();
    conf.setLong(CommonConfigurationKeys.FS_DU_INTERVAL_KEY, 10000L);
    DU du = new DU(file, 10000L, 0, -1);
    du.incDfsUsed(-Long.MAX_VALUE);
    long duSize = du.getUsed();
    assertTrue(String.valueOf(duSize), duSize >= 0L);
  }

  @Test
  public void testDUSetInitialValue() throws IOException {
    File file = new File(DU_DIR, "dataX");
    createFile(file, 8192);
    DU du = new DU(file, 3000, 0, 1024);
    du.init();
    assertTrue("Initial usage setting not honored", du.getUsed() == 1024);

    // wait until the first du runs.
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ie) {}

    assertTrue("Usage didn't get updated", du.getUsed() == 8192);
  }



}
