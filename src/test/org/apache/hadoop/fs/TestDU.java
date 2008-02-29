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

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

/** This test makes sure that "DU" does not get to run on each call to getUsed */ 
public class TestDU extends TestCase {
  final static private File DU_DIR = new File(
      System.getProperty("test.build.data","/tmp"), "dutmp");

  public void setUp() throws IOException {
      FileUtil.fullyDelete(DU_DIR);
      assertTrue(DU_DIR.mkdirs());
  }

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
  
  /*
   * Find a number that is a multiple of the block size in this file system
   */
  private int getBlockSize() throws IOException, InterruptedException {
    File file = new File(DU_DIR, "small");
    createFile(file, 128); // this is an arbitrary number. It has to be big enough for the filesystem to report
                           // any usage at all. For instance, NFS reports 0 blocks if the file is <= 64 bytes

    Thread.sleep(5000); // let the metadata updater catch up

    DU du = new DU(file, 0);
    return (int) du.getUsed();
  }

  public void testDU() throws IOException, InterruptedException {
    int blockSize = getBlockSize();

    File file = new File(DU_DIR, "data");
    createFile(file, 2 * blockSize);

    Thread.sleep(5000); // let the metadata updater catch up
    
    DU du = new DU(file, 0);
    long size = du.getUsed();

    assertEquals(2 * blockSize, size);
  }
}
