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
package org.apache.hadoop.dfs;

import javax.swing.filechooser.FileSystemView;
import junit.framework.TestCase;
import java.io.*;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class tests the DFS positional read functionality in a single node
 * mini-cluster.
 * @author Milind Bhandarkar
 */
public class TestPread extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 4096;

  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    // create and write a file that contains three blocks of data
    DataOutputStream stm = fileSys.create(name, true, 4096, (short)1,
        (long)blockSize);
    byte[] buffer = new byte[(int)(3*blockSize)];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  private void checkAndEraseData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      this.assertEquals(message+" byte "+(from+idx)+" differs. expected "+
          expected[from+idx]+" actual "+actual[idx],
          actual[idx], expected[from+idx]);
      actual[idx] = 0;
    }
  }
  
  private void doPread(FSDataInputStream stm, long position, byte[] buffer,
      int offset, int length) throws IOException {
    int nread = 0;
    while (nread < length) {
      int nbytes = stm.read(position+nread, buffer, offset+nread, length-nread);
      assertTrue("Error in pread", nbytes > 0);
      nread += nbytes;
    }
  }
  private void pReadFile(FileSystem fileSys, Path name) throws IOException {
    FSDataInputStream stm = fileSys.open(name);
    byte[] expected = new byte[(int)(3*blockSize)];
    Random rand = new Random(seed);
    rand.nextBytes(expected);
    // do a sanity check. Read first 4K bytes
    byte[] actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 0, expected, "Read Sanity Test");
    // now do a pread for the first 8K bytes
    actual = new byte[8192];
    doPread(stm, 0L, actual, 0, 8192);
    checkAndEraseData(actual, 0, expected, "Pread Test 1");
    // Now check to see if the normal read returns 4K-8K byte range
    actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 4096, expected, "Pread Test 2");
    // Now see if we can cross a single block boundary successfully
    // read 4K bytes from blockSize - 2K offset
    stm.readFully(blockSize - 2048, actual, 0, 4096);
    checkAndEraseData(actual, (int)(blockSize-2048), expected, "Pread Test 3");
    // now see if we can cross two block boundaries successfully
    // read blockSize + 4K bytes from blockSize - 2K offset
    actual = new byte[(int)(blockSize+4096)];
    stm.readFully(blockSize - 2048, actual);
    checkAndEraseData(actual, (int)(blockSize-2048), expected, "Pread Test 4");
    // now check that even after all these preads, we can still read
    // bytes 8K-12K
    actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 8192, expected, "Pread Test 5");
    // all done
    stm.close();
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name);
    assertTrue(!fileSys.exists(name));
  }
  
  /**
   * Tests positional read in DFS.
   */
  public void testPreadDFS() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(65312, conf, 3, false);
    FileSystem fileSys = cluster.getFileSystem();
    try {
      Path file1 = new Path("preadtest.dat");
      writeFile(fileSys, file1);
      pReadFile(fileSys, file1);
      cleanupFile(fileSys, file1);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
  
  /**
   * Tests positional read in LocalFS.
   */
  public void testPreadLocalFS() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fileSys = FileSystem.getNamed("local", conf);
    try {
      Path file1 = new Path("build/test/data", "preadtest.dat");
      writeFile(fileSys, file1);
      pReadFile(fileSys, file1);
      cleanupFile(fileSys, file1);
    } finally {
      fileSys.close();
    }
  }
}
