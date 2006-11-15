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

import junit.framework.TestCase;
import java.io.*;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class tests the creation of files with block-size
 * smaller than the default buffer size of 4K.
 * @author Milind Bhandarkar
 */
public class TestSmallBlock extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 1;
  static final int fileSize = 20;

  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    // create and write a file that contains three blocks of data
    FSOutputStream stm = fileSys.createRaw(name, true, (short)1,
        (long)blockSize);
    byte[] buffer = new byte[fileSize];
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
  
  private void checkFile(FileSystem fileSys, Path name) throws IOException {
    String[][] locations = fileSys.getFileCacheHints(name, 0, fileSize);
    assertEquals("Number of blocks", fileSize, locations.length);
    FSInputStream stm = fileSys.openRaw(name);
    byte[] expected = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(expected);
    // do a sanity check. Read the file
    byte[] actual = new byte[fileSize];
    stm.readFully(0, actual);
    checkAndEraseData(actual, 0, expected, "Read Sanity Test");
    stm.close();
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name);
    assertTrue(!fileSys.exists(name));
  }
  
  /**
   * Tests small block size in in DFS.
   */
  public void testSmallBlock() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(65312, conf, 1, false);
    FileSystem fileSys = cluster.getFileSystem();
    try {
      Path file1 = new Path("smallblocktest.dat");
      writeFile(fileSys, file1);
      checkFile(fileSys, file1);
      cleanupFile(fileSys, file1);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
}
