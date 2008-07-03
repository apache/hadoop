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
package org.apache.hadoop.hdfs;

import junit.framework.TestCase;
import java.io.*;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class tests if FSInputChecker works correctly.
 */
public class TestFSInputChecker extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int BYTES_PER_SUM = 10;
  static final int BLOCK_SIZE = 2*BYTES_PER_SUM;
  static final int HALF_CHUNK_SIZE = BYTES_PER_SUM/2;
  static final int FILE_SIZE = 2*BLOCK_SIZE-1;
  static final short NUM_OF_DATANODES = 2;
  byte[] expected = new byte[FILE_SIZE];
  byte[] actual;
  FSDataInputStream stm;

  /* create a file */
  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, 
                     fileSys.getConf().getInt("io.file.buffer.size", 4096),
                     NUM_OF_DATANODES, BLOCK_SIZE);
    stm.write(expected);
    stm.close();
  }
  
  /*validate data*/
  private void checkAndEraseData(byte[] actual, int from, byte[] expected, 
      String message) throws Exception {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                        expected[from+idx]+" actual "+actual[idx],
                        actual[idx], expected[from+idx]);
      actual[idx] = 0;
    }
  }
  
  /* test read and getPos */
  private void checkReadAndGetPos() throws Exception {
    actual = new byte[FILE_SIZE];
    // test reads that do not cross checksum boundary
    stm.seek(0);
    int offset;
    for(offset=0; offset<BLOCK_SIZE+BYTES_PER_SUM;
                  offset += BYTES_PER_SUM ) {
      assertEquals(stm.getPos(), offset);
      stm.readFully(actual, offset, BYTES_PER_SUM);
    }
    stm.readFully(actual, offset, FILE_SIZE-BLOCK_SIZE-BYTES_PER_SUM);
    assertEquals(stm.getPos(), FILE_SIZE);
    checkAndEraseData(actual, 0, expected, "Read Sanity Test");
    
    // test reads that cross checksum boundary
    stm.seek(0L);
    assertEquals(stm.getPos(), 0L);
    stm.readFully(actual, 0, HALF_CHUNK_SIZE);
    assertEquals(stm.getPos(), HALF_CHUNK_SIZE);
    stm.readFully(actual, HALF_CHUNK_SIZE, BLOCK_SIZE-HALF_CHUNK_SIZE);
    assertEquals(stm.getPos(), BLOCK_SIZE);
    stm.readFully(actual, BLOCK_SIZE, BYTES_PER_SUM+HALF_CHUNK_SIZE);
    assertEquals(stm.getPos(), BLOCK_SIZE+BYTES_PER_SUM+HALF_CHUNK_SIZE);
    stm.readFully(actual, 2*BLOCK_SIZE-HALF_CHUNK_SIZE, 
        FILE_SIZE-(2*BLOCK_SIZE-HALF_CHUNK_SIZE));
    assertEquals(stm.getPos(), FILE_SIZE);
    checkAndEraseData(actual, 0, expected, "Read Sanity Test");
    
    // test read that cross block boundary
    stm.seek(0L);
    stm.readFully(actual, 0, BYTES_PER_SUM+HALF_CHUNK_SIZE);
    assertEquals(stm.getPos(), BYTES_PER_SUM+HALF_CHUNK_SIZE);
    stm.readFully(actual, BYTES_PER_SUM+HALF_CHUNK_SIZE, BYTES_PER_SUM);
    assertEquals(stm.getPos(), BLOCK_SIZE+HALF_CHUNK_SIZE);
    stm.readFully(actual, BLOCK_SIZE+HALF_CHUNK_SIZE,
        FILE_SIZE-BLOCK_SIZE-HALF_CHUNK_SIZE);
    assertEquals(stm.getPos(), FILE_SIZE);
    checkAndEraseData(actual, 0, expected, "Read Sanity Test");
  }
  
  /* test if one seek is correct */
  private void testSeek1(int offset) 
  throws Exception {
    stm.seek(offset);
    assertEquals(offset, stm.getPos());
    stm.readFully(actual);
    checkAndEraseData(actual, offset, expected, "Read Sanity Test");
  }

  /* test seek() */
  private void checkSeek( ) throws Exception {
    actual = new byte[HALF_CHUNK_SIZE];
    
    // test seeks to checksum boundary
    testSeek1(0);
    testSeek1(BYTES_PER_SUM);
    testSeek1(BLOCK_SIZE);
    
    // test seek to non-checksum-boundary pos
    testSeek1(BLOCK_SIZE+HALF_CHUNK_SIZE);
    testSeek1(HALF_CHUNK_SIZE);
    
    // test seek to a position at the same checksum chunk
    testSeek1(HALF_CHUNK_SIZE/2);
    testSeek1(HALF_CHUNK_SIZE*3/2);
    
    // test end of file
    actual = new byte[1];
    testSeek1(FILE_SIZE-1);
    
    String errMsg = null;
    try {
      stm.seek(FILE_SIZE);
    } catch (IOException e) {
      errMsg = e.getMessage();
    }
    assertTrue(errMsg==null);
  }

  /* test if one skip is correct */
  private void testSkip1(int skippedBytes) 
  throws Exception {
    long oldPos = stm.getPos();
    long nSkipped = stm.skip(skippedBytes);
    long newPos = oldPos+nSkipped;
    assertEquals(stm.getPos(), newPos);
    stm.readFully(actual);
    checkAndEraseData(actual, (int)newPos, expected, "Read Sanity Test");
  }

  /* test skip() */
  private void checkSkip( ) throws Exception {
    actual = new byte[HALF_CHUNK_SIZE];
    
    // test skip to a checksum boundary
    stm.seek(0);
    testSkip1(BYTES_PER_SUM);
    testSkip1(HALF_CHUNK_SIZE);
    testSkip1(HALF_CHUNK_SIZE);
    
    // test skip to non-checksum-boundary pos
    stm.seek(0);
    testSkip1(HALF_CHUNK_SIZE+1);
    testSkip1(BYTES_PER_SUM);
    testSkip1(HALF_CHUNK_SIZE);
    
    // test skip to a position at the same checksum chunk
    stm.seek(0);
    testSkip1(1);
    testSkip1(1);
    
    // test skip to end of file
    stm.seek(0);
    actual = new byte[1];
    testSkip1(FILE_SIZE-1);
    
    stm.seek(0);
    assertEquals(stm.skip(FILE_SIZE), FILE_SIZE);
    assertEquals(stm.skip(10), 0);
    
    stm.seek(0);
    assertEquals(stm.skip(FILE_SIZE+10), FILE_SIZE);
    stm.seek(10);
    assertEquals(stm.skip(FILE_SIZE), FILE_SIZE-10);
  }

  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }
  
  /**
   * Tests read/seek/getPos/skipped opeation for input stream.
   */
  private void testChecker(ChecksumFileSystem fileSys, boolean readCS)
  throws Exception {
    Path file = new Path("try.dat");
    if( readCS ) {
      writeFile(fileSys, file);
    } else {
      writeFile(fileSys.getRawFileSystem(), file);
    }
    stm = fileSys.open(file);
    checkReadAndGetPos();
    checkSeek();
    checkSkip();
    //checkMark
    assertFalse(stm.markSupported());
    stm.close();
    cleanupFile(fileSys, file);
  }
  
  public void testFSInputChecker() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    conf.setInt("io.bytes.per.checksum", BYTES_PER_SUM);
    conf.set("fs.hdfs.impl",
             "org.apache.hadoop.hdfs.ChecksumDistributedFileSystem");
    Random rand = new Random(seed);
    rand.nextBytes(expected);

    // test DFS
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    ChecksumFileSystem fileSys = (ChecksumFileSystem)cluster.getFileSystem();
    try {
      testChecker(fileSys, true);
      testChecker(fileSys, false);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
    
    
    // test Local FS
    fileSys = FileSystem.getLocal(conf);
    try {
      testChecker(fileSys, true);
      testChecker(fileSys, false);
    }finally {
      fileSys.close();
    }
  }
}
