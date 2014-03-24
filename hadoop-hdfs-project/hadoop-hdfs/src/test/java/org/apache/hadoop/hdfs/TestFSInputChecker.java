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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.Test;

/**
 * This class tests if FSInputChecker works correctly.
 */
public class TestFSInputChecker {
  static final long seed = 0xDEADBEEFL;
  static final int BYTES_PER_SUM = 10;
  static final int BLOCK_SIZE = 2*BYTES_PER_SUM;
  static final int HALF_CHUNK_SIZE = BYTES_PER_SUM/2;
  static final int FILE_SIZE = 2*BLOCK_SIZE-1;
  static final short NUM_OF_DATANODES = 2;
  final byte[] expected = new byte[FILE_SIZE];
  byte[] actual;
  FSDataInputStream stm;
  final Random rand = new Random(seed);

  /* create a file */
  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, new FsPermission((short)0777),
        true, fileSys.getConf().getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        NUM_OF_DATANODES, BLOCK_SIZE, null);
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
    IOUtils.skipFully(stm, skippedBytes);
    long newPos = oldPos + skippedBytes;
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
    testSkip1(HALF_CHUNK_SIZE + 1);
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
    IOUtils.skipFully(stm, FILE_SIZE);
    try {
      IOUtils.skipFully(stm, 10);
      fail("expected to get a PrematureEOFException");
    } catch (EOFException e) {
      assertEquals(e.getMessage(), "Premature EOF from inputStream " +
          "after skipping 0 byte(s).");
    }
    
    stm.seek(0);
    try {
      IOUtils.skipFully(stm, FILE_SIZE + 10);
      fail("expected to get a PrematureEOFException");
    } catch (EOFException e) {
      assertEquals(e.getMessage(), "Premature EOF from inputStream " +
          "after skipping " + FILE_SIZE + " byte(s).");
    }
    stm.seek(10);
    try {
      IOUtils.skipFully(stm, FILE_SIZE);
      fail("expected to get a PrematureEOFException");
    } catch (EOFException e) {
      assertEquals(e.getMessage(), "Premature EOF from inputStream " +
          "after skipping " + (FILE_SIZE - 10) + " byte(s).");
    }
  }

  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }
  
  /**
   * Tests read/seek/getPos/skipped opeation for input stream.
   */
  private void testChecker(FileSystem fileSys, boolean readCS)
  throws Exception {
    Path file = new Path("try.dat");
    writeFile(fileSys, file);

    try {
      if (!readCS) {
        fileSys.setVerifyChecksum(false);
      }

      stm = fileSys.open(file);
      checkReadAndGetPos();
      checkSeek();
      checkSkip();
      //checkMark
      assertFalse(stm.markSupported());
      stm.close();
    } finally {
      if (!readCS) {
        fileSys.setVerifyChecksum(true);
      }
      cleanupFile(fileSys, file);
    }
  }
  
  private void testFileCorruption(LocalFileSystem fileSys) throws IOException {
    // create a file and verify that checksum corruption results in 
    // a checksum exception on LocalFS
    
    String dir = PathUtils.getTestDirName(getClass());
    Path file = new Path(dir + "/corruption-test.dat");
    Path crcFile = new Path(dir + "/.corruption-test.dat.crc");
    
    writeFile(fileSys, file);
    
    int fileLen = (int)fileSys.getFileStatus(file).getLen();
    
    byte [] buf = new byte[fileLen];

    InputStream in = fileSys.open(file);
    IOUtils.readFully(in, buf, 0, buf.length);
    in.close();
    
    // check .crc corruption
    checkFileCorruption(fileSys, file, crcFile);
    fileSys.delete(file, true);
    
    writeFile(fileSys, file);
    
    // check data corrutpion
    checkFileCorruption(fileSys, file, file);
    
    fileSys.delete(file, true);
  }
  
  private void checkFileCorruption(LocalFileSystem fileSys, Path file, 
                                   Path fileToCorrupt) throws IOException {
    
    // corrupt the file 
    RandomAccessFile out = 
      new RandomAccessFile(new File(fileToCorrupt.toString()), "rw");
    
    byte[] buf = new byte[(int)fileSys.getFileStatus(file).getLen()];    
    int corruptFileLen = (int)fileSys.getFileStatus(fileToCorrupt).getLen();
    assertTrue(buf.length >= corruptFileLen);
    
    rand.nextBytes(buf);
    out.seek(corruptFileLen/2);
    out.write(buf, 0, corruptFileLen/4);
    out.close();

    boolean gotException = false;
    
    InputStream in = fileSys.open(file);
    try {
      IOUtils.readFully(in, buf, 0, buf.length);
    } catch (ChecksumException e) {
      gotException = true;
    }
    assertTrue(gotException);
    in.close();    
  }
  
  @Test
  public void testFSInputChecker() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BYTES_PER_SUM);
    rand.nextBytes(expected);

    // test DFS
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fileSys = cluster.getFileSystem();
    try {
      testChecker(fileSys, true);
      testChecker(fileSys, false);
      testSeekAndRead(fileSys);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
    
    
    // test Local FS
    fileSys = FileSystem.getLocal(conf);
    try {
      testChecker(fileSys, true);
      testChecker(fileSys, false);
      testFileCorruption((LocalFileSystem)fileSys);
      testSeekAndRead(fileSys);
    }finally {
      fileSys.close();
    }
  }

  private void testSeekAndRead(FileSystem fileSys)
  throws IOException {
    Path file = new Path("try.dat");
    writeFile(fileSys, file);
    stm = fileSys.open(
        file,
        fileSys.getConf().getInt(
            CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096));
    checkSeekAndRead();
    stm.close();
    cleanupFile(fileSys, file);
  }

  private void checkSeekAndRead() throws IOException {
    int position = 1;
    int len = 2 * BYTES_PER_SUM - position;
    readAndCompare(stm, position, len);

    position = BYTES_PER_SUM;
    len = BYTES_PER_SUM;
    readAndCompare(stm, position, len);
  }

  private void readAndCompare(FSDataInputStream in, int position, int len)
      throws IOException {
    byte[] b = new byte[len];
    in.seek(position);
    IOUtils.readFully(in, b, 0, b.length);

    for (int i = 0; i < b.length; i++) {
      assertEquals(expected[position + i], b[i]);
    }
  }
}
