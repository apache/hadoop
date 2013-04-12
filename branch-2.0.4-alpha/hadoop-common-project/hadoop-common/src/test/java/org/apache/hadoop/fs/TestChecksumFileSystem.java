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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import static org.apache.hadoop.fs.FileSystemTestHelper.*;
import org.apache.hadoop.conf.Configuration;
import org.junit.*;
import static org.junit.Assert.*;

public class TestChecksumFileSystem {
  static final String TEST_ROOT_DIR
    = System.getProperty("test.build.data","build/test/data/work-dir/localfs");

  static LocalFileSystem localFs;

  @Before
  public void resetLocalFs() throws Exception {
    localFs = FileSystem.getLocal(new Configuration());
    localFs.setVerifyChecksum(true);
  }

  @Test
  public void testgetChecksumLength() throws Exception {
    assertEquals(8, ChecksumFileSystem.getChecksumLength(0L, 512));
    assertEquals(12, ChecksumFileSystem.getChecksumLength(1L, 512));
    assertEquals(12, ChecksumFileSystem.getChecksumLength(512L, 512));
    assertEquals(16, ChecksumFileSystem.getChecksumLength(513L, 512));
    assertEquals(16, ChecksumFileSystem.getChecksumLength(1023L, 512));
    assertEquals(16, ChecksumFileSystem.getChecksumLength(1024L, 512));
    assertEquals(408, ChecksumFileSystem.getChecksumLength(100L, 1));
    assertEquals(4000000000008L,
                 ChecksumFileSystem.getChecksumLength(10000000000000L, 10));    
  } 
  
  @Test
  public void testVerifyChecksum() throws Exception {    
    Path testPath = new Path(TEST_ROOT_DIR, "testPath");
    Path testPath11 = new Path(TEST_ROOT_DIR, "testPath11");
    FSDataOutputStream fout = localFs.create(testPath);
    fout.write("testing".getBytes());
    fout.close();
    
    fout = localFs.create(testPath11);
    fout.write("testing you".getBytes());
    fout.close();

    // Exercise some boundary cases - a divisor of the chunk size
    // the chunk size, 2x chunk size, and +/-1 around these.
    readFile(localFs, testPath, 128);
    readFile(localFs, testPath, 511);
    readFile(localFs, testPath, 512);
    readFile(localFs, testPath, 513);
    readFile(localFs, testPath, 1023);
    readFile(localFs, testPath, 1024);
    readFile(localFs, testPath, 1025);

    localFs.delete(localFs.getChecksumFile(testPath), true);
    assertTrue("checksum deleted", !localFs.exists(localFs.getChecksumFile(testPath)));
    
    //copying the wrong checksum file
    FileUtil.copy(localFs, localFs.getChecksumFile(testPath11), localFs, 
        localFs.getChecksumFile(testPath),false,true,localFs.getConf());
    assertTrue("checksum exists", localFs.exists(localFs.getChecksumFile(testPath)));
    
    boolean errorRead = false;
    try {
      readFile(localFs, testPath, 1024);
    }catch(ChecksumException ie) {
      errorRead = true;
    }
    assertTrue("error reading", errorRead);
    
    //now setting verify false, the read should succeed
    localFs.setVerifyChecksum(false);
    String str = readFile(localFs, testPath, 1024).toString();
    assertTrue("read", "testing".equals(str));
  }

  @Test
  public void testMultiChunkFile() throws Exception {
    Path testPath = new Path(TEST_ROOT_DIR, "testMultiChunk");
    FSDataOutputStream fout = localFs.create(testPath);
    for (int i = 0; i < 1000; i++) {
      fout.write(("testing" + i).getBytes());
    }
    fout.close();

    // Exercise some boundary cases - a divisor of the chunk size
    // the chunk size, 2x chunk size, and +/-1 around these.
    readFile(localFs, testPath, 128);
    readFile(localFs, testPath, 511);
    readFile(localFs, testPath, 512);
    readFile(localFs, testPath, 513);
    readFile(localFs, testPath, 1023);
    readFile(localFs, testPath, 1024);
    readFile(localFs, testPath, 1025);
  }

  /**
   * Test to ensure that if the checksum file is truncated, a
   * ChecksumException is thrown
   */
  @Test
  public void testTruncatedChecksum() throws Exception { 
    Path testPath = new Path(TEST_ROOT_DIR, "testtruncatedcrc");
    FSDataOutputStream fout = localFs.create(testPath);
    fout.write("testing truncation".getBytes());
    fout.close();

    // Read in the checksum
    Path checksumFile = localFs.getChecksumFile(testPath);
    FileSystem rawFs = localFs.getRawFileSystem();
    FSDataInputStream checksumStream = rawFs.open(checksumFile);
    byte buf[] = new byte[8192];
    int read = checksumStream.read(buf, 0, buf.length);
    checksumStream.close();

    // Now rewrite the checksum file with the last byte missing
    FSDataOutputStream replaceStream = rawFs.create(checksumFile);
    replaceStream.write(buf, 0, read - 1);
    replaceStream.close();

    // Now reading the file should fail with a ChecksumException
    try {
      readFile(localFs, testPath, 1024);
      fail("Did not throw a ChecksumException when reading truncated " +
           "crc file");
    } catch(ChecksumException ie) {
    }

    // telling it not to verify checksums, should avoid issue.
    localFs.setVerifyChecksum(false);
    String str = readFile(localFs, testPath, 1024).toString();
    assertTrue("read", "testing truncation".equals(str));
  }
  
  @Test
  public void testStreamType() throws Exception {
    Path testPath = new Path(TEST_ROOT_DIR, "testStreamType");
    localFs.create(testPath).close();    
    FSDataInputStream in = null;
    
    localFs.setVerifyChecksum(true);
    in = localFs.open(testPath);
    assertTrue("stream is input checker",
        in.getWrappedStream() instanceof FSInputChecker);
    
    localFs.setVerifyChecksum(false);
    in = localFs.open(testPath);
    assertFalse("stream is not input checker",
        in.getWrappedStream() instanceof FSInputChecker);
  }
  
  @Test
  public void testCorruptedChecksum() throws Exception {
    Path testPath = new Path(TEST_ROOT_DIR, "testCorruptChecksum");
    Path checksumPath = localFs.getChecksumFile(testPath);

    // write a file to generate checksum
    FSDataOutputStream out = localFs.create(testPath, true);
    out.write("testing 1 2 3".getBytes());
    out.close();
    assertTrue(localFs.exists(checksumPath));
    FileStatus stat = localFs.getFileStatus(checksumPath);
    
    // alter file directly so checksum is invalid
    out = localFs.getRawFileSystem().create(testPath, true);
    out.write("testing stale checksum".getBytes());
    out.close();
    assertTrue(localFs.exists(checksumPath));
    // checksum didn't change on disk
    assertEquals(stat, localFs.getFileStatus(checksumPath));

    Exception e = null;
    try {
      localFs.setVerifyChecksum(true);
      readFile(localFs, testPath, 1024);
    } catch (ChecksumException ce) {
      e = ce;
    } finally {
      assertNotNull("got checksum error", e);
    }

    localFs.setVerifyChecksum(false);
    String str = readFile(localFs, testPath, 1024);
    assertEquals("testing stale checksum", str);
  }
  
  @Test
  public void testRenameFileToFile() throws Exception {
    Path srcPath = new Path(TEST_ROOT_DIR, "testRenameSrc");
    Path dstPath = new Path(TEST_ROOT_DIR, "testRenameDst");
    verifyRename(srcPath, dstPath, false);
  }

  @Test
  public void testRenameFileIntoDir() throws Exception {
    Path srcPath = new Path(TEST_ROOT_DIR, "testRenameSrc");
    Path dstPath = new Path(TEST_ROOT_DIR, "testRenameDir");
    localFs.mkdirs(dstPath);
    verifyRename(srcPath, dstPath, true);
  }

  @Test
  public void testRenameFileIntoDirFile() throws Exception {
    Path srcPath = new Path(TEST_ROOT_DIR, "testRenameSrc");
    Path dstPath = new Path(TEST_ROOT_DIR, "testRenameDir/testRenameDst");
    assertTrue(localFs.mkdirs(dstPath));
    verifyRename(srcPath, dstPath, false);
  }


  void verifyRename(Path srcPath, Path dstPath, boolean dstIsDir)
      throws Exception { 
    localFs.delete(srcPath,true);
    localFs.delete(dstPath,true);
    
    Path realDstPath = dstPath;
    if (dstIsDir) {
      localFs.mkdirs(dstPath);
      realDstPath = new Path(dstPath, srcPath.getName());
    }
    
    // ensure file + checksum are moved
    writeFile(localFs, srcPath, 1);
    assertTrue(localFs.exists(localFs.getChecksumFile(srcPath)));
    assertTrue(localFs.rename(srcPath, dstPath));
    assertTrue(localFs.exists(localFs.getChecksumFile(realDstPath)));

    // create a file with no checksum, rename, ensure dst checksum is removed    
    writeFile(localFs.getRawFileSystem(), srcPath, 1);
    assertFalse(localFs.exists(localFs.getChecksumFile(srcPath)));
    assertTrue(localFs.rename(srcPath, dstPath));
    assertFalse(localFs.exists(localFs.getChecksumFile(realDstPath)));
    
    // create file with checksum, rename over prior dst with no checksum
    writeFile(localFs, srcPath, 1);
    assertTrue(localFs.exists(localFs.getChecksumFile(srcPath)));
    assertTrue(localFs.rename(srcPath, dstPath));
    assertTrue(localFs.exists(localFs.getChecksumFile(realDstPath)));
  }
}
