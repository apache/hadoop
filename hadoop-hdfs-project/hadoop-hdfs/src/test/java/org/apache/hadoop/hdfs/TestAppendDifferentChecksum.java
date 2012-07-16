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

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test cases for trying to append to a file with a different
 * checksum than the file was originally written with.
 */
public class TestAppendDifferentChecksum {
  private static final int SEGMENT_LENGTH = 1500;

  // run the randomized test for 5 seconds
  private static final long RANDOM_TEST_RUNTIME = 5000;
  private static MiniDFSCluster cluster;
  private static FileSystem fs; 
  

  @BeforeClass
  public static void setupCluster() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 4096);
    conf.set("fs.hdfs.impl.disable.cache", "true");
    cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(1)
      .build();
    fs = cluster.getFileSystem();
  }
  
  @AfterClass
  public static void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * This test does not run, since switching chunksize with append
   * is not implemented. Please see HDFS-2130 for a discussion of the
   * difficulties in doing so.
   */
  @Test
  @Ignore("this is not implemented! See HDFS-2130")
  public void testSwitchChunkSize() throws IOException {
    FileSystem fsWithSmallChunk = createFsWithChecksum("CRC32", 512);
    FileSystem fsWithBigChunk = createFsWithChecksum("CRC32", 1024);
    Path p = new Path("/testSwitchChunkSize");
    appendWithTwoFs(p, fsWithSmallChunk, fsWithBigChunk);
    AppendTestUtil.check(fsWithSmallChunk, p, SEGMENT_LENGTH * 2);
    AppendTestUtil.check(fsWithBigChunk, p, SEGMENT_LENGTH * 2);
  }
  
  /**
   * Simple unit test which writes some data with one algorithm,
   * then appends with another.
   */
  @Test
  public void testSwitchAlgorithms() throws IOException {
    FileSystem fsWithCrc32 = createFsWithChecksum("CRC32", 512);
    FileSystem fsWithCrc32C = createFsWithChecksum("CRC32C", 512);
    
    Path p = new Path("/testSwitchAlgorithms");
    appendWithTwoFs(p, fsWithCrc32, fsWithCrc32C);
    // Regardless of which FS is used to read, it should pick up
    // the on-disk checksum!
    AppendTestUtil.check(fsWithCrc32C, p, SEGMENT_LENGTH * 2);
    AppendTestUtil.check(fsWithCrc32, p, SEGMENT_LENGTH * 2);
  }
  
  /**
   * Test which randomly alternates between appending with
   * CRC32 and with CRC32C, crossing several block boundaries.
   * Then, checks that all of the data can be read back correct.
   */
  @Test(timeout=RANDOM_TEST_RUNTIME*2)
  public void testAlgoSwitchRandomized() throws IOException {
    FileSystem fsWithCrc32 = createFsWithChecksum("CRC32", 512);
    FileSystem fsWithCrc32C = createFsWithChecksum("CRC32C", 512);

    Path p = new Path("/testAlgoSwitchRandomized");
    long seed = Time.now();
    System.out.println("seed: " + seed);
    Random r = new Random(seed);
    
    // Create empty to start
    IOUtils.closeStream(fsWithCrc32.create(p));
    
    long st = Time.now();
    int len = 0;
    while (Time.now() - st < RANDOM_TEST_RUNTIME) {
      int thisLen = r.nextInt(500);
      FileSystem fs = (r.nextBoolean() ? fsWithCrc32 : fsWithCrc32C);
      FSDataOutputStream stm = fs.append(p);
      try {
        AppendTestUtil.write(stm, len, thisLen);
      } finally {
        stm.close();
      }
      len += thisLen;
    }
    
    AppendTestUtil.check(fsWithCrc32, p, len);
    AppendTestUtil.check(fsWithCrc32C, p, len);
  }
  
  private FileSystem createFsWithChecksum(String type, int bytes)
      throws IOException {
    Configuration conf = new Configuration(fs.getConf());
    conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, type);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, bytes);
    return FileSystem.get(conf);
  }


  private void appendWithTwoFs(Path p, FileSystem fs1, FileSystem fs2)
      throws IOException {
    FSDataOutputStream stm = fs1.create(p);
    try {
      AppendTestUtil.write(stm, 0, SEGMENT_LENGTH);
    } finally {
      stm.close();
    }
    
    stm = fs2.append(p);
    try {
      AppendTestUtil.write(stm, SEGMENT_LENGTH, SEGMENT_LENGTH);
    } finally {
      stm.close();
    }    
  }

}
