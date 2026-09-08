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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CompositeCrcFileChecksum;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Options.ChecksumCombineMode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestGetFileChecksum {
  private static final int BLOCKSIZE = 1024;
  private static final short REPLICATION = 3;

  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  public void testGetFileChecksum(final Path foo, final int appendLength)
      throws Exception {
    final int appendRounds = 16;
    FileChecksum[] fc = new FileChecksum[appendRounds + 1];
    DFSTestUtil.createFile(dfs, foo, appendLength, REPLICATION, 0L);
    fc[0] = dfs.getFileChecksum(foo);
    for (int i = 0; i < appendRounds; i++) {
      DFSTestUtil.appendFile(dfs, foo, appendLength);
      fc[i + 1] = dfs.getFileChecksum(foo);
    }

    for (int i = 0; i < appendRounds + 1; i++) {
      FileChecksum checksum = dfs.getFileChecksum(foo, appendLength * (i+1));
      assertTrue(checksum.equals(fc[i]));
    }
  }

  @Test
  public void testGetFileChecksumForBlocksUnderConstruction() {
    try {
      FSDataOutputStream file = dfs.create(new Path("/testFile"));
      file.write("Performance Testing".getBytes());
      dfs.getFileChecksum(new Path("/testFile"));
      fail("getFileChecksum should fail for files "
          + "with blocks under construction");
    } catch (IOException ie) {
      assertTrue(ie.getMessage()
          .contains("Fail to get checksum, since file /testFile "
              + "is under construction."));
    }
  }
  @Test
  public void testGetFileChecksum() throws Exception {
    testGetFileChecksum(new Path("/foo"), BLOCKSIZE / 4);
    testGetFileChecksum(new Path("/bar"), BLOCKSIZE / 4 - 1);
  }

  /**
   * HDFS-17803: Verify that getFileChecksum() on an empty file returns the
   * correct checksum type matching the configured combine mode.
   * Previously COMPOSITE_CRC mode always returned MD5MD5CRC for empty files.
   */
  @Test
  @Timeout(60)
  public void testEmptyFileChecksumType() throws Exception {
    // --- COMPOSITE_CRC mode ---
    Configuration crcConf = new Configuration(conf);
    crcConf.set(HdfsClientConfigKeys.DFS_CHECKSUM_COMBINE_MODE_KEY,
        ChecksumCombineMode.COMPOSITE_CRC.name());
    try (MiniDFSCluster crcCluster = new MiniDFSCluster.Builder(crcConf)
        .numDataNodes(REPLICATION).build()) {
      crcCluster.waitActive();
      DistributedFileSystem crcDfs = crcCluster.getFileSystem();
      Path emptyFile = new Path("/emptyCompositeCrc");
      crcDfs.create(emptyFile).close();
      FileChecksum checksum = crcDfs.getFileChecksum(emptyFile);
      assertEquals(CompositeCrcFileChecksum.class, checksum.getClass(),
          "Expected CompositeCrcFileChecksum for empty file in COMPOSITE_CRC mode, got: "
              + checksum.getClass().getSimpleName());
    }

    // --- MD5MD5CRC mode (legacy) ---
    Configuration md5Conf = new Configuration(conf);
    md5Conf.set(HdfsClientConfigKeys.DFS_CHECKSUM_COMBINE_MODE_KEY,
        ChecksumCombineMode.MD5MD5CRC.name());
    try (MiniDFSCluster md5Cluster = new MiniDFSCluster.Builder(md5Conf)
        .numDataNodes(REPLICATION).build()) {
      md5Cluster.waitActive();
      DistributedFileSystem md5Dfs = md5Cluster.getFileSystem();
      Path emptyFile = new Path("/emptyMd5");
      md5Dfs.create(emptyFile).close();
      FileChecksum checksum = md5Dfs.getFileChecksum(emptyFile);
      assertTrue(checksum instanceof MD5MD5CRC32FileChecksum,
          "Expected MD5MD5CRC32FileChecksum for empty file in MD5MD5CRC mode, got: "
              + checksum.getClass().getSimpleName());
    }
  }
}
