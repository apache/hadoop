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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.FileDescriptor;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator;
import org.apache.hadoop.io.nativeio.NativeIOException;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCachingStrategy {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCachingStrategy.class);
  private static final int MAX_TEST_FILE_LEN = 1024 * 1024;
  private static final int WRITE_PACKET_SIZE = HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;

  private final static TestRecordingCacheTracker tracker =
      new TestRecordingCacheTracker();

  @BeforeClass
  public static void setupTest() {
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);

    // Track calls to posix_fadvise.
    NativeIO.POSIX.setCacheManipulator(tracker);
    
    // Normally, we wait for a few megabytes of data to be read or written 
    // before dropping the cache.  This is to avoid an excessive number of
    // JNI calls to the posix_fadvise function.  However, for the purpose
    // of this test, we want to use small files and see all fadvise calls
    // happen.
    BlockSender.CACHE_DROP_INTERVAL_BYTES = 4096;
    BlockReceiver.CACHE_DROP_LAG_BYTES = 4096;
  }

  private static class Stats {
    private final String fileName;
    private final boolean dropped[] = new boolean[MAX_TEST_FILE_LEN];

    Stats(String fileName) {
      this.fileName = fileName;
    }
    
    synchronized void fadvise(int offset, int len, int flags) {
      LOG.debug("got fadvise(offset={}, len={}, flags={})", offset, len, flags);
      if (flags == POSIX_FADV_DONTNEED) {
        for (int i = 0; i < len; i++) {
          dropped[(offset + i)] = true;
        }
      }
    }

    synchronized void assertNotDroppedInRange(int start, int end) {
      for (int i = start; i < end; i++) {
        if (dropped[i]) {
          throw new RuntimeException("in file " + fileName + ", we " +
              "dropped the cache at offset " + i);
        }
      }
    }
    
    synchronized void assertDroppedInRange(int start, int end) {
      for (int i = start; i < end; i++) {
        if (!dropped[i]) {
          throw new RuntimeException("in file " + fileName + ", we " +
              "did not drop the cache at offset " + i);
        }
      }
    }
    
    synchronized void clear() {
      Arrays.fill(dropped, false);
    }
  }

  private static class TestRecordingCacheTracker extends CacheManipulator {
    private final Map<String, Stats> map = new TreeMap<>();

    @Override
    public void posixFadviseIfPossible(String name,
      FileDescriptor fd, long offset, long len, int flags)
          throws NativeIOException {
      if ((len < 0) || (len > Integer.MAX_VALUE)) {
        throw new RuntimeException("invalid length of " + len +
            " passed to posixFadviseIfPossible");
      }
      if ((offset < 0) || (offset > Integer.MAX_VALUE)) {
        throw new RuntimeException("invalid offset of " + offset +
            " passed to posixFadviseIfPossible");
      }
      Stats stats = map.get(name);
      if (stats == null) {
        stats = new Stats(name);
        map.put(name, stats);
      }
      stats.fadvise((int)offset, (int)len, flags);
      super.posixFadviseIfPossible(name, fd, offset, len, flags);
    }

    synchronized void clear() {
      map.clear();
    }
    
    synchronized Stats getStats(String fileName) {
      return map.get(fileName);
    }
    
    synchronized public String toString() {
      StringBuilder bld = new StringBuilder();
      bld.append("TestRecordingCacheManipulator{");
      String prefix = "";
      for (String fileName : map.keySet()) {
        bld.append(prefix);
        prefix = ", ";
        bld.append(fileName);
      }
      bld.append("}");
      return bld.toString();
    }
  }

  static void createHdfsFile(FileSystem fs, Path p, long length,
        Boolean dropBehind) throws Exception {
    FSDataOutputStream fos = null;
    try {
      // create file with replication factor of 1
      fos = fs.create(p, (short)1);
      if (dropBehind != null) {
        fos.setDropBehind(dropBehind);
      }
      byte buf[] = new byte[8196];
      while (length > 0) {
        int amt = (length > buf.length) ? buf.length : (int)length;
        fos.write(buf, 0, amt);
        length -= amt;
      }
    } catch (IOException e) {
      LOG.error("ioexception", e);
    } finally {
      if (fos != null) {
        fos.close();
      }
    }
  }
  
  static long readHdfsFile(FileSystem fs, Path p, long length,
      Boolean dropBehind) throws Exception {
    FSDataInputStream fis = null;
    long totalRead = 0;
    try {
      fis = fs.open(p);
      if (dropBehind != null) {
        fis.setDropBehind(dropBehind);
      }
      byte buf[] = new byte[8196];
      while (length > 0) {
        int amt = (length > buf.length) ? buf.length : (int)length;
        int ret = fis.read(buf, 0, amt);
        if (ret == -1) {
          return totalRead;
        }
        totalRead += ret;
        length -= ret;
      }
    } catch (IOException e) {
      LOG.error("ioexception", e);
    } finally {
      if (fis != null) {
        fis.close();
      }
    }
    throw new RuntimeException("unreachable");
  }
 
  @Test(timeout=120000)
  public void testFadviseAfterWriteThenRead() throws Exception {
    // start a cluster
    LOG.info("testFadviseAfterWriteThenRead");
    tracker.clear();
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    String TEST_PATH = "/test";
    int TEST_PATH_LEN = MAX_TEST_FILE_LEN;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
          .build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      // create new file
      createHdfsFile(fs, new Path(TEST_PATH), TEST_PATH_LEN, true);
      // verify that we dropped everything from the cache during file creation.
      ExtendedBlock block = cluster.getNameNode().getRpcServer().getBlockLocations(
          TEST_PATH, 0, Long.MAX_VALUE).get(0).getBlock();
      String fadvisedFileName = cluster.getBlockFile(0, block).getName();
      Stats stats = tracker.getStats(fadvisedFileName);
      stats.assertDroppedInRange(0, TEST_PATH_LEN - WRITE_PACKET_SIZE);
      stats.clear();
      
      // read file
      readHdfsFile(fs, new Path(TEST_PATH), Long.MAX_VALUE, true);
      // verify that we dropped everything from the cache.
      Assert.assertNotNull(stats);
      stats.assertDroppedInRange(0, TEST_PATH_LEN - WRITE_PACKET_SIZE);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /***
   * Test the scenario where the DataNode defaults to not dropping the cache,
   * but our client defaults are set.
   */
  @Test(timeout=120000)
  public void testClientDefaults() throws Exception {
    // start a cluster
    LOG.info("testClientDefaults");
    tracker.clear();
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_READS_KEY, false);
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_KEY, false);
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS, true);
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_WRITES, true);
    MiniDFSCluster cluster = null;
    String TEST_PATH = "/test";
    int TEST_PATH_LEN = MAX_TEST_FILE_LEN;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
          .build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      
      // create new file
      createHdfsFile(fs, new Path(TEST_PATH), TEST_PATH_LEN, null);
      // verify that we dropped everything from the cache during file creation.
      ExtendedBlock block = cluster.getNameNode().getRpcServer().getBlockLocations(
          TEST_PATH, 0, Long.MAX_VALUE).get(0).getBlock();
      String fadvisedFileName = cluster.getBlockFile(0, block).getName();
      Stats stats = tracker.getStats(fadvisedFileName);
      stats.assertDroppedInRange(0, TEST_PATH_LEN - WRITE_PACKET_SIZE);
      stats.clear();
      
      // read file
      readHdfsFile(fs, new Path(TEST_PATH), Long.MAX_VALUE, null);
      // verify that we dropped everything from the cache.
      Assert.assertNotNull(stats);
      stats.assertDroppedInRange(0, TEST_PATH_LEN - WRITE_PACKET_SIZE);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=120000)
  public void testFadviseSkippedForSmallReads() throws Exception {
    // start a cluster
    LOG.info("testFadviseSkippedForSmallReads");
    tracker.clear();
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_READS_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_KEY, true);
    MiniDFSCluster cluster = null;
    String TEST_PATH = "/test";
    int TEST_PATH_LEN = MAX_TEST_FILE_LEN;
    FSDataInputStream fis = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
          .build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      // create new file
      createHdfsFile(fs, new Path(TEST_PATH), TEST_PATH_LEN, null);
      // Since the DataNode was configured with drop-behind, and we didn't
      // specify any policy, we should have done drop-behind.
      ExtendedBlock block = cluster.getNameNode().getRpcServer().getBlockLocations(
          TEST_PATH, 0, Long.MAX_VALUE).get(0).getBlock();
      String fadvisedFileName = cluster.getBlockFile(0, block).getName();
      Stats stats = tracker.getStats(fadvisedFileName);
      stats.assertDroppedInRange(0, TEST_PATH_LEN - WRITE_PACKET_SIZE);
      stats.clear();
      stats.assertNotDroppedInRange(0, TEST_PATH_LEN);

      // read file
      fis = fs.open(new Path(TEST_PATH));
      byte buf[] = new byte[17];
      fis.readFully(4096, buf, 0, buf.length);

      // we should not have dropped anything because of the small read.
      stats = tracker.getStats(fadvisedFileName);
      stats.assertNotDroppedInRange(0, TEST_PATH_LEN - WRITE_PACKET_SIZE);
    } finally {
      IOUtils.cleanup(null, fis);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test(timeout=120000)
  public void testNoFadviseAfterWriteThenRead() throws Exception {
    // start a cluster
    LOG.info("testNoFadviseAfterWriteThenRead");
    tracker.clear();
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    String TEST_PATH = "/test";
    int TEST_PATH_LEN = MAX_TEST_FILE_LEN;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
          .build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      // create new file
      createHdfsFile(fs, new Path(TEST_PATH), TEST_PATH_LEN, false);
      // verify that we did not drop everything from the cache during file creation.
      ExtendedBlock block = cluster.getNameNode().getRpcServer().getBlockLocations(
          TEST_PATH, 0, Long.MAX_VALUE).get(0).getBlock();
      String fadvisedFileName = cluster.getBlockFile(0, block).getName();
      Stats stats = tracker.getStats(fadvisedFileName);
      Assert.assertNull(stats);
      
      // read file
      readHdfsFile(fs, new Path(TEST_PATH), Long.MAX_VALUE, false);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=120000)
  public void testSeekAfterSetDropBehind() throws Exception {
    // start a cluster
    LOG.info("testSeekAfterSetDropBehind");
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    String TEST_PATH = "/test";
    int TEST_PATH_LEN = MAX_TEST_FILE_LEN;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
          .build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      createHdfsFile(fs, new Path(TEST_PATH), TEST_PATH_LEN, false);
      // verify that we can seek after setDropBehind
      try (FSDataInputStream fis = fs.open(new Path(TEST_PATH))) {
        Assert.assertTrue(fis.read() != -1); // create BlockReader
        fis.setDropBehind(false); // clear BlockReader
        fis.seek(2); // seek
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
