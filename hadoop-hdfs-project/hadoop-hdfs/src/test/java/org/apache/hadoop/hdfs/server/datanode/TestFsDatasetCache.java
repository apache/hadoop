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

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doReturn;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetCache.PageRounder;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MappableBlock;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlockIdCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

public class TestFsDatasetCache {
  private static final Log LOG = LogFactory.getLog(TestFsDatasetCache.class);

  // Most Linux installs allow a default of 64KB locked memory
  private static final long CACHE_CAPACITY = 64 * 1024;
  private static final long BLOCK_SIZE = 4096;

  private static Configuration conf;
  private static MiniDFSCluster cluster = null;
  private static FileSystem fs;
  private static NameNode nn;
  private static FSImage fsImage;
  private static DataNode dn;
  private static FsDatasetSpi<?> fsd;
  private static DatanodeProtocolClientSideTranslatorPB spyNN;
  private static PageRounder rounder = new PageRounder();

  @Before
  public void setUp() throws Exception {
    assumeTrue(!Path.WINDOWS);
    assumeTrue(NativeIO.getMemlockLimit() >= CACHE_CAPACITY);
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_CACHING_ENABLED_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
        CACHE_CAPACITY);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    nn = cluster.getNameNode();
    fsImage = nn.getFSImage();
    dn = cluster.getDataNodes().get(0);
    fsd = dn.getFSDataset();

    spyNN = DataNodeTestUtils.spyOnBposToNN(dn, nn);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static void setHeartbeatResponse(DatanodeCommand[] cmds)
      throws IOException {
    HeartbeatResponse response = new HeartbeatResponse(
        cmds,
        new NNHAStatusHeartbeat(HAServiceState.ACTIVE,
        fsImage.getLastAppliedOrWrittenTxId()));
    doReturn(response).when(spyNN).sendHeartbeat(
        (DatanodeRegistration) any(),
        (StorageReport[]) any(), anyLong(), anyLong(),
        anyInt(), anyInt(), anyInt());
  }

  private static DatanodeCommand[] cacheBlock(HdfsBlockLocation loc) {
    return cacheBlocks(new HdfsBlockLocation[] {loc});
  }

  private static DatanodeCommand[] cacheBlocks(HdfsBlockLocation[] locs) {
    return new DatanodeCommand[] {
        getResponse(locs, DatanodeProtocol.DNA_CACHE)
    };
  }

  private static DatanodeCommand[] uncacheBlock(HdfsBlockLocation loc) {
    return uncacheBlocks(new HdfsBlockLocation[] {loc});
  }

  private static DatanodeCommand[] uncacheBlocks(HdfsBlockLocation[] locs) {
    return new DatanodeCommand[] {
        getResponse(locs, DatanodeProtocol.DNA_UNCACHE)
    };
  }

  /**
   * Creates a cache or uncache DatanodeCommand from an array of locations
   */
  private static DatanodeCommand getResponse(HdfsBlockLocation[] locs,
      int action) {
    String bpid = locs[0].getLocatedBlock().getBlock().getBlockPoolId();
    long[] blocks = new long[locs.length];
    for (int i=0; i<locs.length; i++) {
      blocks[i] = locs[i].getLocatedBlock().getBlock().getBlockId();
    }
    return new BlockIdCommand(action, bpid, blocks);
  }

  private static long[] getBlockSizes(HdfsBlockLocation[] locs)
      throws Exception {
    long[] sizes = new long[locs.length];
    for (int i=0; i<locs.length; i++) {
      HdfsBlockLocation loc = locs[i];
      String bpid = loc.getLocatedBlock().getBlock().getBlockPoolId();
      Block block = loc.getLocatedBlock().getBlock().getLocalBlock();
      ExtendedBlock extBlock = new ExtendedBlock(bpid, block);
      FileChannel blockChannel =
          ((FileInputStream)fsd.getBlockInputStream(extBlock, 0)).getChannel();
      sizes[i] = blockChannel.size();
    }
    return sizes;
  }

  /**
   * Blocks until cache usage hits the expected new value.
   */
  private long verifyExpectedCacheUsage(final long expected) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      private int tries = 0;
      
      @Override
      public Boolean get() {
        long curDnCacheUsed = fsd.getDnCacheUsed();
        if (curDnCacheUsed != expected) {
          if (tries++ > 10) {
            LOG.info("verifyExpectedCacheUsage: expected " +
                expected + ", got " + curDnCacheUsed + "; " +
                "memlock limit = " + NativeIO.getMemlockLimit() +
                ".  Waiting...");
          }
          return false;
        }
        return true;
      }
    }, 100, 60000);
    return expected;
  }

  @Test(timeout=600000)
  public void testCacheAndUncacheBlock() throws Exception {
    LOG.info("beginning testCacheAndUncacheBlock");
    final int NUM_BLOCKS = 5;

    verifyExpectedCacheUsage(0);

    // Write a test file
    final Path testFile = new Path("/testCacheBlock");
    final long testFileLen = BLOCK_SIZE*NUM_BLOCKS;
    DFSTestUtil.createFile(fs, testFile, testFileLen, (short)1, 0xABBAl);

    // Get the details of the written file
    HdfsBlockLocation[] locs =
        (HdfsBlockLocation[])fs.getFileBlockLocations(testFile, 0, testFileLen);
    assertEquals("Unexpected number of blocks", NUM_BLOCKS, locs.length);
    final long[] blockSizes = getBlockSizes(locs);

    // Check initial state
    final long cacheCapacity = fsd.getDnCacheCapacity();
    long cacheUsed = fsd.getDnCacheUsed();
    long current = 0;
    assertEquals("Unexpected cache capacity", CACHE_CAPACITY, cacheCapacity);
    assertEquals("Unexpected amount of cache used", current, cacheUsed);

    // Cache each block in succession, checking each time
    for (int i=0; i<NUM_BLOCKS; i++) {
      setHeartbeatResponse(cacheBlock(locs[i]));
      current = verifyExpectedCacheUsage(current + blockSizes[i]);
    }

    // Uncache each block in succession, again checking each time
    for (int i=0; i<NUM_BLOCKS; i++) {
      setHeartbeatResponse(uncacheBlock(locs[i]));
      current = verifyExpectedCacheUsage(current - blockSizes[i]);
    }
    LOG.info("finishing testCacheAndUncacheBlock");
  }

  @Test(timeout=600000)
  public void testFilesExceedMaxLockedMemory() throws Exception {
    LOG.info("beginning testFilesExceedMaxLockedMemory");

    // We don't want to deal with page rounding issues, so skip this
    // test if page size is weird
    long osPageSize = NativeIO.getOperatingSystemPageSize();
    assumeTrue(osPageSize == 4096); 

    // Create some test files that will exceed total cache capacity
    final int numFiles = 5;
    final long fileSize = 15000;

    final Path[] testFiles = new Path[numFiles];
    final HdfsBlockLocation[][] fileLocs = new HdfsBlockLocation[numFiles][];
    final long[] fileSizes = new long[numFiles];
    for (int i=0; i<numFiles; i++) {
      testFiles[i] = new Path("/testFilesExceedMaxLockedMemory-" + i);
      DFSTestUtil.createFile(fs, testFiles[i], fileSize, (short)1, 0xDFAl);
      fileLocs[i] = (HdfsBlockLocation[])fs.getFileBlockLocations(
          testFiles[i], 0, fileSize);
      // Get the file size (sum of blocks)
      long[] sizes = getBlockSizes(fileLocs[i]);
      for (int j=0; j<sizes.length; j++) {
        fileSizes[i] += sizes[j];
      }
    }

    // Cache the first n-1 files
    long total = 0;
    verifyExpectedCacheUsage(0);
    for (int i=0; i<numFiles-1; i++) {
      setHeartbeatResponse(cacheBlocks(fileLocs[i]));
      total = verifyExpectedCacheUsage(rounder.round(total + fileSizes[i]));
    }

    // nth file should hit a capacity exception
    final LogVerificationAppender appender = new LogVerificationAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);
    setHeartbeatResponse(cacheBlocks(fileLocs[numFiles-1]));

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        int lines = appender.countLinesWithMessage(
            "more bytes in the cache: " +
            DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY);
        return lines > 0;
      }
    }, 500, 30000);

    // Uncache the n-1 files
    for (int i=0; i<numFiles-1; i++) {
      setHeartbeatResponse(uncacheBlocks(fileLocs[i]));
      total -= rounder.round(fileSizes[i]);
      verifyExpectedCacheUsage(total);
    }
    LOG.info("finishing testFilesExceedMaxLockedMemory");
  }

  @Test(timeout=600000)
  public void testUncachingBlocksBeforeCachingFinishes() throws Exception {
    LOG.info("beginning testUncachingBlocksBeforeCachingFinishes");
    final int NUM_BLOCKS = 5;

    verifyExpectedCacheUsage(0);

    // Write a test file
    final Path testFile = new Path("/testCacheBlock");
    final long testFileLen = BLOCK_SIZE*NUM_BLOCKS;
    DFSTestUtil.createFile(fs, testFile, testFileLen, (short)1, 0xABBAl);

    // Get the details of the written file
    HdfsBlockLocation[] locs =
        (HdfsBlockLocation[])fs.getFileBlockLocations(testFile, 0, testFileLen);
    assertEquals("Unexpected number of blocks", NUM_BLOCKS, locs.length);
    final long[] blockSizes = getBlockSizes(locs);

    // Check initial state
    final long cacheCapacity = fsd.getDnCacheCapacity();
    long cacheUsed = fsd.getDnCacheUsed();
    long current = 0;
    assertEquals("Unexpected cache capacity", CACHE_CAPACITY, cacheCapacity);
    assertEquals("Unexpected amount of cache used", current, cacheUsed);

    MappableBlock.mlocker = new MappableBlock.Mlocker() {
      @Override
      public void mlock(MappedByteBuffer mmap, long length) throws IOException {
        LOG.info("An mlock operation is starting.");
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          Assert.fail();
        }
      }
    };
    // Starting caching each block in succession.  The usedBytes amount
    // should increase, even though caching doesn't complete on any of them.
    for (int i=0; i<NUM_BLOCKS; i++) {
      setHeartbeatResponse(cacheBlock(locs[i]));
      current = verifyExpectedCacheUsage(current + blockSizes[i]);
    }
    
    setHeartbeatResponse(new DatanodeCommand[] {
      getResponse(locs, DatanodeProtocol.DNA_UNCACHE)
    });

    // wait until all caching jobs are finished cancelling.
    current = verifyExpectedCacheUsage(0);
    LOG.info("finishing testUncachingBlocksBeforeCachingFinishes");
  }
}
