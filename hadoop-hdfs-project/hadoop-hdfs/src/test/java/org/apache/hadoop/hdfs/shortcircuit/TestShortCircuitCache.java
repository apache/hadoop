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
package org.apache.hadoop.hdfs.shortcircuit;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_KEY;
import static org.hamcrest.CoreMatchers.equalTo;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReaderTestUtil;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.net.DomainPeer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager.PerDatanodeVisitorInfo;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager.Visitor;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache.CacheVisitor;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache.ShortCircuitReplicaCreator;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

public class TestShortCircuitCache {
  static final Log LOG = LogFactory.getLog(TestShortCircuitCache.class);
  
  private static class TestFileDescriptorPair {
    final TemporarySocketDirectory dir = new TemporarySocketDirectory();
    final FileInputStream[] fis;

    public TestFileDescriptorPair() throws IOException {
      fis = new FileInputStream[2];
      for (int i = 0; i < 2; i++) {
        String name = dir.getDir() + "/file" + i;
        FileOutputStream fos = new FileOutputStream(name);
        if (i == 0) {
          // write 'data' file
          fos.write(1);
        } else {
          // write 'metadata' file
          BlockMetadataHeader header =
              new BlockMetadataHeader((short)1,
                  DataChecksum.newDataChecksum(DataChecksum.Type.NULL, 4));
          DataOutputStream dos = new DataOutputStream(fos);
          BlockMetadataHeader.writeHeader(dos, header);
          dos.close();
        }
        fos.close();
        fis[i] = new FileInputStream(name);
      }
    }

    public FileInputStream[] getFileInputStreams() {
      return fis;
    }

    public void close() throws IOException {
      IOUtils.cleanup(LOG, fis);
      dir.close();
    }

    public boolean compareWith(FileInputStream data, FileInputStream meta) {
      return ((data == fis[0]) && (meta == fis[1]));
    }
  }

  private static class SimpleReplicaCreator
      implements ShortCircuitReplicaCreator {
    private final int blockId;
    private final ShortCircuitCache cache;
    private final TestFileDescriptorPair pair;

    SimpleReplicaCreator(int blockId, ShortCircuitCache cache,
        TestFileDescriptorPair pair) {
      this.blockId = blockId;
      this.cache = cache;
      this.pair = pair;
    }

    @Override
    public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
      try {
        ExtendedBlockId key = new ExtendedBlockId(blockId, "test_bp1");
        return new ShortCircuitReplicaInfo(
            new ShortCircuitReplica(key,
                pair.getFileInputStreams()[0], pair.getFileInputStreams()[1],
                cache, Time.monotonicNow(), null));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test(timeout=60000)
  public void testCreateAndDestroy() throws Exception {
    ShortCircuitCache cache =
        new ShortCircuitCache(10, 1, 10, 1, 1, 10000, 0);
    cache.close();
  }
  
  @Test(timeout=60000)
  public void testAddAndRetrieve() throws Exception {
    final ShortCircuitCache cache =
        new ShortCircuitCache(10, 10000000, 10, 10000000, 1, 10000, 0);
    final TestFileDescriptorPair pair = new TestFileDescriptorPair();
    ShortCircuitReplicaInfo replicaInfo1 =
      cache.fetchOrCreate(new ExtendedBlockId(123, "test_bp1"),
        new SimpleReplicaCreator(123, cache, pair));
    Preconditions.checkNotNull(replicaInfo1.getReplica());
    Preconditions.checkState(replicaInfo1.getInvalidTokenException() == null);
    pair.compareWith(replicaInfo1.getReplica().getDataStream(),
                     replicaInfo1.getReplica().getMetaStream());
    ShortCircuitReplicaInfo replicaInfo2 =
      cache.fetchOrCreate(new ExtendedBlockId(123, "test_bp1"),
          new ShortCircuitReplicaCreator() {
        @Override
        public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
          Assert.fail("expected to use existing entry.");
          return null;
        }
      });
    Preconditions.checkNotNull(replicaInfo2.getReplica());
    Preconditions.checkState(replicaInfo2.getInvalidTokenException() == null);
    Preconditions.checkState(replicaInfo1 == replicaInfo2);
    pair.compareWith(replicaInfo2.getReplica().getDataStream(),
                     replicaInfo2.getReplica().getMetaStream());
    replicaInfo1.getReplica().unref();
    replicaInfo2.getReplica().unref();
    
    // Even after the reference count falls to 0, we still keep the replica
    // around for a while (we have configured the expiry period to be really,
    // really long here)
    ShortCircuitReplicaInfo replicaInfo3 =
      cache.fetchOrCreate(
          new ExtendedBlockId(123, "test_bp1"), new ShortCircuitReplicaCreator() {
        @Override
        public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
          Assert.fail("expected to use existing entry.");
          return null;
        }
      });
    Preconditions.checkNotNull(replicaInfo3.getReplica());
    Preconditions.checkState(replicaInfo3.getInvalidTokenException() == null);
    replicaInfo3.getReplica().unref();
    
    pair.close();
    cache.close();
  }

  @Test(timeout=60000)
  public void testExpiry() throws Exception {
    final ShortCircuitCache cache =
        new ShortCircuitCache(2, 1, 1, 10000000, 1, 10000000, 0);
    final TestFileDescriptorPair pair = new TestFileDescriptorPair();
    ShortCircuitReplicaInfo replicaInfo1 =
      cache.fetchOrCreate(
        new ExtendedBlockId(123, "test_bp1"),
          new SimpleReplicaCreator(123, cache, pair));
    Preconditions.checkNotNull(replicaInfo1.getReplica());
    Preconditions.checkState(replicaInfo1.getInvalidTokenException() == null);
    pair.compareWith(replicaInfo1.getReplica().getDataStream(),
                     replicaInfo1.getReplica().getMetaStream());
    replicaInfo1.getReplica().unref();
    final MutableBoolean triedToCreate = new MutableBoolean(false);
    do {
      Thread.sleep(10);
      ShortCircuitReplicaInfo replicaInfo2 =
        cache.fetchOrCreate(
          new ExtendedBlockId(123, "test_bp1"), new ShortCircuitReplicaCreator() {
          @Override
          public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
            triedToCreate.setValue(true);
            return null;
          }
        });
      if ((replicaInfo2 != null) && (replicaInfo2.getReplica() != null)) {
        replicaInfo2.getReplica().unref();
      }
    } while (triedToCreate.isFalse());
    cache.close();
  }
  
  
  @Test(timeout=60000)
  public void testEviction() throws Exception {
    final ShortCircuitCache cache =
        new ShortCircuitCache(2, 10000000, 1, 10000000, 1, 10000, 0);
    final TestFileDescriptorPair pairs[] = new TestFileDescriptorPair[] {
      new TestFileDescriptorPair(),
      new TestFileDescriptorPair(),
      new TestFileDescriptorPair(),
    };
    ShortCircuitReplicaInfo replicaInfos[] = new ShortCircuitReplicaInfo[] {
      null,
      null,
      null
    };
    for (int i = 0; i < pairs.length; i++) {
      replicaInfos[i] = cache.fetchOrCreate(
          new ExtendedBlockId(i, "test_bp1"), 
            new SimpleReplicaCreator(i, cache, pairs[i]));
      Preconditions.checkNotNull(replicaInfos[i].getReplica());
      Preconditions.checkState(replicaInfos[i].getInvalidTokenException() == null);
      pairs[i].compareWith(replicaInfos[i].getReplica().getDataStream(),
                           replicaInfos[i].getReplica().getMetaStream());
    }
    // At this point, we have 3 replicas in use.
    // Let's close them all.
    for (int i = 0; i < pairs.length; i++) {
      replicaInfos[i].getReplica().unref();
    }
    // The last two replicas should still be cached.
    for (int i = 1; i < pairs.length; i++) {
      final Integer iVal = new Integer(i);
      replicaInfos[i] = cache.fetchOrCreate(
          new ExtendedBlockId(i, "test_bp1"),
            new ShortCircuitReplicaCreator() {
        @Override
        public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
          Assert.fail("expected to use existing entry for " + iVal);
          return null;
        }
      });
      Preconditions.checkNotNull(replicaInfos[i].getReplica());
      Preconditions.checkState(replicaInfos[i].getInvalidTokenException() == null);
      pairs[i].compareWith(replicaInfos[i].getReplica().getDataStream(),
                           replicaInfos[i].getReplica().getMetaStream());
    }
    // The first (oldest) replica should not be cached.
    final MutableBoolean calledCreate = new MutableBoolean(false);
    replicaInfos[0] = cache.fetchOrCreate(
        new ExtendedBlockId(0, "test_bp1"),
          new ShortCircuitReplicaCreator() {
        @Override
        public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
          calledCreate.setValue(true);
          return null;
        }
      });
    Preconditions.checkState(replicaInfos[0].getReplica() == null);
    Assert.assertTrue(calledCreate.isTrue());
    // Clean up
    for (int i = 1; i < pairs.length; i++) {
      replicaInfos[i].getReplica().unref();
    }
    for (int i = 0; i < pairs.length; i++) {
      pairs[i].close();
    }
    cache.close();
  }
  
  @Test(timeout=60000)
  public void testTimeBasedStaleness() throws Exception {
    // Set up the cache with a short staleness time.
    final ShortCircuitCache cache =
        new ShortCircuitCache(2, 10000000, 1, 10000000, 1, 10, 0);
    final TestFileDescriptorPair pairs[] = new TestFileDescriptorPair[] {
      new TestFileDescriptorPair(),
      new TestFileDescriptorPair(),
    };
    ShortCircuitReplicaInfo replicaInfos[] = new ShortCircuitReplicaInfo[] {
      null,
      null
    };
    final long HOUR_IN_MS = 60 * 60 * 1000;
    for (int i = 0; i < pairs.length; i++) {
      final Integer iVal = new Integer(i);
      final ExtendedBlockId key = new ExtendedBlockId(i, "test_bp1");
      replicaInfos[i] = cache.fetchOrCreate(key,
          new ShortCircuitReplicaCreator() {
        @Override
        public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
          try {
            return new ShortCircuitReplicaInfo(
                new ShortCircuitReplica(key,
                    pairs[iVal].getFileInputStreams()[0],
                    pairs[iVal].getFileInputStreams()[1],
                    cache, Time.monotonicNow() + (iVal * HOUR_IN_MS), null));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      Preconditions.checkNotNull(replicaInfos[i].getReplica());
      Preconditions.checkState(replicaInfos[i].getInvalidTokenException() == null);
      pairs[i].compareWith(replicaInfos[i].getReplica().getDataStream(),
                           replicaInfos[i].getReplica().getMetaStream());
    }

    // Keep trying to getOrCreate block 0 until it goes stale (and we must re-create.)
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        ShortCircuitReplicaInfo info = cache.fetchOrCreate(
          new ExtendedBlockId(0, "test_bp1"), new ShortCircuitReplicaCreator() {
          @Override
          public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
            return null;
          }
        });
        if (info.getReplica() != null) {
          info.getReplica().unref();
          return false;
        }
        return true;
      }
    }, 500, 60000);

    // Make sure that second replica did not go stale.
    ShortCircuitReplicaInfo info = cache.fetchOrCreate(
        new ExtendedBlockId(1, "test_bp1"), new ShortCircuitReplicaCreator() {
      @Override
      public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
        Assert.fail("second replica went stale, despite 1 " +
            "hour staleness time.");
        return null;
      }
    });
    info.getReplica().unref();

    // Clean up
    for (int i = 1; i < pairs.length; i++) {
      replicaInfos[i].getReplica().unref();
    }
    cache.close();
  }

  private static Configuration createShortCircuitConf(String testName,
      TemporarySocketDirectory sockDir) {
    Configuration conf = new Configuration();
    conf.set(DFS_CLIENT_CONTEXT, testName);
    conf.setLong(DFS_BLOCK_SIZE_KEY, 4096);
    conf.set(DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),
        testName).getAbsolutePath());
    conf.setBoolean(DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
    conf.setBoolean(DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY,
        false);
    conf.setBoolean(DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, false);
    DFSInputStream.tcpReadsDisabledForTesting = true;
    DomainSocket.disableBindPathValidation();
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
    return conf;
  }
  
  private static DomainPeer getDomainPeerToDn(Configuration conf)
      throws IOException {
    DomainSocket sock =
        DomainSocket.connect(conf.get(DFS_DOMAIN_SOCKET_PATH_KEY));
    return new DomainPeer(sock);
  }
  
  @Test(timeout=60000)
  public void testAllocShm() throws Exception {
    BlockReaderTestUtil.enableShortCircuitShmTracing();
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    Configuration conf = createShortCircuitConf("testAllocShm", sockDir);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    final ShortCircuitCache cache =
        fs.getClient().getClientContext().getShortCircuitCache();
    cache.getDfsClientShmManager().visit(new Visitor() {
      @Override
      public void visit(HashMap<DatanodeInfo, PerDatanodeVisitorInfo> info)
          throws IOException {
        // The ClientShmManager starts off empty
        Assert.assertEquals(0,  info.size());
      }
    });
    DomainPeer peer = getDomainPeerToDn(conf);
    MutableBoolean usedPeer = new MutableBoolean(false);
    ExtendedBlockId blockId = new ExtendedBlockId(123, "xyz");
    final DatanodeInfo datanode =
        new DatanodeInfo(cluster.getDataNodes().get(0).getDatanodeId());
    // Allocating the first shm slot requires using up a peer.
    Slot slot = cache.allocShmSlot(datanode, peer, usedPeer,
                    blockId, "testAllocShm_client");
    Assert.assertNotNull(slot);
    Assert.assertTrue(usedPeer.booleanValue());
    cache.getDfsClientShmManager().visit(new Visitor() {
      @Override
      public void visit(HashMap<DatanodeInfo, PerDatanodeVisitorInfo> info)
          throws IOException {
        // The ClientShmManager starts off empty
        Assert.assertEquals(1,  info.size());
        PerDatanodeVisitorInfo vinfo = info.get(datanode);
        Assert.assertFalse(vinfo.disabled);
        Assert.assertEquals(0, vinfo.full.size());
        Assert.assertEquals(1, vinfo.notFull.size());
      }
    });
    cache.scheduleSlotReleaser(slot);
    // Wait for the slot to be released, and the shared memory area to be
    // closed.  Since we didn't register this shared memory segment on the
    // server, it will also be a test of how well the server deals with
    // bogus client behavior.
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        final MutableBoolean done = new MutableBoolean(false);
        try {
          cache.getDfsClientShmManager().visit(new Visitor() {
            @Override
            public void visit(HashMap<DatanodeInfo, PerDatanodeVisitorInfo> info)
                throws IOException {
              done.setValue(info.get(datanode).full.isEmpty() &&
                  info.get(datanode).notFull.isEmpty());
            }
          });
        } catch (IOException e) {
          LOG.error("error running visitor", e);
        }
        return done.booleanValue();
      }
    }, 10, 60000);
    cluster.shutdown();
    sockDir.close();
  }

  @Test(timeout=60000)
  public void testShmBasedStaleness() throws Exception {
    BlockReaderTestUtil.enableShortCircuitShmTracing();
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    Configuration conf = createShortCircuitConf("testShmBasedStaleness", sockDir);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    final ShortCircuitCache cache =
        fs.getClient().getClientContext().getShortCircuitCache();
    String TEST_FILE = "/test_file";
    final int TEST_FILE_LEN = 8193;
    final int SEED = 0xFADED;
    DFSTestUtil.createFile(fs, new Path(TEST_FILE), TEST_FILE_LEN,
        (short)1, SEED);
    FSDataInputStream fis = fs.open(new Path(TEST_FILE));
    int first = fis.read();
    final ExtendedBlock block =
        DFSTestUtil.getFirstBlock(fs, new Path(TEST_FILE));
    Assert.assertTrue(first != -1);
    cache.accept(new CacheVisitor() {
      @Override
      public void visit(int numOutstandingMmaps,
          Map<ExtendedBlockId, ShortCircuitReplica> replicas,
          Map<ExtendedBlockId, InvalidToken> failedLoads,
          Map<Long, ShortCircuitReplica> evictable,
          Map<Long, ShortCircuitReplica> evictableMmapped) {
        ShortCircuitReplica replica = replicas.get(
            ExtendedBlockId.fromExtendedBlock(block));
        Assert.assertNotNull(replica);
        Assert.assertTrue(replica.getSlot().isValid());
      }
    });
    // Stop the Namenode.  This will close the socket keeping the client's
    // shared memory segment alive, and make it stale.
    cluster.getDataNodes().get(0).shutdown();
    cache.accept(new CacheVisitor() {
      @Override
      public void visit(int numOutstandingMmaps,
          Map<ExtendedBlockId, ShortCircuitReplica> replicas,
          Map<ExtendedBlockId, InvalidToken> failedLoads,
          Map<Long, ShortCircuitReplica> evictable,
          Map<Long, ShortCircuitReplica> evictableMmapped) {
        ShortCircuitReplica replica = replicas.get(
            ExtendedBlockId.fromExtendedBlock(block));
        Assert.assertNotNull(replica);
        Assert.assertFalse(replica.getSlot().isValid());
      }
    });
    cluster.shutdown();
  }

  /**
   * Test unlinking a file whose blocks we are caching in the DFSClient.
   * The DataNode will notify the DFSClient that the replica is stale via the
   * ShortCircuitShm.
   */
  @Test(timeout=60000)
  public void testUnlinkingReplicasInFileDescriptorCache() throws Exception {
    BlockReaderTestUtil.enableShortCircuitShmTracing();
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    Configuration conf = createShortCircuitConf(
        "testUnlinkingReplicasInFileDescriptorCache", sockDir);
    // We don't want the CacheCleaner to time out short-circuit shared memory
    // segments during the test, so set the timeout really high.
    conf.setLong(DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_KEY,
        1000000000L);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    final ShortCircuitCache cache =
        fs.getClient().getClientContext().getShortCircuitCache();
    cache.getDfsClientShmManager().visit(new Visitor() {
      @Override
      public void visit(HashMap<DatanodeInfo, PerDatanodeVisitorInfo> info)
          throws IOException {
        // The ClientShmManager starts off empty.
        Assert.assertEquals(0,  info.size());
      }
    });
    final Path TEST_PATH = new Path("/test_file");
    final int TEST_FILE_LEN = 8193;
    final int SEED = 0xFADE0;
    DFSTestUtil.createFile(fs, TEST_PATH, TEST_FILE_LEN,
        (short)1, SEED);
    byte contents[] = DFSTestUtil.readFileBuffer(fs, TEST_PATH);
    byte expected[] = DFSTestUtil.
        calculateFileContentsFromSeed(SEED, TEST_FILE_LEN);
    Assert.assertTrue(Arrays.equals(contents, expected));
    // Loading this file brought the ShortCircuitReplica into our local
    // replica cache.
    final DatanodeInfo datanode =
        new DatanodeInfo(cluster.getDataNodes().get(0).getDatanodeId());
    cache.getDfsClientShmManager().visit(new Visitor() {
      @Override
      public void visit(HashMap<DatanodeInfo, PerDatanodeVisitorInfo> info)
          throws IOException {
        Assert.assertTrue(info.get(datanode).full.isEmpty());
        Assert.assertFalse(info.get(datanode).disabled);
        Assert.assertEquals(1, info.get(datanode).notFull.values().size());
        DfsClientShm shm =
            info.get(datanode).notFull.values().iterator().next();
        Assert.assertFalse(shm.isDisconnected());
      }
    });
    // Remove the file whose blocks we just read.
    fs.delete(TEST_PATH, false);

    // Wait for the replica to be purged from the DFSClient's cache.
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      MutableBoolean done = new MutableBoolean(true);
      @Override
      public Boolean get() {
        try {
          done.setValue(true);
          cache.getDfsClientShmManager().visit(new Visitor() {
            @Override
            public void visit(HashMap<DatanodeInfo,
                  PerDatanodeVisitorInfo> info) throws IOException {
              Assert.assertTrue(info.get(datanode).full.isEmpty());
              Assert.assertFalse(info.get(datanode).disabled);
              Assert.assertEquals(1,
                  info.get(datanode).notFull.values().size());
              DfsClientShm shm = info.get(datanode).notFull.values().
                  iterator().next();
              // Check that all slots have been invalidated.
              for (Iterator<Slot> iter = shm.slotIterator();
                   iter.hasNext(); ) {
                Slot slot = iter.next();
                if (slot.isValid()) {
                  done.setValue(false);
                }
              }
            }
          });
        } catch (IOException e) {
          LOG.error("error running visitor", e);
        }
        return done.booleanValue();
      }
    }, 10, 60000);
    cluster.shutdown();
    sockDir.close();
  }
}
