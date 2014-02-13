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

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.client.ShortCircuitCache;
import org.apache.hadoop.hdfs.client.ShortCircuitCache.ShortCircuitReplicaCreator;
import org.apache.hadoop.hdfs.client.ShortCircuitReplica;
import org.apache.hadoop.hdfs.client.ShortCircuitReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestShortCircuitCache {
  static final Log LOG = LogFactory.getLog(TestShortCircuitCache.class);
  
  private static class TestFileDescriptorPair {
    TemporarySocketDirectory dir = new TemporarySocketDirectory();
    FileInputStream fis[];

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
                cache, Time.monotonicNow()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test(timeout=60000)
  public void testCreateAndDestroy() throws Exception {
    ShortCircuitCache cache =
        new ShortCircuitCache(10, 1, 10, 1, 1, 10000);
    cache.close();
  }
  
  @Test(timeout=60000)
  public void testAddAndRetrieve() throws Exception {
    final ShortCircuitCache cache =
        new ShortCircuitCache(10, 10000000, 10, 10000000, 1, 10000);
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
        new ShortCircuitCache(2, 1, 1, 10000000, 1, 10000);
    final TestFileDescriptorPair pair = new TestFileDescriptorPair();
    ShortCircuitReplicaInfo replicaInfo1 =
      cache.fetchOrCreate(
        new ExtendedBlockId(123, "test_bp1"), new SimpleReplicaCreator(123, cache, pair));
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
        new ShortCircuitCache(2, 10000000, 1, 10000000, 1, 10000);
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
  public void testStaleness() throws Exception {
    // Set up the cache with a short staleness time.
    final ShortCircuitCache cache =
        new ShortCircuitCache(2, 10000000, 1, 10000000, 1, 10);
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
                    cache, Time.monotonicNow() + (iVal * HOUR_IN_MS)));
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
}
