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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.LevelDBFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBuilder;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Optional;

import static org.apache.hadoop.test.MetricsAsserts.getFloatGauge;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the implementation of {@link ProvidedVolumeReplicaMap}.
 */
public class TestProvidedVolumeReplicaMap {

  private ProvidedVolumeReplicaMap providedReplicaMap;
  private BlockAliasMap.Reader mockAliasMapReader;
  private final String bpid = "BPID";
  private Configuration conf = new Configuration();
  private FileSystem mockFS;
  private ProvidedVolumeImpl mockVol;
  private DataNodeMetrics dnMetrics;

  @Before
  public void setup() {
    mockVol = mock(ProvidedVolumeImpl.class);
    when(mockVol.getStorageType()).thenReturn(StorageType.PROVIDED);
    mockAliasMapReader = mock(
        LevelDBFileRegionAliasMap.Reader.class);
    mockFS = mock(FileSystem.class);
    dnMetrics = DataNodeMetrics.create(conf, "dnName");
  }

  @Test
  public void testReplicaMapGet() throws Exception {
    conf.setLong(DFSConfigKeys.DFS_PROVIDED_REPLICAMAP_CACHE_SIZE, 100);
    providedReplicaMap = new ProvidedVolumeReplicaMap(mockAliasMapReader,
        mockVol, dnMetrics, conf, mockFS);
    providedReplicaMap.initBlockPool(bpid);
    long block1 = 100;
    long genStamp1 = 1;
    long blkLen = 128;
    FileRegion fr1 =
        new FileRegion(block1, mock(Path.class), 0, blkLen, genStamp1);
    when(mockAliasMapReader.resolve(block1)).thenReturn(Optional.of(fr1));

    // verify block pool id
    String[] blockPools = providedReplicaMap.getBlockPoolList();
    assertEquals(1, blockPools.length);
    assertEquals(bpid, blockPools[0]);

    // first time, the block is loaded on demand from the alias map.
    ReplicaInfo replica = providedReplicaMap.get(bpid, block1);
    assertNotNull(replica);
    assertEquals(genStamp1, replica.getGenerationStamp());
    assertEquals(blkLen, replica.getBlockDataLength());

    // the second time, alias map shouldn't be used.
    when(mockAliasMapReader.resolve(block1)).thenReturn(Optional.empty());
    replica = providedReplicaMap.get(bpid, block1);
    assertNotNull(replica);

    long block2 = 200;
    when(mockAliasMapReader.resolve(block2)).thenReturn(Optional.empty());
    assertNull(providedReplicaMap.get(bpid, block2));

    // block pool id doesn't exist in map
    assertNull(providedReplicaMap.get("NEW-BPID", block1));
  }

  @Test
  public void testReplicaMapCaching() throws Exception {
    int cacheSize = 10;
    conf.setLong(DFSConfigKeys.DFS_PROVIDED_REPLICAMAP_CACHE_SIZE, cacheSize);
    conf.setBoolean(DFSConfigKeys.DFS_PROVIDED_REPLICAMAP_CACHE_ENABLE_STATS,
        true);
    providedReplicaMap = new ProvidedVolumeReplicaMap(mockAliasMapReader,
        mockVol, dnMetrics, conf, mockFS);
    providedReplicaMap.initBlockPool(bpid);
    long firstBlockId = 100;
    int totalReplicas = 50;
    ReplicaInfo[] replicas = new ReplicaInfo[totalReplicas];

    for (long blockId = firstBlockId; blockId < firstBlockId + totalReplicas;
         blockId++) {
      FileRegion fr =
          new FileRegion(blockId, mock(Path.class), 0, blockId, blockId);
      when(mockAliasMapReader.resolve(blockId)).thenReturn(Optional.of(fr));
      replicas[(int)(blockId-firstBlockId)] = new ReplicaBuilder(
          HdfsServerConstants.ReplicaState.FINALIZED)
          .setFileRegion(fr)
          .setConf(conf)
          .setFsVolume(mockVol)
          .setRemoteFS(mockFS)
          .build();
    }

    // read the first cache-size number of blocks
    for (long blockId = firstBlockId; blockId < firstBlockId + cacheSize;
         blockId++) {
      assertNotNull(providedReplicaMap.get(bpid, blockId));
    }

    // only those loaded so far should be returned
    Collection<ReplicaInfo> replicasLoaded = providedReplicaMap.replicas(bpid);
    assertTrue(replicasLoaded.size() <= cacheSize);

    for (ReplicaInfo info : replicas) {
      if (info.getBlockId() >= firstBlockId + cacheSize) {
        // these cannot appear in the cache as they haven't read even once.
        assertFalse(replicasLoaded.contains(info));
      }
    }

    // at most cacheSize number of replicas must be preset at any time
    for (long blockId = firstBlockId + cacheSize;
         blockId < firstBlockId + totalReplicas;
         blockId++) {
      ReplicaInfo info = providedReplicaMap.get(bpid, blockId);
      assertNotNull(info);
      replicasLoaded = providedReplicaMap.replicas(bpid);
      assertTrue(replicasLoaded.size() <= cacheSize);
    }

    assertEquals(totalReplicas,
        getLongCounter("NumAliasMapLookups", getMetrics(dnMetrics.name())));
    // hit rate will be 0 as each replica is loaded only once.
    assertEquals(0,
        getFloatGauge("AliasMapCacheHitRate", getMetrics(dnMetrics.name())),
        0.001);
    // get the last block 10 times; each should be a cache hit.
    long lastBlockId = firstBlockId + totalReplicas - 1;
    for (int count = 0; count < 10; count++) {
      providedReplicaMap.get(bpid, lastBlockId);
    }
    assertEquals(10.0/(totalReplicas + 10),
        getFloatGauge("AliasMapCacheHitRate", getMetrics(dnMetrics.name())),
        0.1);
  }
}
