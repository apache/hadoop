/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.NullConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolumeSet;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Helper class to create various cluster configurations at run time.
 */
public class DiskBalancerTestUtil {
  static final Logger LOG = LoggerFactory.getLogger(TestDiskBalancer.class);
  public static final long MB = 1024 * 1024L;
  public static final long GB = MB * 1024L;
  public static final long TB = GB * 1024L;
  private static int[] diskSizes =
      {1, 2, 3, 4, 5, 6, 7, 8, 9, 100, 200, 300, 400, 500, 600, 700, 800, 900};
  private Random rand;
  private String stringTable =
      "ABCDEDFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0987654321";

  /**
   * Constructs a util class.
   */
  public DiskBalancerTestUtil() {
    this.rand = new Random(Time.monotonicNow());
  }

  /**
   * Returns a random string.
   *
   * @param length - Number of chars in the string
   * @return random String
   */
  private String getRandomName(int length) {
    StringBuilder name = new StringBuilder();
    for (int x = 0; x < length; x++) {
      name.append(stringTable.charAt(rand.nextInt(stringTable.length())));
    }
    return name.toString();
  }

  /**
   * Returns a Random Storage Type.
   *
   * @return - StorageType
   */
  private StorageType getRandomStorageType() {
    return StorageType.parseStorageType(rand.nextInt(3));
  }

  /**
   * Returns random capacity, if the size is smaller than 10
   * they are TBs otherwise the size is assigned to GB range.
   *
   * @return Long - Disk Size
   */
  private long getRandomCapacity() {
    int size = diskSizes[rand.nextInt(diskSizes.length)];
    if (size < 10) {
      return size * TB;
    } else {
      return size * GB;
    }
  }

  /**
   * Some value under 20% in these tests.
   */
  private long getRandomReserved(long capacity) {
    double rcap = capacity * 0.2d;
    double randDouble = rand.nextDouble();
    double temp = randDouble * rcap;
    return (new Double(temp)).longValue();

  }

  /**
   * Some value less that capacity - reserved.
   */
  private long getRandomDfsUsed(long capacity, long reserved) {
    double rcap = capacity - reserved;
    double randDouble = rand.nextDouble();
    double temp = randDouble * rcap;
    return (new Double(temp)).longValue();
  }

  /**
   * Creates a Random Volume of a specific storageType.
   *
   * @return Volume
   */
  public DiskBalancerVolume createRandomVolume() {
    return createRandomVolume(getRandomStorageType());
  }

  /**
   * Creates a Random Volume for testing purpose.
   *
   * @param type - StorageType
   * @return DiskBalancerVolume
   */
  public DiskBalancerVolume createRandomVolume(StorageType type) {
    DiskBalancerVolume volume = new DiskBalancerVolume();
    volume.setPath("/tmp/disk/" + getRandomName(10));
    volume.setStorageType(type.toString());
    volume.setTransient(type.isTransient());

    volume.setCapacity(getRandomCapacity());
    volume.setReserved(getRandomReserved(volume.getCapacity()));
    volume
        .setUsed(getRandomDfsUsed(volume.getCapacity(), volume.getReserved()));
    volume.setUuid(UUID.randomUUID().toString());
    return volume;
  }

  /**
   * Creates a RandomVolumeSet.
   *
   * @param type      - Storage Type
   * @param diskCount - How many disks you need.
   * @return volumeSet
   * @throws Exception
   */
  public DiskBalancerVolumeSet createRandomVolumeSet(StorageType type,
                                                     int diskCount)
      throws Exception {

    Preconditions.checkState(diskCount > 0);
    DiskBalancerVolumeSet volumeSet =
        new DiskBalancerVolumeSet(type.isTransient());
    for (int x = 0; x < diskCount; x++) {
      volumeSet.addVolume(createRandomVolume(type));
    }
    assert (volumeSet.getVolumeCount() == diskCount);
    return volumeSet;
  }

  /**
   * Creates a RandomDataNode.
   *
   * @param diskTypes - Storage types needed in the Node
   * @param diskCount - Disk count - that many disks of each type is created
   * @return DataNode
   * @throws Exception
   */
  public DiskBalancerDataNode createRandomDataNode(StorageType[] diskTypes,
                                                   int diskCount)
      throws Exception {
    Preconditions.checkState(diskTypes.length > 0);
    Preconditions.checkState(diskCount > 0);

    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    for (StorageType t : diskTypes) {
      DiskBalancerVolumeSet vSet = createRandomVolumeSet(t, diskCount);
      for (DiskBalancerVolume v : vSet.getVolumes()) {
        node.addVolume(v);
      }
    }
    return node;
  }

  /**
   * Creates a RandomCluster.
   *
   * @param dataNodeCount - How many nodes you need
   * @param diskTypes     - StorageTypes you need in each node
   * @param diskCount     - How many disks you need of each type.
   * @return Cluster
   * @throws Exception
   */
  public DiskBalancerCluster createRandCluster(int dataNodeCount,
                                               StorageType[] diskTypes,
                                               int diskCount)

      throws Exception {
    Preconditions.checkState(diskTypes.length > 0);
    Preconditions.checkState(diskCount > 0);
    Preconditions.checkState(dataNodeCount > 0);
    NullConnector nullConnector = new NullConnector();
    DiskBalancerCluster cluster = new DiskBalancerCluster(nullConnector);

    // once we add these nodes into the connector, cluster will read them
    // from the connector.
    for (int x = 0; x < dataNodeCount; x++) {
      nullConnector.addNode(createRandomDataNode(diskTypes, diskCount));
    }

    // with this call we have populated the cluster info
    cluster.readClusterInfo();
    return cluster;
  }

  /**
   * Returns the number of blocks on a volume.
   *
   * @param source - Source Volume.
   * @return Number of Blocks.
   * @throws IOException
   */
  public static int getBlockCount(FsVolumeSpi source,
                                  boolean checkblockPoolCount)
      throws IOException {
    int count = 0;
    for (String blockPoolID : source.getBlockPoolList()) {
      FsVolumeSpi.BlockIterator sourceIter =
          source.newBlockIterator(blockPoolID, "TestDiskBalancerSource");
      int blockCount = 0;
      while (!sourceIter.atEnd()) {
        ExtendedBlock block = sourceIter.nextBlock();
        if (block != null) {
          blockCount++;
        }
      }
      if (checkblockPoolCount) {
        LOG.info("Block Pool Id:  {}, blockCount: {}", blockPoolID, blockCount);
        assertTrue(blockCount > 0);
      }
      count += blockCount;
    }
    return count;
  }

  public static MiniDFSCluster newImbalancedCluster(
      final Configuration conf,
      final int numDatanodes,
      final long[] storageCapacities,
      final int defaultBlockSize,
      final int fileLen)
      throws IOException, InterruptedException, TimeoutException {
    return newImbalancedCluster(
      conf,
      numDatanodes,
      storageCapacities,
      defaultBlockSize,
      fileLen,
      null);
  }

  public static MiniDFSCluster newImbalancedCluster(
      final Configuration conf,
      final int numDatanodes,
      final long[] storageCapacities,
      final int defaultBlockSize,
      final int fileLen,
      final StartupOption dnOption)
      throws IOException, InterruptedException, TimeoutException {
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, defaultBlockSize);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, defaultBlockSize);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);

    final String fileName = "/" + UUID.randomUUID().toString();
    final Path filePath = new Path(fileName);

    Preconditions.checkNotNull(storageCapacities);
    Preconditions.checkArgument(
        storageCapacities.length == 2,
        "need to specify capacities for two storages.");

    // Write a file and restart the cluster
    File basedir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, basedir)
        .numDataNodes(numDatanodes)
        .storageCapacities(storageCapacities)
        .storageTypes(new StorageType[]{StorageType.DISK, StorageType.DISK})
        .storagesPerDatanode(2)
        .dnStartupOption(dnOption)
        .build();
    FsVolumeImpl source = null;
    FsVolumeImpl dest = null;

    cluster.waitActive();
    Random r = new Random();
    FileSystem fs = cluster.getFileSystem(0);
    TestBalancer.createFile(cluster, filePath, fileLen, (short) 1, 0);

    DFSTestUtil.waitReplication(fs, filePath, (short) 1);
    cluster.restartDataNodes();
    cluster.waitActive();

    // Get the data node and move all data to one disk.
    for (int i = 0; i < numDatanodes; i++) {
      DataNode dnNode = cluster.getDataNodes().get(i);
      try (FsDatasetSpi.FsVolumeReferences refs =
               dnNode.getFSDataset().getFsVolumeReferences()) {
        source = (FsVolumeImpl) refs.get(0);
        dest = (FsVolumeImpl) refs.get(1);
        assertTrue(DiskBalancerTestUtil.getBlockCount(source, true) > 0);
        DiskBalancerTestUtil.moveAllDataToDestVolume(dnNode.getFSDataset(),
            source, dest);
        assertEquals(0, DiskBalancerTestUtil.getBlockCount(source, false));
      }
    }

    cluster.restartDataNodes();
    cluster.waitActive();

    return cluster;
  }

  /**
   * Moves all blocks to the destination volume.
   *
   * @param fsDataset - Dataset
   * @param source    - Source Volume.
   * @param dest      - Destination Volume.
   * @throws IOException
   */
  public static void moveAllDataToDestVolume(FsDatasetSpi fsDataset,
      FsVolumeSpi source, FsVolumeSpi dest) throws IOException {

    for (String blockPoolID : source.getBlockPoolList()) {
      FsVolumeSpi.BlockIterator sourceIter =
          source.newBlockIterator(blockPoolID, "TestDiskBalancerSource");
      while (!sourceIter.atEnd()) {
        ExtendedBlock block = sourceIter.nextBlock();
        if (block != null) {
          fsDataset.moveBlockAcrossVolumes(block, dest);
        }
      }
    }
  }
}
