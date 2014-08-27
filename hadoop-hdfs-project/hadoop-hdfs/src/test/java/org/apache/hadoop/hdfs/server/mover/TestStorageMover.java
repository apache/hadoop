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
package org.apache.hadoop.hdfs.server.mover;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.*;

/**
 * Test the data migration tool (for Archival Storage)
 */
public class TestStorageMover {
  private static final long BLOCK_SIZE = 1024;
  private static final short REPL = 3;
  private static final int NUM_DATANODES = 6;
  private static final Configuration DEFAULT_CONF = new HdfsConfiguration();
  private static final BlockStoragePolicy.Suite DEFAULT_POLICIES;
  private static final BlockStoragePolicy HOT;
  private static final BlockStoragePolicy WARM;
  private static final BlockStoragePolicy COLD;

  static {
    DEFAULT_POLICIES = BlockStoragePolicy.readBlockStorageSuite(new
        HdfsConfiguration());
    HOT = DEFAULT_POLICIES.getPolicy("HOT");
    WARM = DEFAULT_POLICIES.getPolicy("WARM");
    COLD = DEFAULT_POLICIES.getPolicy("COLD");
    Dispatcher.setBlockMoveWaitTime(10 * 1000);
  }

  /**
   * This scheme defines files/directories and their block storage policies. It
   * also defines snapshots.
   */
  static class NamespaceScheme {
    final List<Path> files;
    final Map<Path, List<String>> snapshotMap;
    final Map<Path, BlockStoragePolicy> policyMap;

    NamespaceScheme(List<Path> files, Map<Path,List<String>> snapshotMap,
                    Map<Path, BlockStoragePolicy> policyMap) {
      this.files = files;
      this.snapshotMap = snapshotMap == null ?
          new HashMap<Path, List<String>>() : snapshotMap;
      this.policyMap = policyMap;
    }
  }

  /**
   * This scheme defines DataNodes and their storage, including storage types
   * and remaining capacities.
   */
  static class ClusterScheme {
    final Configuration conf;
    final int numDataNodes;
    final short repl;
    final StorageType[][] storageTypes;
    final long[][] storageCapacities;

    ClusterScheme(Configuration conf, int numDataNodes, short repl,
        StorageType[][] types, long[][] capacities) {
      Preconditions.checkArgument(types == null || types.length == numDataNodes);
      Preconditions.checkArgument(capacities == null || capacities.length ==
          numDataNodes);
      this.conf = conf;
      this.numDataNodes = numDataNodes;
      this.repl = repl;
      this.storageTypes = types;
      this.storageCapacities = capacities;
    }
  }

  class MigrationTest {
    private final ClusterScheme clusterScheme;
    private final NamespaceScheme nsScheme;
    private final Configuration conf;

    private MiniDFSCluster cluster;
    private DistributedFileSystem dfs;
    private final BlockStoragePolicy.Suite policies;

    MigrationTest(ClusterScheme cScheme, NamespaceScheme nsScheme) {
      this.clusterScheme = cScheme;
      this.nsScheme = nsScheme;
      this.conf = clusterScheme.conf;
      this.policies = BlockStoragePolicy.readBlockStorageSuite(conf);
    }

    /**
     * Set up the cluster and start NameNode and DataNodes according to the
     * corresponding scheme.
     */
    void setupCluster() throws Exception {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(clusterScheme
          .numDataNodes).storageTypes(clusterScheme.storageTypes)
          .storageCapacities(clusterScheme.storageCapacities).build();
      cluster.waitActive();
      dfs = cluster.getFileSystem();
    }

    void shutdownCluster() throws Exception {
      IOUtils.cleanup(null, dfs);
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    /**
     * Create files/directories and set their storage policies according to the
     * corresponding scheme.
     */
    void prepareNamespace() throws Exception {
      for (Path file : nsScheme.files) {
        DFSTestUtil.createFile(dfs, file, BLOCK_SIZE * 2, clusterScheme.repl,
            0L);
      }
      for (Map.Entry<Path, List<String>> entry : nsScheme.snapshotMap.entrySet()) {
        for (String snapshot : entry.getValue()) {
          SnapshotTestHelper.createSnapshot(dfs, entry.getKey(), snapshot);
        }
      }
      for (Map.Entry<Path, BlockStoragePolicy> entry : nsScheme.policyMap.entrySet()) {
        dfs.setStoragePolicy(entry.getKey(), entry.getValue().getName());
      }
    }

    /**
     * Run the migration tool.
     */
    void migrate(String... args) throws Exception {
      runMover();
    }

    /**
     * Verify block locations after running the migration tool.
     */
    void verify(boolean verifyAll) throws Exception {
      if (verifyAll) {
        verifyNamespace();
      } else {
        // TODO verify according to the given path list

      }
    }

    private void runMover() throws Exception {
      Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
      int result = Mover.run(namenodes, conf);
      Assert.assertEquals(ExitStatus.SUCCESS.getExitCode(), result);
    }

    private void verifyNamespace() throws Exception {
      HdfsFileStatus status = dfs.getClient().getFileInfo("/");
      verifyRecursively(null, status);
    }

    private void verifyRecursively(final Path parent,
        final HdfsFileStatus status) throws Exception {
      if (status.isDir()) {
        Path fullPath = parent == null ?
            new Path("/") : status.getFullPath(parent);
        DirectoryListing children = dfs.getClient().listPaths(
            fullPath.toString(), HdfsFileStatus.EMPTY_NAME, true);
        for (HdfsFileStatus child : children.getPartialListing()) {
          verifyRecursively(fullPath, child);
        }
      } else if (!status.isSymlink()) { // is file
        HdfsLocatedFileStatus fileStatus = (HdfsLocatedFileStatus) status;
        byte policyId = fileStatus.getStoragePolicy();
        BlockStoragePolicy policy = policies.getPolicy(policyId);
        final List<StorageType> types = policy.chooseStorageTypes(
            status.getReplication());
        for(LocatedBlock lb : fileStatus.getBlockLocations().getLocatedBlocks()) {
          final Mover.StorageTypeDiff diff = new Mover.StorageTypeDiff(types,
              lb.getStorageTypes());
          Assert.assertTrue(diff.removeOverlap());
        }
      }
    }
  }

  private static StorageType[][] genStorageTypes(int numDataNodes) {
    StorageType[][] types = new StorageType[numDataNodes][];
    for (int i = 0; i < types.length; i++) {
      types[i] = new StorageType[]{StorageType.DISK, StorageType.ARCHIVE};
    }
    return types;
  }

  private void runTest(MigrationTest test) throws Exception {
    test.setupCluster();
    try {
      test.prepareNamespace();
      test.migrate();
      Thread.sleep(5000); // let the NN finish deletion
      test.verify(true);
    } finally {
      test.shutdownCluster();
    }
  }

  /**
   * A normal case for Mover: move a file into archival storage
   */
  @Test
  public void testMigrateFileToArchival() throws Exception {
    final Path foo = new Path("/foo");
    Map<Path, BlockStoragePolicy> policyMap = Maps.newHashMap();
    policyMap.put(foo, COLD);
    NamespaceScheme nsScheme = new NamespaceScheme(Arrays.asList(foo), null,
        policyMap);
    ClusterScheme clusterScheme = new ClusterScheme(DEFAULT_CONF,
        NUM_DATANODES, REPL, genStorageTypes(NUM_DATANODES), null);
    MigrationTest test = new MigrationTest(clusterScheme, nsScheme);
    runTest(test);
  }
}
