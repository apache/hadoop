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
package org.apache.hadoop.hdfs.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Performance test of the new DFSNetworkTopology chooseRandom.
 *
 * NOTE that the tests are not for correctness but for performance comparison,
 * so the tests are printing and writing down values rather than doing assertion
 * checks or timeout checks. Therefore, it is pointless to run these
 * tests without something reading the value. So disabled the tests to for now,
 * anyone interested in looking at the numbers can enable them.
 */
@Ignore
public class TestDFSNetworkTopologyPerformance {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestDFSNetworkTopologyPerformance.class);
  private static NetworkTopology cluster;
  private static DFSNetworkTopology dfscluster;
  private DatanodeDescriptor[] dataNodes;

  private final static int NODE_NUM = 2000;
  private final static int OP_NUM = 20000;

  private final static int L1_NUM = 5;
  private final static int L2_NUM = 10;
  private final static int L3_NUM = 10;

  private final static float NS_TO_MS = 1000000;

  private final static Random RANDOM = new Random();

  private Node node;
  private long totalStart;
  private long totalEnd;
  private int totalTrials;
  private float totalMs;
  private Set<Node> excluded;
  private static String[] racks;
  private static String[] hosts;
  private static StorageType[] types;

  private static long[] records;
  private long localStart;
  private long localEnd;


  @BeforeClass
  public static void init() throws Exception {
    racks = new String[NODE_NUM];
    hosts = new String[NODE_NUM];
    types = new StorageType[NODE_NUM];
    records = new long[OP_NUM];
    for (int i = 0; i < NODE_NUM; i++) {
      racks[i] = getRandLocation();
      hosts[i] = "host" + i;
    }
  }

  @Before
  public void setup() throws Exception {
    cluster = NetworkTopology.getInstance(new Configuration());
    dfscluster = DFSNetworkTopology.getInstance(new Configuration());
    excluded = new HashSet<>();
  }

  /**
   * In this test, all the node types are uniformly distributed. i.e. a node
   * has equal of being DISK, RAM_DISK, SSD and ARCHIVE. This test did two tests
   * first test runs the old chooseRandom approach, the second test runs the
   * new chooseRandom approach.
   * @throws Exception
   */
  @Test
  public void testUniformStorageType() throws Exception {
    EnumSet<StorageType> missingTypes = EnumSet.allOf(StorageType.class);

    for (int i = 0; i < NODE_NUM; i++) {
      types[i] = getRandType();
      missingTypes.remove(types);
    }

    if (missingTypes.size() != 0) {
      // it is statistically very, very rare that missingTypes is not length 0
      // at this point. But if it happened, we do the trick by inserting 1 per
      // remaining types randomly into the array, such that it is guaranteed all
      // types exist
      Set<Integer> usedIdx = new HashSet<>();
      int idx;
      for (StorageType type : missingTypes) {
        do {
          idx = RANDOM.nextInt(NODE_NUM);
        } while (usedIdx.contains(idx));
        usedIdx.add(idx);
        types[idx] = type;
      }
    }

    addNodeByTypes(types);

    // wait a bit for things to become stable
    Thread.sleep(1000);
    printMemUsage("before test1");

    totalStart = System.nanoTime();
    totalTrials = 0;
    for (int i = 0; i < OP_NUM; i++) {
      StorageType type = StorageType.values()[i%StorageType.values().length];
      // mimic the behaviour of current code:
      // 1. chooseRandom on NetworkTopology
      // 2. if it satisfies, we are good, break
      // 3. if not, add to excluded, try again
      // The reason of doing simulated behaviour is that in NetworkTopology,
      // the code that does this is a wrapper method that is fairly complex
      // and does something more than just chooseRandom. So it would be unfair
      // to the current code if we compare the wrapper with the new chooseRandom
      // because we should only compare the two chooseRandom methods.
      // However the way current chooseRandom works involves checking storage
      // type of return nodes. We still need to simulate this behaviour.
      localStart = System.nanoTime();
      do {
        totalTrials += 1;
        node = cluster.chooseRandom("", excluded);
        assertNotNull(node);
        if (isType(node, type)) {
          break;
        }
        excluded.add(node);
      } while (true);
      excluded.clear();
      localEnd = System.nanoTime();
      records[i] = localEnd - localStart;
    }
    totalEnd = System.nanoTime();
    totalMs = (totalEnd - totalStart)/NS_TO_MS;
    // 4 trials on average.
    LOG.info("total time: {} avg time: {} avg trials: {}",
        totalMs, totalMs / OP_NUM, (float)totalTrials / OP_NUM);

    // wait a bit for things to become stable
    Thread.sleep(1000);
    printMemUsage("after test1 before test2");

    totalStart = System.nanoTime();
    for (int i = 0; i < OP_NUM; i++) {
      StorageType type = StorageType.values()[i%StorageType.values().length];
      localStart = System.nanoTime();
      node = dfscluster.chooseRandomWithStorageType("", excluded, type);
      assertNotNull(node);
      // with dfs cluster, the returned is always already the required type;
      // add assertion mainly to make a more fair comparison
      assertTrue(isType(node, type));
      localEnd = System.nanoTime();
      records[i] = localEnd - localStart;
    }
    totalEnd = System.nanoTime();
    totalMs = (totalEnd - totalStart)/NS_TO_MS;
    LOG.info("total time: {} avg time: {}", totalMs, totalMs / OP_NUM);

    printMemUsage("after test2");
  }

  /**
   * There are two storage types DISK and ARCHIVE. DISK compose the majority and
   * ARCHIVE only compose 1/20 nodes, uniformly distributed. This test also runs
   * two tests, one with old approach and one with the new approach. Try to
   * search for ARCHIVE type devices. This test show how new approach can
   * outperform the old one.
   * @throws Exception
   */
  @Test
  public void testUnbalancedStorageType() throws Exception {
    for (int i = 0; i < NODE_NUM; i++) {
      types[i] = StorageType.DISK;
    }
    for (int i = 0; i < NODE_NUM/20; i++) {
      types[RANDOM.nextInt(NODE_NUM)] = StorageType.ARCHIVE;
    }
    addNodeByTypes(types);

    // wait a bit for things to become stable
    Thread.sleep(1000);
    printMemUsage("before test1");

    totalStart = System.nanoTime();
    totalTrials = 0;
    for (int i = 0; i < OP_NUM; i++) {
      // mimic the behaviour of current code:
      // 1. chooseRandom on NetworkTopology
      // 2. if it satisfies, we are good, break
      // 3. if not, add to excluded, try again
      localStart = System.nanoTime();
      do {
        totalTrials += 1;
        node = cluster.chooseRandom("", excluded);
        assertNotNull(node);
        if (isType(node, StorageType.ARCHIVE)) {
          break;
        }
        excluded.add(node);
      } while (true);
      excluded.clear();
      localEnd = System.nanoTime();
      records[i] = localEnd - localStart;
    }
    totalEnd = System.nanoTime();
    totalMs = (totalEnd - totalStart)/NS_TO_MS;
    // on average it takes 20 trials
    LOG.info("total time: {} avg time: {} avg trials: {}",
        totalMs, totalMs / OP_NUM, (float)totalTrials / OP_NUM);
    // wait a bit for things to become stable
    Thread.sleep(1000);
    printMemUsage("after test1 before test2");

    totalStart = System.nanoTime();
    for (int i = 0; i < OP_NUM; i++) {
      localStart = System.nanoTime();
      node = dfscluster.chooseRandomWithStorageType("", excluded,
          StorageType.ARCHIVE);
      assertNotNull(node);
      // with dfs cluster, the returned is always already the required type;
      // add assertion mainly to make a more fair comparison
      assertTrue(isType(node, StorageType.ARCHIVE));
      localEnd = System.nanoTime();
      records[i] = localEnd - localStart;
    }
    totalEnd = System.nanoTime();
    totalMs = (totalEnd - totalStart)/NS_TO_MS;
    LOG.info("total time: {} avg time: {}", totalMs, totalMs/OP_NUM);

    printMemUsage("after test2");
  }

  /**
   * There is only one storage type (DISK) in the cluster. And tries to select
   * a DISK devices every time also. One approach will always succeed in one
   * call. This test shows how bad the new approach is compared to the optimal
   * situation.
   * @throws Exception
   */
  @Test
  public void testSameStorageType() throws Exception {
    for (int i = 0; i < NODE_NUM; i++) {
      types[i] = StorageType.DISK;
    }
    addNodeByTypes(types);

    // wait a bit for things to become stable
    Thread.sleep(1000);
    printMemUsage("before test1");

    totalStart = System.nanoTime();
    totalTrials = 0;
    for (int i = 0; i < OP_NUM; i++) {
      // mimic the behaviour of current code:
      // 1. chooseRandom on NetworkTopology
      // 2. if it satisfies, we are good, break
      // 3. if not, add to excluded, try again
      do {
        totalTrials += 1;
        node = cluster.chooseRandom("", excluded);
        assertNotNull(node);
        if (isType(node, StorageType.DISK)) {
          break;
        }
        excluded.add(node);
      } while (true);
      excluded.clear();
    }
    totalEnd = System.nanoTime();
    totalMs = (totalEnd - totalStart)/NS_TO_MS;
    // on average it takes 20 trials
    LOG.info("total time: {} avg time: {} avg trials: {}",
        totalMs, totalMs / OP_NUM, (float)totalTrials / OP_NUM);
    // wait a bit for things to become stable
    Thread.sleep(1000);
    printMemUsage("after test1 before test2");

    totalStart = System.nanoTime();
    for (int i = 0; i < OP_NUM; i++) {
      node = dfscluster.chooseRandomWithStorageType("", excluded,
          StorageType.DISK);
      assertNotNull(node);
      // with dfs cluster, the returned is always already the required type;
      // add assertion mainly to make a more fair comparison
      assertTrue(isType(node, StorageType.DISK));
    }
    totalEnd = System.nanoTime();
    totalMs = (totalEnd - totalStart) / NS_TO_MS;
    LOG.info("total time: {} avg time: {}", totalMs, totalMs / OP_NUM);

    printMemUsage("after test2");
  }

  private boolean coinFlip(double chance) {
    return RANDOM.nextInt(NODE_NUM) <= NODE_NUM*chance;
  }

  /**
   * This is a helper test, can be changed to different distribution each run.
   * Changing the value percentage = X where X is between 0.0 to 1.0 will result
   * in different outcomes. This is to help understanding what is the boundary
   * that makes the new approach better than the old one. The lower X is, the
   * less likely the old approach will succeed in one call, in which case the
   * new approach is more likely to be better.
   * @throws Exception
   */
  @Test
  public void testPercentageStorageType() throws Exception {
    double percentage = 0.9;
    for (int i = 0; i < NODE_NUM; i++) {
      if (coinFlip(percentage)) {
        types[i] = StorageType.ARCHIVE;
      } else {
        types[i] = StorageType.DISK;
      }
    }
    addNodeByTypes(types);

    // wait a bit for things to become stable
    Thread.sleep(1000);
    printMemUsage("before test1");

    totalStart = System.nanoTime();
    totalTrials = 0;
    for (int i = 0; i < OP_NUM; i++) {
      // mimic the behaviour of current code:
      // 1. chooseRandom on NetworkTopology
      // 2. if it satisfies, we are good, break
      // 3. if not, add to excluded, try again
      localStart = System.nanoTime();
      do {
        totalTrials += 1;
        node = cluster.chooseRandom("", excluded);
        assertNotNull(node);
        if (isType(node, StorageType.ARCHIVE)) {
          break;
        }
        excluded.add(node);
      } while (true);
      excluded.clear();
      localEnd = System.nanoTime();
      records[i] = localEnd - localStart;
    }
    totalEnd = System.nanoTime();
    totalMs = (totalEnd - totalStart) / NS_TO_MS;
    // on average it takes 20 trials
    LOG.info("total time: {} avg time: {} avg trials: {}",
        totalMs, totalMs / OP_NUM, (float)totalTrials / OP_NUM);
    // wait a bit for things to become stable
    Thread.sleep(1000);
    printMemUsage("after test1 before test2");

    totalStart = System.nanoTime();
    for (int i = 0; i < OP_NUM; i++) {
      localStart = System.nanoTime();
      node = dfscluster.chooseRandomWithStorageType("", excluded,
          StorageType.ARCHIVE);
      assertNotNull(node);
      // with dfs cluster, the returned is always already the required type;
      // add assertion mainly to make a more fair comparison
      assertTrue(isType(node, StorageType.ARCHIVE));
      localEnd = System.nanoTime();
      records[i] = localEnd - localStart;
    }
    totalEnd = System.nanoTime();
    totalMs = (totalEnd - totalStart) / NS_TO_MS;
    LOG.info("total time: {} avg time: {}", totalMs, totalMs / OP_NUM);

    printMemUsage("after test2");
  }

  /**
   * Similar to the previous test, change the percentage value to understand
   * the performance of the mixed approach. More specifically, this test takes
   * the approach that, it uses old approach for the first try, only if the
   * old approach failed in the first try, it makes another call with the
   * new approach. There is no comparison within this test.
   * @throws Exception
   */
  @Test
  public void testPercentageStorageTypeWithMixedTopology() throws Exception {
    double percentage = 0.9;
    for (int i = 0; i < NODE_NUM; i++) {
      if (coinFlip(percentage)) {
        types[i] = StorageType.ARCHIVE;
      } else {
        types[i] = StorageType.DISK;
      }
    }
    addNodeByTypes(types);

    // wait a bit for things to become stable
    Thread.sleep(1000);
    printMemUsage("before test1");

    totalStart = System.nanoTime();
    totalTrials = 0;
    for (int i = 0; i < OP_NUM; i++) {
      // mimic the behavior of current code:
      // 1. chooseRandom on NetworkTopology
      // 2. if it satisfies, we are good, do the next operation
      // 3. if not, chooseRandom on DFSNetworkTopology
      localStart = System.nanoTime();

      totalTrials += 1;
      node = cluster.chooseRandom("", excluded);
      assertNotNull(node);
      if (!isType(node, StorageType.ARCHIVE)) {
        totalTrials += 1;
        excluded.add(node);
        node = dfscluster.chooseRandomWithStorageType("", excluded,
            StorageType.ARCHIVE);
      }
      assertTrue(isType(node, StorageType.ARCHIVE));

      excluded.clear();
      localEnd = System.nanoTime();
      records[i] = localEnd - localStart;
    }
    totalEnd = System.nanoTime();
    totalMs = (totalEnd - totalStart)/NS_TO_MS;
    LOG.info("total time: {} avg time: {} avg trials: {}",
        totalMs, totalMs / OP_NUM, (float)totalTrials / OP_NUM);
    // wait a bit for things to become stable
    Thread.sleep(1000);
    printMemUsage("test StorageType with mixed topology.");
  }

  /**
   * Print out the memory usage statistics. Note that this is an estimation by
   * Java Runtime. Should not take the actual value too serious but more focus
   * on the relative changes.
   * @param message a prefix message
   * @throws Exception throws exception
   */
  private void printMemUsage(String message) throws Exception {
    Runtime runtime = Runtime.getRuntime();
    NumberFormat format = NumberFormat.getInstance();
    StringBuilder sb = new StringBuilder();
    sb.append(message);

    long maxMemory = runtime.maxMemory();
    long allocatedMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();

    sb.append("\nfree memory: " + format.format(freeMemory / 1024));
    sb.append("\nallocated memory: " + format.format(allocatedMemory / 1024));
    sb.append("\nmax memory: " + format.format(maxMemory / 1024));
    sb.append("\ntotal free memory: " + format.format(
        (freeMemory + (maxMemory - allocatedMemory)) / 1024));
    LOG.info(sb.toString());
  }

  private void addNodeByTypes(StorageType[] allTypes) {
    DatanodeStorageInfo[] storages =
        DFSTestUtil.createDatanodeStorageInfos(
            NODE_NUM, racks, hosts, allTypes);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);
    for (int i = 0; i < NODE_NUM; i++) {
      cluster.add(dataNodes[i]);
      dfscluster.add(dataNodes[i]);
    }
  }

  private static String getRandLocation() {
    int l1 = RANDOM.nextInt(L1_NUM) + 1;
    int l2 = RANDOM.nextInt(L2_NUM) + 1;
    int l3 = RANDOM.nextInt(L3_NUM) + 1;
    return String.format("/%d/%d/%d", l1, l2, l3);
  }

  private StorageType getRandType() {
    int len = StorageType.values().length;
    return StorageType.values()[RANDOM.nextInt(len)];
  }

  private boolean isType(Node n, StorageType type) {
    // no need to check n == null, because it's done by the caller
    if (!(n instanceof DatanodeDescriptor)) {
      return false;
    }
    DatanodeDescriptor dnDescriptor = (DatanodeDescriptor)n;
    return dnDescriptor.hasStorageType(type);
  }
}
