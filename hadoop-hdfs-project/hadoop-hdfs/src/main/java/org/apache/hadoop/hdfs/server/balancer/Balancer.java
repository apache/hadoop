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
package org.apache.hadoop.hdfs.server.balancer;

import static org.apache.hadoop.hdfs.protocol.BlockType.CONTIGUOUS;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Source;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Task;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Util;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicies;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.util.Preconditions;

/** <p>The balancer is a tool that balances disk space usage on an HDFS cluster
 * when some datanodes become full or when new empty nodes join the cluster.
 * The tool is deployed as an application program that can be run by the 
 * cluster administrator on a live HDFS cluster while applications
 * adding and deleting files.
 * 
 * <p>SYNOPSIS
 * <pre>
 * To start:
 *      bin/start-balancer.sh [-threshold {@literal <threshold>}]
 *      Example: bin/ start-balancer.sh 
 *                     start the balancer with a default threshold of 10%
 *               bin/ start-balancer.sh -threshold 5
 *                     start the balancer with a threshold of 5%
 *               bin/ start-balancer.sh -idleiterations 20
 *                     start the balancer with maximum 20 consecutive idle iterations
 *               bin/ start-balancer.sh -idleiterations -1
 *                     run the balancer with default threshold infinitely
 * To stop:
 *      bin/ stop-balancer.sh
 * </pre>
 * 
 * <p>DESCRIPTION
 * <p>The threshold parameter is a fraction in the range of (1%, 100%) with a 
 * default value of 10%. The threshold sets a target for whether the cluster 
 * is balanced. A cluster is balanced if for each datanode, the utilization 
 * of the node (ratio of used space at the node to total capacity of the node) 
 * differs from the utilization of the (ratio of used space in the cluster 
 * to total capacity of the cluster) by no more than the threshold value. 
 * The smaller the threshold, the more balanced a cluster will become. 
 * It takes more time to run the balancer for small threshold values. 
 * Also for a very small threshold the cluster may not be able to reach the 
 * balanced state when applications write and delete files concurrently.
 * 
 * <p>The tool moves blocks from highly utilized datanodes to poorly 
 * utilized datanodes iteratively. In each iteration a datanode moves or 
 * receives no more than the lesser of 10G bytes or the threshold fraction 
 * of its capacity. Each iteration runs no more than 20 minutes.
 * At the end of each iteration, the balancer obtains updated datanodes
 * information from the namenode.
 * 
 * <p>A system property that limits the balancer's use of bandwidth is 
 * defined in the default configuration file:
 * <pre>
 * &lt;property&gt;
 *   &lt;name&gt;dfs.datanode.balance.bandwidthPerSec&lt;/name&gt;
 *   &lt;value&gt;1048576&lt;/value&gt;
 * &lt;description&gt;  Specifies the maximum bandwidth that each datanode
 * can utilize for the balancing purpose in term of the number of bytes 
 * per second.
 * &lt;/description&gt;
 * &lt;/property&gt;
 * </pre>
 * 
 * <p>This property determines the maximum speed at which a block will be 
 * moved from one datanode to another. The default value is 1MB/s. The higher 
 * the bandwidth, the faster a cluster can reach the balanced state, 
 * but with greater competition with application processes. If an 
 * administrator changes the value of this property in the configuration 
 * file, the change is observed when HDFS is next restarted.
 * 
 * <p>MONITERING BALANCER PROGRESS
 * <p>After the balancer is started, an output file name where the balancer 
 * progress will be recorded is printed on the screen.  The administrator 
 * can monitor the running of the balancer by reading the output file. 
 * The output shows the balancer's status iteration by iteration. In each 
 * iteration it prints the starting time, the iteration number, the total 
 * number of bytes that have been moved in the previous iterations, 
 * the total number of bytes that are left to move in order for the cluster 
 * to be balanced, and the number of bytes that are being moved in this 
 * iteration. Normally "Bytes Already Moved" is increasing while "Bytes Left 
 * To Move" is decreasing.
 * 
 * <p>Running multiple instances of the balancer in an HDFS cluster is 
 * prohibited by the tool.
 * 
 * <p>The balancer automatically exits when any of the following five 
 * conditions is satisfied:
 * <ol>
 * <li>The cluster is balanced;
 * <li>No block can be moved;
 * <li>No block has been moved for specified consecutive iterations (5 by default);
 * <li>An IOException occurs while communicating with the namenode;
 * <li>Another balancer is running.
 * </ol>
 * 
 * <p>Upon exit, a balancer returns an exit code and prints one of the 
 * following messages to the output file in corresponding to the above exit 
 * reasons:
 * <ol>
 * <li>The cluster is balanced. Exiting
 * <li>No block can be moved. Exiting...
 * <li>No block has been moved for specified iterations (5 by default). Exiting...
 * <li>Received an IO exception: failure reason. Exiting...
 * <li>Another balancer is running. Exiting...
 * </ol>
 * 
 * <p>The administrator can interrupt the execution of the balancer at any 
 * time by running the command "stop-balancer.sh" on the machine where the 
 * balancer is running.
 */

@InterfaceAudience.Private
public class Balancer {
  static final Logger LOG = LoggerFactory.getLogger(Balancer.class);

  static final Path BALANCER_ID_PATH = new Path("/system/balancer.id");

  private static final String USAGE = "Usage: hdfs balancer"
      + "\n\t[-policy <policy>]\tthe balancing policy: "
      + BalancingPolicy.Node.INSTANCE.getName() + " or "
      + BalancingPolicy.Pool.INSTANCE.getName()
      + "\n\t[-threshold <threshold>]\tPercentage of disk capacity"
      + "\n\t[-exclude [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tExcludes the specified datanodes."
      + "\n\t[-include [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tIncludes only the specified datanodes."
      + "\n\t[-source [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tPick only the specified datanodes as source nodes."
      + "\n\t[-blockpools <comma-separated list of blockpool ids>]"
      + "\tThe balancer will only run on blockpools included in this list."
      + "\n\t[-idleiterations <idleiterations>]"
      + "\tNumber of consecutive idle iterations (-1 for Infinite) before "
      + "exit."
      + "\n\t[-runDuringUpgrade]"
      + "\tWhether to run the balancer during an ongoing HDFS upgrade."
      + "This is usually not desired since it will not affect used space "
      + "on over-utilized machines."
      + "\n\t[-asService]\tRun as a long running service."
      + "\n\t[-sortTopNodes]"
      + "\tSort datanodes based on the utilization so "
      + "that highly utilized datanodes get scheduled first."
      + "\n\t[-hotBlockTimeInterval]\tprefer to move cold blocks.";

  @VisibleForTesting
  private static volatile boolean serviceRunning = false;

  private static final AtomicInteger EXCEPTIONS_SINCE_LAST_BALANCE =
      new AtomicInteger(0);
  private static final AtomicInteger
      FAILED_TIMES_SINCE_LAST_SUCCESSFUL_BALANCE = new AtomicInteger(0);

  private final Dispatcher dispatcher;
  private final NameNodeConnector nnc;
  private final BalancingPolicy policy;
  private final Set<String> sourceNodes;
  private final boolean runDuringUpgrade;
  private final double threshold;
  private final long maxSizeToMove;
  private final long defaultBlockSize;
  private final boolean sortTopNodes;
  private final BalancerMetrics metrics;

  // all data node lists
  private final Collection<Source> overUtilized = new LinkedList<Source>();
  private final Collection<Source> aboveAvgUtilized = new LinkedList<Source>();
  private final Collection<StorageGroup> belowAvgUtilized
      = new LinkedList<StorageGroup>();
  private final Collection<StorageGroup> underUtilized
      = new LinkedList<StorageGroup>();

  /* Check that this Balancer is compatible with the Block Placement Policy
   * used by the Namenode.
   */
  private static void checkReplicationPolicyCompatibility(Configuration conf
      ) throws UnsupportedActionException {
    BlockPlacementPolicies placementPolicies =
        new BlockPlacementPolicies(conf, null, null, null);
    if (!(placementPolicies.getPolicy(CONTIGUOUS) instanceof
        BlockPlacementPolicyDefault)) {
      throw new UnsupportedActionException(
          "Balancer without BlockPlacementPolicyDefault");
    }
  }

  static long getLong(Configuration conf, String key, long defaultValue) {
    final long v = conf.getLong(key, defaultValue);
    LOG.info(key + " = " + v + " (default=" + defaultValue + ")");
    if (v <= 0) {
      throw new HadoopIllegalArgumentException(key + " = " + v  + " <= " + 0);
    }
    return v;
  }

  static long getLongBytes(Configuration conf, String key, long defaultValue) {
    final long v = conf.getLongBytes(key, defaultValue);
    LOG.info(key + " = " + v + " (default=" + defaultValue + ")");
    if (v <= 0) {
      throw new HadoopIllegalArgumentException(key + " = " + v  + " <= " + 0);
    }
    return v;
  }

  static int getInt(Configuration conf, String key, int defaultValue) {
    final int v = conf.getInt(key, defaultValue);
    LOG.info(key + " = " + v + " (default=" + defaultValue + ")");
    if (v <= 0) {
      throw new HadoopIllegalArgumentException(key + " = " + v  + " <= " + 0);
    }
    return v;
  }

  static int getExceptionsSinceLastBalance() {
    return EXCEPTIONS_SINCE_LAST_BALANCE.get();
  }

  static int getFailedTimesSinceLastSuccessfulBalance() {
    return FAILED_TIMES_SINCE_LAST_SUCCESSFUL_BALANCE.get();
  }

  /**
   * Construct a balancer.
   * Initialize balancer. It sets the value of the threshold, and 
   * builds the communication proxies to
   * namenode as a client and a secondary namenode and retry proxies
   * when connection fails.
   */
  Balancer(NameNodeConnector theblockpool, BalancerParameters p,
      Configuration conf) {
    // NameNode configuration parameters for balancing
    getInt(conf, DFSConfigKeys.DFS_NAMENODE_GETBLOCKS_MAX_QPS_KEY,
        DFSConfigKeys.DFS_NAMENODE_GETBLOCKS_MAX_QPS_DEFAULT);
    final long movedWinWidth = getLong(conf,
        DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY,
        DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_DEFAULT);
    final int moverThreads = getInt(conf,
        DFSConfigKeys.DFS_BALANCER_MOVERTHREADS_KEY,
        DFSConfigKeys.DFS_BALANCER_MOVERTHREADS_DEFAULT);
    final int dispatcherThreads = getInt(conf,
        DFSConfigKeys.DFS_BALANCER_DISPATCHERTHREADS_KEY,
        DFSConfigKeys.DFS_BALANCER_DISPATCHERTHREADS_DEFAULT);
    final long getBlocksSize = getLongBytes(conf,
        DFSConfigKeys.DFS_BALANCER_GETBLOCKS_SIZE_KEY,
        DFSConfigKeys.DFS_BALANCER_GETBLOCKS_SIZE_DEFAULT);
    final long getBlocksMinBlockSize = getLongBytes(conf,
        DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_DEFAULT);
    final int blockMoveTimeout = conf.getInt(
        DFSConfigKeys.DFS_BALANCER_BLOCK_MOVE_TIMEOUT,
        DFSConfigKeys.DFS_BALANCER_BLOCK_MOVE_TIMEOUT_DEFAULT);
    final int maxNoMoveInterval = conf.getInt(
        DFSConfigKeys.DFS_BALANCER_MAX_NO_MOVE_INTERVAL_KEY,
        DFSConfigKeys.DFS_BALANCER_MAX_NO_MOVE_INTERVAL_DEFAULT);
    final long maxIterationTime = conf.getLong(
        DFSConfigKeys.DFS_BALANCER_MAX_ITERATION_TIME_KEY,
        DFSConfigKeys.DFS_BALANCER_MAX_ITERATION_TIME_DEFAULT);
    /**
     * Balancer prefer to get blocks which are belong to the cold files
     * created before this time period.
     */
    final long hotBlockTimeInterval =
        p.getHotBlockTimeInterval() != 0L ? p.getHotBlockTimeInterval() :
            conf.getTimeDuration(
            DFSConfigKeys.DFS_BALANCER_GETBLOCKS_HOT_TIME_INTERVAL_KEY,
            DFSConfigKeys.DFS_BALANCER_GETBLOCKS_HOT_TIME_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);

    // DataNode configuration parameters for balancing
    final int maxConcurrentMovesPerNode = getInt(conf,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);
    getLongBytes(conf, DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT);

    this.nnc = theblockpool;
    this.dispatcher =
        new Dispatcher(theblockpool, p.getIncludedNodes(),
            p.getExcludedNodes(), movedWinWidth, moverThreads,
            dispatcherThreads, maxConcurrentMovesPerNode, getBlocksSize,
            getBlocksMinBlockSize, blockMoveTimeout, maxNoMoveInterval,
            maxIterationTime, hotBlockTimeInterval, conf);
    this.threshold = p.getThreshold();
    this.policy = p.getBalancingPolicy();
    this.sourceNodes = p.getSourceNodes();
    this.runDuringUpgrade = p.getRunDuringUpgrade();
    this.sortTopNodes = p.getSortTopNodes();

    this.maxSizeToMove = getLongBytes(conf,
        DFSConfigKeys.DFS_BALANCER_MAX_SIZE_TO_MOVE_KEY,
        DFSConfigKeys.DFS_BALANCER_MAX_SIZE_TO_MOVE_DEFAULT);
    this.defaultBlockSize = getLongBytes(conf,
        DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    this.metrics = BalancerMetrics.create(this);
  }
  
  private static long getCapacity(DatanodeStorageReport report, StorageType t) {
    long capacity = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (r.getStorage().getStorageType() == t) {
        capacity += r.getCapacity();
      }
    }
    return capacity;
  }

  private long getRemaining(DatanodeStorageReport report, StorageType t) {
    long remaining = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (r.getStorage().getStorageType() == t) {
        if (r.getRemaining() >= defaultBlockSize) {
          remaining += r.getRemaining();
        }
      }
    }
    return remaining;
  }

  /**
   * Given a datanode storage set, build a network topology and decide
   * over-utilized storages, above average utilized storages, 
   * below average utilized storages, and underutilized storages. 
   * The input datanode storage set is shuffled in order to randomize
   * to the storage matching later on.
   *
   * @return the number of bytes needed to move in order to balance the cluster.
   */
  private long init(List<DatanodeStorageReport> reports) {
    // compute average utilization
    for (DatanodeStorageReport r : reports) {
      policy.accumulateSpaces(r);
    }
    policy.initAvgUtilization();
    // Store the capacity % of over utilized nodes for sorting, if needed.
    Map<Source, Double> overUtilizedPercentage = new HashMap<>();

    // create network topology and classify utilization collections: 
    //   over-utilized, above-average, below-average and under-utilized.
    long overLoadedBytes = 0L, underLoadedBytes = 0L;
    for(DatanodeStorageReport r : reports) {
      final DDatanode dn = dispatcher.newDatanode(r.getDatanodeInfo());
      final boolean isSource = Util.isIncluded(sourceNodes, dn.getDatanodeInfo());
      for(StorageType t : StorageType.getMovableTypes()) {
        final Double utilization = policy.getUtilization(r, t);
        if (utilization == null) { // datanode does not have such storage type
          continue;
        }
        
        final double average = policy.getAvgUtilization(t);
        if (utilization >= average && !isSource) {
          LOG.info(dn + "[" + t + "] has utilization=" + utilization
              + " >= average=" + average
              + " but it is not specified as a source; skipping it.");
          continue;
        }

        final double utilizationDiff = utilization - average;
        final long capacity = getCapacity(r, t);
        final double thresholdDiff = Math.abs(utilizationDiff) - threshold;
        final long maxSize2Move = computeMaxSize2Move(capacity,
            getRemaining(r, t), utilizationDiff, maxSizeToMove);

        final StorageGroup g;
        if (utilizationDiff > 0) {
          final Source s = dn.addSource(t, maxSize2Move, dispatcher);
          if (thresholdDiff <= 0) { // within threshold
            aboveAvgUtilized.add(s);
          } else {
            overLoadedBytes += percentage2bytes(thresholdDiff, capacity);
            overUtilized.add(s);
            overUtilizedPercentage.put(s, utilization);
          }
          g = s;
        } else {
          g = dn.addTarget(t, maxSize2Move);
          if (thresholdDiff <= 0) { // within threshold
            belowAvgUtilized.add(g);
          } else {
            underLoadedBytes += percentage2bytes(thresholdDiff, capacity);
            underUtilized.add(g);
          }
        }
        dispatcher.getStorageGroupMap().put(g);
      }
    }

    if (sortTopNodes) {
      sortOverUtilized(overUtilizedPercentage);
    }

    logUtilizationCollections();
    metrics.setNumOfOverUtilizedNodes(overUtilized.size());
    metrics.setNumOfUnderUtilizedNodes(underUtilized.size());
    
    Preconditions.checkState(dispatcher.getStorageGroupMap().size()
        == overUtilized.size() + underUtilized.size() + aboveAvgUtilized.size()
           + belowAvgUtilized.size(),
        "Mismatched number of storage groups");
    
    // return number of bytes to be moved in order to make the cluster balanced
    return Math.max(overLoadedBytes, underLoadedBytes);
  }

  private void sortOverUtilized(Map<Source, Double> overUtilizedPercentage) {
    Preconditions.checkState(overUtilized instanceof List,
        "Collection overUtilized is not a List.");

    LOG.info("Sorting over-utilized nodes by capacity" +
        " to bring down top used datanode capacity faster");

    List<Source> list = (List<Source>) overUtilized;
    list.sort(
        (Source source1, Source source2) ->
            (Double.compare(overUtilizedPercentage.get(source2),
                overUtilizedPercentage.get(source1)))
    );
  }

  private static long computeMaxSize2Move(final long capacity, final long remaining,
      final double utilizationDiff, final long max) {
    final double diff = Math.abs(utilizationDiff);
    long maxSizeToMove = percentage2bytes(diff, capacity);
    if (utilizationDiff < 0) {
      maxSizeToMove = Math.min(remaining, maxSizeToMove);
    }
    return Math.min(max, maxSizeToMove);
  }

  private static long percentage2bytes(double percentage, long capacity) {
    Preconditions.checkArgument(percentage >= 0, "percentage = %s < 0",
        percentage);
    return (long)(percentage * capacity / 100.0);
  }

  /* log the over utilized & under utilized nodes */
  private void logUtilizationCollections() {
    logUtilizationCollection("over-utilized", overUtilized);
    if (LOG.isTraceEnabled()) {
      logUtilizationCollection("above-average", aboveAvgUtilized);
      logUtilizationCollection("below-average", belowAvgUtilized);
    }
    logUtilizationCollection("underutilized", underUtilized);
  }

  private static <T extends StorageGroup>
      void logUtilizationCollection(String name, Collection<T> items) {
    LOG.info(items.size() + " " + name + ": " + items);
  }

  /**
   * Decide all <source, target> pairs and
   * the number of bytes to move from a source to a target
   * Maximum bytes to be moved per storage group is
   * min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
   * @return total number of bytes to move in this iteration
   */
  private long chooseStorageGroups() {
    // First, match nodes on the same node group if cluster is node group aware
    if (dispatcher.getCluster().isNodeGroupAware()) {
      chooseStorageGroups(Matcher.SAME_NODE_GROUP);
    }
    
    // Then, match nodes on the same rack
    chooseStorageGroups(Matcher.SAME_RACK);
    // At last, match all remaining nodes
    chooseStorageGroups(Matcher.ANY_OTHER);
    
    return dispatcher.bytesToMove();
  }

  /** Decide all <source, target> pairs according to the matcher. */
  private void chooseStorageGroups(final Matcher matcher) {
    /* first step: match each overUtilized datanode (source) to
     * one or more underUtilized datanodes (targets).
     */
    LOG.info("chooseStorageGroups for " + matcher + ": overUtilized => underUtilized");
    chooseStorageGroups(overUtilized, underUtilized, matcher);
    
    /* match each remaining overutilized datanode (source) to 
     * below average utilized datanodes (targets).
     * Note only overutilized datanodes that haven't had that max bytes to move
     * satisfied in step 1 are selected
     */
    LOG.info("chooseStorageGroups for " + matcher + ": overUtilized => belowAvgUtilized");
    chooseStorageGroups(overUtilized, belowAvgUtilized, matcher);

    /* match each remaining underutilized datanode (target) to 
     * above average utilized datanodes (source).
     * Note only underutilized datanodes that have not had that max bytes to
     * move satisfied in step 1 are selected.
     */
    LOG.info("chooseStorageGroups for " + matcher + ": underUtilized => aboveAvgUtilized");
    chooseStorageGroups(underUtilized, aboveAvgUtilized, matcher);
  }

  /**
   * For each datanode, choose matching nodes from the candidates. Either the
   * datanodes or the candidates are source nodes with (utilization > Avg), and
   * the others are target nodes with (utilization < Avg).
   */
  private <G extends StorageGroup, C extends StorageGroup>
      void chooseStorageGroups(Collection<G> groups, Collection<C> candidates,
          Matcher matcher) {
    for(final Iterator<G> i = groups.iterator(); i.hasNext();) {
      final G g = i.next();
      for(; choose4One(g, candidates, matcher); );
      if (!g.hasSpaceForScheduling()) {
        i.remove();
      }
    }
  }

  /**
   * For the given datanode, choose a candidate and then schedule it.
   * @return true if a candidate is chosen; false if no candidates is chosen.
   */
  private <C extends StorageGroup> boolean choose4One(StorageGroup g,
      Collection<C> candidates, Matcher matcher) {
    final Iterator<C> i = candidates.iterator();
    final C chosen = chooseCandidate(g, i, matcher);
  
    if (chosen == null) {
      return false;
    }
    if (g instanceof Source) {
      matchSourceWithTargetToMove((Source)g, chosen);
    } else {
      matchSourceWithTargetToMove((Source)chosen, g);
    }
    if (!chosen.hasSpaceForScheduling()) {
      i.remove();
    }
    return true;
  }
  
  private void matchSourceWithTargetToMove(Source source, StorageGroup target) {
    long size = Math.min(source.availableSizeToMove(), target.availableSizeToMove());
    final Task task = new Task(target, size);
    source.addTask(task);
    target.incScheduledSize(task.getSize());
    dispatcher.add(source, target);
    LOG.info("Decided to move "+StringUtils.byteDesc(size)+" bytes from "
        + source.getDisplayName() + " to " + target.getDisplayName());
  }
  
  /** Choose a candidate for the given datanode. */
  private <G extends StorageGroup, C extends StorageGroup>
      C chooseCandidate(G g, Iterator<C> candidates, Matcher matcher) {
    if (g.hasSpaceForScheduling()) {
      for(; candidates.hasNext(); ) {
        final C c = candidates.next();
        if (!c.hasSpaceForScheduling()) {
          candidates.remove();
        } else if (matchStorageGroups(c, g, matcher)) {
          return c;
        }
      }
    }
    return null;
  }

  private boolean matchStorageGroups(StorageGroup left, StorageGroup right,
      Matcher matcher) {
    return left.getStorageType() == right.getStorageType()
        && matcher.match(dispatcher.getCluster(),
            left.getDatanodeInfo(), right.getDatanodeInfo());
  }

  /* reset all fields in a balancer preparing for the next iteration */
  void resetData(Configuration conf) {
    this.overUtilized.clear();
    this.aboveAvgUtilized.clear();
    this.belowAvgUtilized.clear();
    this.underUtilized.clear();
    this.policy.reset();
    dispatcher.reset(conf);
  }

  NameNodeConnector getNnc() {
    return nnc;
  }

  static class Result {
    private final ExitStatus exitStatus;
    private final long bytesLeftToMove;
    private final long bytesBeingMoved;
    private final long bytesAlreadyMoved;
    private final long blocksMoved;

    Result(ExitStatus exitStatus, long bytesLeftToMove, long bytesBeingMoved,
           long bytesAlreadyMoved, long blocksMoved) {
      this.exitStatus = exitStatus;
      this.bytesLeftToMove = bytesLeftToMove;
      this.bytesBeingMoved = bytesBeingMoved;
      this.bytesAlreadyMoved = bytesAlreadyMoved;
      this.blocksMoved = blocksMoved;
    }

    public ExitStatus getExitStatus() {
      return exitStatus;
    }

    public long getBytesLeftToMove() {
      return bytesLeftToMove;
    }

    public long getBytesBeingMoved() {
      return bytesBeingMoved;
    }

    public long getBytesAlreadyMoved() {
      return bytesAlreadyMoved;
    }

    public long getBlocksMoved() {
      return blocksMoved;
    }

    void print(int iteration, NameNodeConnector nnc, PrintStream out) {
      out.printf("%-24s %10d  %19s  %18s  %17s  %17s  %s%n",
          DateFormat.getDateTimeInstance().format(new Date()), iteration,
          StringUtils.byteDesc(bytesAlreadyMoved),
          StringUtils.byteDesc(bytesLeftToMove),
          StringUtils.byteDesc(bytesBeingMoved),
          blocksMoved,
          nnc.getNameNodeUri());
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this)
          .append("exitStatus", exitStatus)
          .append("bytesLeftToMove", bytesLeftToMove)
          .append("bytesBeingMoved", bytesBeingMoved)
          .append("bytesAlreadyMoved", bytesAlreadyMoved)
          .append("blocksMoved", blocksMoved)
          .toString();
    }
  }

  Result newResult(ExitStatus exitStatus, long bytesLeftToMove, long bytesBeingMoved) {
    return new Result(exitStatus, bytesLeftToMove, bytesBeingMoved,
        dispatcher.getBytesMoved(), dispatcher.getBlocksMoved());
  }

  Result newResult(ExitStatus exitStatus) {
    return new Result(exitStatus, -1, -1, dispatcher.getBytesMoved(),
        dispatcher.getBlocksMoved());
  }

  /** Run an iteration for all datanodes. */
  Result runOneIteration() {
    try {
      metrics.setIterateRunning(true);
      final List<DatanodeStorageReport> reports = dispatcher.init();
      final long bytesLeftToMove = init(reports);
      metrics.setBytesLeftToMove(bytesLeftToMove);
      if (bytesLeftToMove == 0) {
        return newResult(ExitStatus.SUCCESS, bytesLeftToMove, 0);
      } else {
        LOG.info( "Need to move "+ StringUtils.byteDesc(bytesLeftToMove)
            + " to make the cluster balanced." );
      }

      // Should not run the balancer during an unfinalized upgrade, since moved
      // blocks are not deleted on the source datanode.
      if (!runDuringUpgrade && nnc.isUpgrading()) {
        System.err.println("Balancer exiting as upgrade is not finalized, "
            + "please finalize the HDFS upgrade before running the balancer.");
        LOG.error("Balancer exiting as upgrade is not finalized, "
            + "please finalize the HDFS upgrade before running the balancer.");
        return newResult(ExitStatus.UNFINALIZED_UPGRADE, bytesLeftToMove, -1);
      }

      /* Decide all the nodes that will participate in the block move and
       * the number of bytes that need to be moved from one node to another
       * in this iteration. Maximum bytes to be moved per node is
       * Min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
       */
      final long bytesBeingMoved = chooseStorageGroups();
      if (bytesBeingMoved == 0) {
        System.out.println("No block can be moved. Exiting...");
        return newResult(ExitStatus.NO_MOVE_BLOCK, bytesLeftToMove, bytesBeingMoved);
      } else {
        LOG.info("Will move {}  in this iteration for {}",
            StringUtils.byteDesc(bytesBeingMoved), nnc.toString());
        LOG.info("Total target DataNodes in this iteration: {}",
            dispatcher.moveTasksTotal());
      }

      /* For each pair of <source, target>, start a thread that repeatedly 
       * decide a block to be moved and its proxy source, 
       * then initiates the move until all bytes are moved or no more block
       * available to move.
       * Exit no byte has been moved for 5 consecutive iterations.
       */
      if (!dispatcher.dispatchAndCheckContinue()) {
        return newResult(ExitStatus.NO_MOVE_PROGRESS, bytesLeftToMove, bytesBeingMoved);
      }

      return newResult(ExitStatus.IN_PROGRESS, bytesLeftToMove, bytesBeingMoved);
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return newResult(ExitStatus.ILLEGAL_ARGUMENTS);
    } catch (IOException e) {
      System.out.println(e + ".  Exiting ...");
      return newResult(ExitStatus.IO_EXCEPTION);
    } catch (InterruptedException e) {
      System.out.println(e + ".  Exiting ...");
      return newResult(ExitStatus.INTERRUPTED);
    } finally {
      metrics.setIterateRunning(false);
      dispatcher.shutdownNow();
    }
  }

  /**
   * Balance all namenodes.
   * For each iteration,
   * for each namenode,
   * execute a {@link Balancer} to work through all datanodes once.  
   */
  static private int doBalance(Collection<URI> namenodes,
      Collection<String> nsIds, final BalancerParameters p, Configuration conf)
      throws IOException, InterruptedException {
    final long sleeptime =
        conf.getTimeDuration(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
            DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT,
            TimeUnit.SECONDS, TimeUnit.MILLISECONDS) * 2 +
        conf.getTimeDuration(
            DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
            DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_DEFAULT,
            TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    LOG.info("namenodes  = " + namenodes);
    LOG.info("parameters = " + p);
    LOG.info("included nodes = " + p.getIncludedNodes());
    LOG.info("excluded nodes = " + p.getExcludedNodes());
    LOG.info("source nodes = " + p.getSourceNodes());
    checkKeytabAndInit(conf);
    System.out.println("Time Stamp               Iteration#"
        + "  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved"
        + "  NameNode");
    
    List<NameNodeConnector> connectors = Collections.emptyList();
    try {
      connectors = NameNodeConnector.newNameNodeConnectors(namenodes, nsIds,
          Balancer.class.getSimpleName(), BALANCER_ID_PATH, conf,
          p.getMaxIdleIteration());
      boolean done = false;
      for(int iteration = 0; !done; iteration++) {
        done = true;
        Collections.shuffle(connectors);
        for(NameNodeConnector nnc : connectors) {
          if (p.getBlockPools().size() == 0
              || p.getBlockPools().contains(nnc.getBlockpoolID())) {
            final Balancer b = new Balancer(nnc, p, conf);
            final Result r = b.runOneIteration();
            r.print(iteration, nnc, System.out);

            // clean all lists
            b.resetData(conf);
            if (r.exitStatus == ExitStatus.IN_PROGRESS) {
              done = false;
            } else if (r.exitStatus != ExitStatus.SUCCESS) {
              // must be an error statue, return.
              return r.exitStatus.getExitCode();
            }
          } else {
            LOG.info("Skipping blockpool " + nnc.getBlockpoolID());
          }
          if (done) {
            System.out.println("The cluster is balanced. Exiting...");
          }
        }
        if (!done) {
          Thread.sleep(sleeptime);
        }
      }
    } finally {
      for(NameNodeConnector nnc : connectors) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  static int run(Collection<URI> namenodes, final BalancerParameters p,
                 Configuration conf) throws IOException, InterruptedException {
    return run(namenodes, null, p, conf);
  }

  static int run(Collection<URI> namenodes, Collection<String> nsIds,
      final BalancerParameters p, Configuration conf)
      throws IOException, InterruptedException {
    DefaultMetricsSystem.initialize("Balancer");
    JvmMetrics.create("Balancer",
        conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY),
        DefaultMetricsSystem.instance());
    if (!p.getRunAsService()) {
      return doBalance(namenodes, nsIds, p, conf);
    }
    if (!serviceRunning) {
      serviceRunning = true;
    } else {
      LOG.warn("Balancer already running as a long-service!");
      return ExitStatus.ALREADY_RUNNING.getExitCode();
    }

    long scheduleInterval = conf.getTimeDuration(
          DFSConfigKeys.DFS_BALANCER_SERVICE_INTERVAL_KEY,
          DFSConfigKeys.DFS_BALANCER_SERVICE_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
    int retryOnException =
          conf.getInt(DFSConfigKeys.DFS_BALANCER_SERVICE_RETRIES_ON_EXCEPTION,
              DFSConfigKeys.DFS_BALANCER_SERVICE_RETRIES_ON_EXCEPTION_DEFAULT);

    while (serviceRunning) {
      try {
        int retCode = doBalance(namenodes, nsIds, p, conf);
        if (retCode < 0) {
          LOG.info("Balance failed, error code: " + retCode);
          FAILED_TIMES_SINCE_LAST_SUCCESSFUL_BALANCE.incrementAndGet();
        } else {
          LOG.info("Balance succeed!");
          FAILED_TIMES_SINCE_LAST_SUCCESSFUL_BALANCE.set(0);
        }
        EXCEPTIONS_SINCE_LAST_BALANCE.set(0);
      } catch (Exception e) {
        if (EXCEPTIONS_SINCE_LAST_BALANCE.incrementAndGet()
            > retryOnException) {
          // The caller will process and log the exception
          throw e;
        }
        LOG.warn(
            "Encounter exception while do balance work. Already tried {} times",
            EXCEPTIONS_SINCE_LAST_BALANCE, e);
      }

      // sleep for next round, will retry for next round when it's interrupted
      LOG.info("Finished one round, will wait for {} for next round",
          time2Str(scheduleInterval));
      Thread.sleep(scheduleInterval);
    }
    DefaultMetricsSystem.shutdown();

    // normal stop
    return 0;
  }

  static void stop() {
    serviceRunning = false;
  }

  private static void checkKeytabAndInit(Configuration conf)
      throws IOException {
    if (conf.getBoolean(DFSConfigKeys.DFS_BALANCER_KEYTAB_ENABLED_KEY,
        DFSConfigKeys.DFS_BALANCER_KEYTAB_ENABLED_DEFAULT)) {
      LOG.info("Keytab is configured, will login using keytab.");
      UserGroupInformation.setConfiguration(conf);
      String addr = conf.get(DFSConfigKeys.DFS_BALANCER_ADDRESS_KEY,
          DFSConfigKeys.DFS_BALANCER_ADDRESS_DEFAULT);
      InetSocketAddress socAddr = NetUtils.createSocketAddr(addr, 0,
          DFSConfigKeys.DFS_BALANCER_ADDRESS_KEY);
      SecurityUtil.login(conf, DFSConfigKeys.DFS_BALANCER_KEYTAB_FILE_KEY,
          DFSConfigKeys.DFS_BALANCER_KERBEROS_PRINCIPAL_KEY,
          socAddr.getHostName());
    }
  }

  /* Given elaspedTime in ms, return a printable string */
  private static String time2Str(long elapsedTime) {
    String unit;
    double time = elapsedTime;
    if (elapsedTime < 1000) {
      unit = "milliseconds";
    } else if (elapsedTime < 60*1000) {
      unit = "seconds";
      time = time/1000;
    } else if (elapsedTime < 3600*1000) {
      unit = "minutes";
      time = time/(60*1000);
    } else {
      unit = "hours";
      time = time/(3600*1000);
    }

    return time+" "+unit;
  }

  static class Cli extends Configured implements Tool {
    /**
     * Parse arguments and then run Balancer.
     * 
     * @param args command specific arguments.
     * @return exit code. 0 indicates success, non-zero indicates failure.
     */
    @Override
    public int run(String[] args) {
      final long startTime = Time.monotonicNow();
      final Configuration conf = getConf();

      try {
        checkReplicationPolicyCompatibility(conf);

        final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
        final Collection<String> nsIds = DFSUtilClient.getNameServiceIds(conf);
        return Balancer.run(namenodes, nsIds, parse(args), conf);
      } catch (IOException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } catch (InterruptedException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.INTERRUPTED.getExitCode();
      } finally {
        System.out.format("%-24s ",
            DateFormat.getDateTimeInstance().format(new Date()));
        System.out.println("Balancing took "
            + time2Str(Time.monotonicNow() - startTime));
      }
    }

    /** parse command line arguments */
    static BalancerParameters parse(String[] args) {
      Set<String> excludedNodes = null;
      Set<String> includedNodes = null;
      BalancerParameters.Builder b = new BalancerParameters.Builder();

      if (args != null) {
        try {
          for(int i = 0; i < args.length; i++) {
            if ("-threshold".equalsIgnoreCase(args[i])) {
              Preconditions.checkArgument(++i < args.length,
                "Threshold value is missing: args = " + Arrays.toString(args));
              try {
                double threshold = Double.parseDouble(args[i]);
                if (threshold < 1 || threshold > 100) {
                  throw new IllegalArgumentException(
                      "Number out of range: threshold = " + threshold);
                }
                LOG.info( "Using a threshold of " + threshold );
                b.setThreshold(threshold);
              } catch(IllegalArgumentException e) {
                System.err.println(
                    "Expecting a number in the range of [1.0, 100.0]: "
                    + args[i]);
                throw e;
              }
            } else if ("-policy".equalsIgnoreCase(args[i])) {
              Preconditions.checkArgument(++i < args.length,
                "Policy value is missing: args = " + Arrays.toString(args));
              try {
                b.setBalancingPolicy(BalancingPolicy.parse(args[i]));
              } catch(IllegalArgumentException e) {
                System.err.println("Illegal policy name: " + args[i]);
                throw e;
              }
            } else if ("-exclude".equalsIgnoreCase(args[i])) {
              excludedNodes = new HashSet<>();
              i = processHostList(args, i, "exclude", excludedNodes);
              b.setExcludedNodes(excludedNodes);
            } else if ("-include".equalsIgnoreCase(args[i])) {
              includedNodes = new HashSet<>();
              i = processHostList(args, i, "include", includedNodes);
              b.setIncludedNodes(includedNodes);
            } else if ("-source".equalsIgnoreCase(args[i])) {
              Set<String> sourceNodes = new HashSet<>();
              i = processHostList(args, i, "source", sourceNodes);
              b.setSourceNodes(sourceNodes);
            } else if ("-blockpools".equalsIgnoreCase(args[i])) {
              Preconditions.checkArgument(
                  ++i < args.length,
                  "blockpools value is missing: args = "
                      + Arrays.toString(args));
              Set<String> blockpools = parseBlockPoolList(args[i]);
              LOG.info("Balancer will run on the following blockpools: "
                  + blockpools.toString());
              b.setBlockpools(blockpools);
            } else if ("-idleiterations".equalsIgnoreCase(args[i])) {
              Preconditions.checkArgument(++i < args.length,
                  "idleiterations value is missing: args = " + Arrays
                      .toString(args));
              int maxIdleIteration = Integer.parseInt(args[i]);
              LOG.info("Using a idleiterations of " + maxIdleIteration);
              b.setMaxIdleIteration(maxIdleIteration);
            } else if ("-runDuringUpgrade".equalsIgnoreCase(args[i])) {
              b.setRunDuringUpgrade(true);
              LOG.info("Will run the balancer even during an ongoing HDFS "
                  + "upgrade. Most users will not want to run the balancer "
                  + "during an upgrade since it will not affect used space "
                  + "on over-utilized machines.");
            } else if ("-asService".equalsIgnoreCase(args[i])) {
              b.setRunAsService(true);
              LOG.info("Balancer will run as a long running service");
            } else if ("-hotBlockTimeInterval".equalsIgnoreCase(args[i])) {
              Preconditions.checkArgument(++i < args.length,
                  "hotBlockTimeInterval value is missing: args = "
                  + Arrays.toString(args));
              long hotBlockTimeInterval = Long.parseLong(args[i]);
              LOG.info("Using a hotBlockTimeInterval of "
                  + hotBlockTimeInterval);
              b.setHotBlockTimeInterval(hotBlockTimeInterval);
            } else if ("-sortTopNodes".equalsIgnoreCase(args[i])) {
              b.setSortTopNodes(true);
              LOG.info("Balancer will sort nodes by" +
                  " capacity usage percentage to prioritize top used nodes");
            } else {
              throw new IllegalArgumentException("args = "
                  + Arrays.toString(args));
            }
          }
          Preconditions.checkArgument(excludedNodes == null || includedNodes == null,
              "-exclude and -include options cannot be specified together.");
        } catch(RuntimeException e) {
          printUsage(System.err);
          throw e;
        }
      }
      return b.build();
    }

    private static int processHostList(String[] args, int i, String type,
        Set<String> nodes) {
      Preconditions.checkArgument(++i < args.length,
          "List of %s nodes | -f <filename> is missing: args=%s",
          type, Arrays.toString(args));
      if ("-f".equalsIgnoreCase(args[i])) {
        Preconditions.checkArgument(++i < args.length,
            "File containing %s nodes is not specified: args=%s",
            type, Arrays.toString(args));

        final String filename = args[i];
        try {
          HostsFileReader.readFileToSet(type, filename, nodes);
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "Failed to read " + type + " node list from file: " + filename);
        }
      } else {
        final String[] addresses = StringUtils.getTrimmedStrings(args[i]);
        nodes.addAll(Arrays.asList(addresses));
      }
      return i;
    }

    private static Set<String> parseBlockPoolList(String string) {
      String[] addrs = StringUtils.getTrimmedStrings(string);
      return new HashSet<String>(Arrays.asList(addrs));
    }

    private static void printUsage(PrintStream out) {
      out.println(USAGE + "\n");
    }
  }

  /**
   * Run a balancer
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting balancer due an exception", e);
      System.exit(-1);
    }
  }
}
