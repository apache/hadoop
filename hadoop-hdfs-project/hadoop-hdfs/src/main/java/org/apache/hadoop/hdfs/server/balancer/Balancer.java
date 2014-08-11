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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Source;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Task;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Util;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Preconditions;

/** <p>The balancer is a tool that balances disk space usage on an HDFS cluster
 * when some datanodes become full or when new empty nodes join the cluster.
 * The tool is deployed as an application program that can be run by the 
 * cluster administrator on a live HDFS cluster while applications
 * adding and deleting files.
 * 
 * <p>SYNOPSIS
 * <pre>
 * To start:
 *      bin/start-balancer.sh [-threshold <threshold>]
 *      Example: bin/ start-balancer.sh 
 *                     start the balancer with a default threshold of 10%
 *               bin/ start-balancer.sh -threshold 5
 *                     start the balancer with a threshold of 5%
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
 * <property>
 *   <name>dfs.balance.bandwidthPerSec</name>
 *   <value>1048576</value>
 * <description>  Specifies the maximum bandwidth that each datanode 
 * can utilize for the balancing purpose in term of the number of bytes 
 * per second. </description>
 * </property>
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
 * <li>No block has been moved for five consecutive iterations;
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
 * <li>No block has been moved for 5 iterations. Exiting...
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
  static final Log LOG = LogFactory.getLog(Balancer.class);

  private static final Path BALANCER_ID_PATH = new Path("/system/balancer.id");

  private static final long GB = 1L << 30; //1GB
  private static final long MAX_SIZE_TO_MOVE = 10*GB;

  private static final String USAGE = "Usage: java "
      + Balancer.class.getSimpleName()
      + "\n\t[-policy <policy>]\tthe balancing policy: "
      + BalancingPolicy.Node.INSTANCE.getName() + " or "
      + BalancingPolicy.Pool.INSTANCE.getName()
      + "\n\t[-threshold <threshold>]\tPercentage of disk capacity"
      + "\n\t[-exclude [-f <hosts-file> | comma-sperated list of hosts]]"
      + "\tExcludes the specified datanodes."
      + "\n\t[-include [-f <hosts-file> | comma-sperated list of hosts]]"
      + "\tIncludes only the specified datanodes.";
  
  private final Dispatcher dispatcher;
  private final BalancingPolicy policy;
  private final double threshold;
  
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
    if (!(BlockPlacementPolicy.getInstance(conf, null, null, null) instanceof 
        BlockPlacementPolicyDefault)) {
      throw new UnsupportedActionException(
          "Balancer without BlockPlacementPolicyDefault");
    }
  }

  /**
   * Construct a balancer.
   * Initialize balancer. It sets the value of the threshold, and 
   * builds the communication proxies to
   * namenode as a client and a secondary namenode and retry proxies
   * when connection fails.
   */
  Balancer(NameNodeConnector theblockpool, Parameters p, Configuration conf) {
    final long movedWinWidth = conf.getLong(
        DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY,
        DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_DEFAULT);
    final int moverThreads = conf.getInt(
        DFSConfigKeys.DFS_BALANCER_MOVERTHREADS_KEY,
        DFSConfigKeys.DFS_BALANCER_MOVERTHREADS_DEFAULT);
    final int dispatcherThreads = conf.getInt(
        DFSConfigKeys.DFS_BALANCER_DISPATCHERTHREADS_KEY,
        DFSConfigKeys.DFS_BALANCER_DISPATCHERTHREADS_DEFAULT);
    final int maxConcurrentMovesPerNode = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);

    this.dispatcher = new Dispatcher(theblockpool, p.nodesToBeIncluded,
        p.nodesToBeExcluded, movedWinWidth, moverThreads, dispatcherThreads,
        maxConcurrentMovesPerNode, conf);
    this.threshold = p.threshold;
    this.policy = p.policy;
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

  private static long getRemaining(DatanodeStorageReport report, StorageType t) {
    long remaining = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (r.getStorage().getStorageType() == t) {
        remaining += r.getRemaining();
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

    // create network topology and classify utilization collections: 
    //   over-utilized, above-average, below-average and under-utilized.
    long overLoadedBytes = 0L, underLoadedBytes = 0L;
    for(DatanodeStorageReport r : reports) {
      final DDatanode dn = dispatcher.newDatanode(r);
      for(StorageType t : StorageType.asList()) {
        final Double utilization = policy.getUtilization(r, t);
        if (utilization == null) { // datanode does not have such storage type 
          continue;
        }
        
        final long capacity = getCapacity(r, t);
        final double utilizationDiff = utilization - policy.getAvgUtilization(t);
        final double thresholdDiff = Math.abs(utilizationDiff) - threshold;
        final long maxSize2Move = computeMaxSize2Move(capacity,
            getRemaining(r, t), utilizationDiff, threshold);

        final StorageGroup g;
        if (utilizationDiff > 0) {
          final Source s = dn.addSource(t, maxSize2Move, dispatcher);
          if (thresholdDiff <= 0) { // within threshold
            aboveAvgUtilized.add(s);
          } else {
            overLoadedBytes += precentage2bytes(thresholdDiff, capacity);
            overUtilized.add(s);
          }
          g = s;
        } else {
          g = dn.addStorageGroup(t, maxSize2Move);
          if (thresholdDiff <= 0) { // within threshold
            belowAvgUtilized.add(g);
          } else {
            underLoadedBytes += precentage2bytes(thresholdDiff, capacity);
            underUtilized.add(g);
          }
        }
        dispatcher.getStorageGroupMap().put(g);
      }
    }

    logUtilizationCollections();
    
    Preconditions.checkState(dispatcher.getStorageGroupMap().size()
        == overUtilized.size() + underUtilized.size() + aboveAvgUtilized.size()
           + belowAvgUtilized.size(),
        "Mismatched number of storage groups");
    
    // return number of bytes to be moved in order to make the cluster balanced
    return Math.max(overLoadedBytes, underLoadedBytes);
  }

  private static long computeMaxSize2Move(final long capacity, final long remaining,
      final double utilizationDiff, final double threshold) {
    final double diff = Math.min(threshold, Math.abs(utilizationDiff));
    long maxSizeToMove = precentage2bytes(diff, capacity);
    if (utilizationDiff < 0) {
      maxSizeToMove = Math.min(remaining, maxSizeToMove);
    }
    return Math.min(MAX_SIZE_TO_MOVE, maxSizeToMove);
  }

  private static long precentage2bytes(double precentage, long capacity) {
    Preconditions.checkArgument(precentage >= 0,
        "precentage = " + precentage + " < 0");
    return (long)(precentage * capacity / 100.0);
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
    chooseStorageGroups(overUtilized, underUtilized, matcher);
    
    /* match each remaining overutilized datanode (source) to 
     * below average utilized datanodes (targets).
     * Note only overutilized datanodes that haven't had that max bytes to move
     * satisfied in step 1 are selected
     */
    chooseStorageGroups(overUtilized, belowAvgUtilized, matcher);

    /* match each remaining underutilized datanode (target) to 
     * above average utilized datanodes (source).
     * Note only underutilized datanodes that have not had that max bytes to
     * move satisfied in step 1 are selected.
     */
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
        } else if (matcher.match(dispatcher.getCluster(),
            g.getDatanodeInfo(), c.getDatanodeInfo())) {
          return c;
        }
      }
    }
    return null;
  }

  /* reset all fields in a balancer preparing for the next iteration */
  private void resetData(Configuration conf) {
    this.overUtilized.clear();
    this.aboveAvgUtilized.clear();
    this.belowAvgUtilized.clear();
    this.underUtilized.clear();
    this.policy.reset();
    dispatcher.reset(conf);;
  }
  
  /** Run an iteration for all datanodes. */
  private ExitStatus run(int iteration, Formatter formatter,
      Configuration conf) {
    try {
      final List<DatanodeStorageReport> reports = dispatcher.init();
      final long bytesLeftToMove = init(reports);
      if (bytesLeftToMove == 0) {
        System.out.println("The cluster is balanced. Exiting...");
        return ExitStatus.SUCCESS;
      } else {
        LOG.info( "Need to move "+ StringUtils.byteDesc(bytesLeftToMove)
            + " to make the cluster balanced." );
      }
      
      /* Decide all the nodes that will participate in the block move and
       * the number of bytes that need to be moved from one node to another
       * in this iteration. Maximum bytes to be moved per node is
       * Min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
       */
      final long bytesToMove = chooseStorageGroups();
      if (bytesToMove == 0) {
        System.out.println("No block can be moved. Exiting...");
        return ExitStatus.NO_MOVE_BLOCK;
      } else {
        LOG.info( "Will move " + StringUtils.byteDesc(bytesToMove) +
            " in this iteration");
      }

      formatter.format("%-24s %10d  %19s  %18s  %17s%n",
          DateFormat.getDateTimeInstance().format(new Date()),
          iteration,
          StringUtils.byteDesc(dispatcher.getBytesMoved()),
          StringUtils.byteDesc(bytesLeftToMove),
          StringUtils.byteDesc(bytesToMove)
          );
      
      /* For each pair of <source, target>, start a thread that repeatedly 
       * decide a block to be moved and its proxy source, 
       * then initiates the move until all bytes are moved or no more block
       * available to move.
       * Exit no byte has been moved for 5 consecutive iterations.
       */
      if (!dispatcher.dispatchAndCheckContinue()) {
        return ExitStatus.NO_MOVE_PROGRESS;
      }

      return ExitStatus.IN_PROGRESS;
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.ILLEGAL_ARGUMENTS;
    } catch (IOException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.IO_EXCEPTION;
    } catch (InterruptedException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.INTERRUPTED;
    } finally {
      dispatcher.shutdownNow();
    }
  }

  /**
   * Balance all namenodes.
   * For each iteration,
   * for each namenode,
   * execute a {@link Balancer} to work through all datanodes once.  
   */
  static int run(Collection<URI> namenodes, final Parameters p,
      Configuration conf) throws IOException, InterruptedException {
    final long sleeptime = 2000*conf.getLong(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT);
    LOG.info("namenodes  = " + namenodes);
    LOG.info("parameters = " + p);
    
    final Formatter formatter = new Formatter(System.out);
    System.out.println("Time Stamp               Iteration#  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved");
    
    final List<NameNodeConnector> connectors
        = new ArrayList<NameNodeConnector>(namenodes.size());
    try {
      for (URI uri : namenodes) {
        final NameNodeConnector nnc = new NameNodeConnector(
            Balancer.class.getSimpleName(), uri, BALANCER_ID_PATH, conf);
        nnc.getKeyManager().startBlockKeyUpdater();
        connectors.add(nnc);
      }
    
      boolean done = false;
      for(int iteration = 0; !done; iteration++) {
        done = true;
        Collections.shuffle(connectors);
        for(NameNodeConnector nnc : connectors) {
          final Balancer b = new Balancer(nnc, p, conf);
          final ExitStatus r = b.run(iteration, formatter, conf);
          // clean all lists
          b.resetData(conf);
          if (r == ExitStatus.IN_PROGRESS) {
            done = false;
          } else if (r != ExitStatus.SUCCESS) {
            //must be an error statue, return.
            return r.getExitCode();
          }
        }

        if (!done) {
          Thread.sleep(sleeptime);
        }
      }
    } finally {
      for(NameNodeConnector nnc : connectors) {
        nnc.close();
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
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

  static class Parameters {
    static final Parameters DEFAULT = new Parameters(
        BalancingPolicy.Node.INSTANCE, 10.0,
        Collections.<String> emptySet(), Collections.<String> emptySet());

    final BalancingPolicy policy;
    final double threshold;
    // exclude the nodes in this set from balancing operations
    Set<String> nodesToBeExcluded;
    //include only these nodes in balancing operations
    Set<String> nodesToBeIncluded;

    Parameters(BalancingPolicy policy, double threshold,
        Set<String> nodesToBeExcluded, Set<String> nodesToBeIncluded) {
      this.policy = policy;
      this.threshold = threshold;
      this.nodesToBeExcluded = nodesToBeExcluded;
      this.nodesToBeIncluded = nodesToBeIncluded;
    }

    @Override
    public String toString() {
      return Balancer.class.getSimpleName() + "." + getClass().getSimpleName()
          + "[" + policy + ", threshold=" + threshold +
          ", number of nodes to be excluded = "+ nodesToBeExcluded.size() +
          ", number of nodes to be included = "+ nodesToBeIncluded.size() +"]";
    }
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
      final long startTime = Time.now();
      final Configuration conf = getConf();

      try {
        checkReplicationPolicyCompatibility(conf);

        final Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
        return Balancer.run(namenodes, parse(args), conf);
      } catch (IOException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } catch (InterruptedException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.INTERRUPTED.getExitCode();
      } finally {
        System.out.format("%-24s ", DateFormat.getDateTimeInstance().format(new Date()));
        System.out.println("Balancing took " + time2Str(Time.now()-startTime));
      }
    }

    /** parse command line arguments */
    static Parameters parse(String[] args) {
      BalancingPolicy policy = Parameters.DEFAULT.policy;
      double threshold = Parameters.DEFAULT.threshold;
      Set<String> nodesTobeExcluded = Parameters.DEFAULT.nodesToBeExcluded;
      Set<String> nodesTobeIncluded = Parameters.DEFAULT.nodesToBeIncluded;

      if (args != null) {
        try {
          for(int i = 0; i < args.length; i++) {
            if ("-threshold".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                "Threshold value is missing: args = " + Arrays.toString(args));
              try {
                threshold = Double.parseDouble(args[i]);
                if (threshold < 1 || threshold > 100) {
                  throw new IllegalArgumentException(
                      "Number out of range: threshold = " + threshold);
                }
                LOG.info( "Using a threshold of " + threshold );
              } catch(IllegalArgumentException e) {
                System.err.println(
                    "Expecting a number in the range of [1.0, 100.0]: "
                    + args[i]);
                throw e;
              }
            } else if ("-policy".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                "Policy value is missing: args = " + Arrays.toString(args));
              try {
                policy = BalancingPolicy.parse(args[i]);
              } catch(IllegalArgumentException e) {
                System.err.println("Illegal policy name: " + args[i]);
                throw e;
              }
            } else if ("-exclude".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                  "List of nodes to exclude | -f <filename> is missing: args = "
                  + Arrays.toString(args));
              if ("-f".equalsIgnoreCase(args[i])) {
                checkArgument(++i < args.length,
                    "File containing nodes to exclude is not specified: args = "
                    + Arrays.toString(args));
                nodesTobeExcluded = Util.getHostListFromFile(args[i], "exclude");
              } else {
                nodesTobeExcluded = Util.parseHostList(args[i]);
              }
            } else if ("-include".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                "List of nodes to include | -f <filename> is missing: args = "
                + Arrays.toString(args));
              if ("-f".equalsIgnoreCase(args[i])) {
                checkArgument(++i < args.length,
                    "File containing nodes to include is not specified: args = "
                    + Arrays.toString(args));
                nodesTobeIncluded = Util.getHostListFromFile(args[i], "include");
               } else {
                nodesTobeIncluded = Util.parseHostList(args[i]);
              }
            } else {
              throw new IllegalArgumentException("args = "
                  + Arrays.toString(args));
            }
          }
          checkArgument(nodesTobeExcluded.isEmpty() || nodesTobeIncluded.isEmpty(),
              "-exclude and -include options cannot be specified together.");
        } catch(RuntimeException e) {
          printUsage(System.err);
          throw e;
        }
      }
      
      return new Parameters(policy, threshold, nodesTobeExcluded, nodesTobeIncluded);
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
