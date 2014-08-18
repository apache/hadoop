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

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockStoragePolicy;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DBlock;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.PendingMove;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Source;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.StorageGroupMap;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@InterfaceAudience.Private
public class Mover {
  static final Log LOG = LogFactory.getLog(Mover.class);

  private static final Path MOVER_ID_PATH = new Path("/system/mover.id");

  private static class StorageMap {
    private final StorageGroupMap<Source> sources
        = new StorageGroupMap<Source>();
    private final StorageGroupMap<StorageGroup> targets
        = new StorageGroupMap<StorageGroup>();
    private final EnumMap<StorageType, List<StorageGroup>> targetStorageTypeMap
        = new EnumMap<StorageType, List<StorageGroup>>(StorageType.class);
    
    private StorageMap() {
      for(StorageType t : StorageType.asList()) {
        targetStorageTypeMap.put(t, new LinkedList<StorageGroup>());
      }
    }
    
    private void add(Source source, StorageGroup target) {
      sources.put(source);
      targets.put(target);
      getTargetStorages(target.getStorageType()).add(target);
    }
    
    private Source getSource(MLocation ml) {
      return get(sources, ml);
    }

    private StorageGroup getTarget(MLocation ml) {
      return get(targets, ml);
    }

    private static <G extends StorageGroup> G get(StorageGroupMap<G> map, MLocation ml) {
      return map.get(ml.datanode.getDatanodeUuid(), ml.storageType);
    }
    
    private List<StorageGroup> getTargetStorages(StorageType t) {
      return targetStorageTypeMap.get(t);
    }
  }

  private final Dispatcher dispatcher;
  private final StorageMap storages;

  private final BlockStoragePolicy.Suite blockStoragePolicies;

  Mover(NameNodeConnector nnc, Configuration conf) {
    final long movedWinWidth = conf.getLong(
        DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_KEY,
        DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_DEFAULT);
    final int moverThreads = conf.getInt(
        DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY,
        DFSConfigKeys.DFS_MOVER_MOVERTHREADS_DEFAULT);
    final int maxConcurrentMovesPerNode = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);

    this.dispatcher = new Dispatcher(nnc, Collections.<String> emptySet(),
        Collections.<String> emptySet(), movedWinWidth, moverThreads, 0,
        maxConcurrentMovesPerNode, conf);
    this.storages = new StorageMap();
    this.blockStoragePolicies = BlockStoragePolicy.readBlockStorageSuite(conf);
  }
  
  private ExitStatus run() {
    try {
      final List<DatanodeStorageReport> reports = dispatcher.init();
      for(DatanodeStorageReport r : reports) {
        final DDatanode dn = dispatcher.newDatanode(r.getDatanodeInfo());
        for(StorageType t : StorageType.asList()) {
          final long maxRemaining = getMaxRemaining(r, t);
          if (maxRemaining > 0L) {
            final Source source = dn.addSource(t, Long.MAX_VALUE, dispatcher); 
            final StorageGroup target = dn.addTarget(t, maxRemaining);
            storages.add(source, target);
          }
        }
      }

      new Processor().processNamespace();

      return ExitStatus.IN_PROGRESS;
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.ILLEGAL_ARGUMENTS;
    } catch (IOException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.IO_EXCEPTION;
    } finally {
      dispatcher.shutdownNow();
    }
  }

  private static long getMaxRemaining(DatanodeStorageReport report, StorageType t) {
    long max = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (r.getStorage().getStorageType() == t) {
        if (r.getRemaining() > max) {
          max = r.getRemaining();
        }
      }
    }
    return max;
  }

  private class Processor {
    private final DFSClient dfs;
  
    private Processor() {
      dfs = dispatcher.getDistributedFileSystem().getClient();
    }
      
    private void processNamespace() {
      try {
        processDirRecursively("", dfs.getFileInfo("/"));
      } catch (IOException e) {
        LOG.warn("Failed to get root directory status. Ignore and continue.", e);
      }
    }

    private void processDirRecursively(String parent, HdfsFileStatus status) {
      if (status.isSymlink()) {
        return; //ignore symlinks
      } else if (status.isDir()) {
        String dir = status.getFullName(parent);
        if (!dir.endsWith(Path.SEPARATOR)) {
          dir = dir + Path.SEPARATOR; 
        }

        for(byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;;) {
          final DirectoryListing children;
          try {
            children = dfs.listPaths(dir, lastReturnedName, true);
          } catch(IOException e) {
            LOG.warn("Failed to list directory " + dir
                + ".  Ignore the directory and continue.", e);
            return;
          }
          if (children == null) {
            return;
          }
          for (HdfsFileStatus child : children.getPartialListing()) {
            processDirRecursively(dir, child);
          }
          if (!children.hasMore()) {
            lastReturnedName = children.getLastName();
          } else {
            return;
          }
        }
      } else { // file
        processFile(parent, (HdfsLocatedFileStatus)status);
      }
    }

    private void processFile(String parent, HdfsLocatedFileStatus status) { 
      final BlockStoragePolicy policy = blockStoragePolicies.getPolicy(
          status.getStoragePolicy());
      final List<StorageType> types = policy.chooseStorageTypes(
          status.getReplication());

      final LocatedBlocks locations = status.getBlockLocations();
      for(LocatedBlock lb : locations.getLocatedBlocks()) {
        final StorageTypeDiff diff = new StorageTypeDiff(types, lb.getStorageTypes());
        if (!diff.removeOverlap()) {
          scheduleMoves4Block(diff, lb);
        }
      }
    }
    
    void scheduleMoves4Block(StorageTypeDiff diff, LocatedBlock lb) {
      final List<MLocation> locations = MLocation.toLocations(lb);
      Collections.shuffle(locations);
      
      final DBlock db = new DBlock(lb.getBlock().getLocalBlock());
      for(MLocation ml : locations) {
        db.addLocation(storages.getTarget(ml));
      }

      for(final Iterator<StorageType> i = diff.existing.iterator(); i.hasNext(); ) {
        final StorageType t = i.next();
        for(final Iterator<MLocation> j = locations.iterator(); j.hasNext(); ) {
          final MLocation ml = j.next();
          final Source source = storages.getSource(ml); 
          if (ml.storageType == t) {
            // try to schedule replica move.
            if (scheduleMoveReplica(db, ml, source, diff.expected)) {
              i.remove();
              j.remove();
            }
          }
        }
      }
    }

    boolean scheduleMoveReplica(DBlock db, MLocation ml, Source source,
        List<StorageType> targetTypes) {
      if (dispatcher.getCluster().isNodeGroupAware()) {
        if (chooseTarget(db, ml, source, targetTypes, Matcher.SAME_NODE_GROUP)) {
          return true;
        }
      }
      
      // Then, match nodes on the same rack
      if (chooseTarget(db, ml, source, targetTypes, Matcher.SAME_RACK)) {
        return true;
      }
      // At last, match all remaining nodes
      if (chooseTarget(db, ml, source, targetTypes, Matcher.ANY_OTHER)) {
        return true;
      }
      return false;
    }

    boolean chooseTarget(DBlock db, MLocation ml, Source source,
        List<StorageType> targetTypes, Matcher matcher) {
      final NetworkTopology cluster = dispatcher.getCluster(); 
      for(final Iterator<StorageType> i = targetTypes.iterator(); i.hasNext(); ) {
        final StorageType t = i.next();
        for(StorageGroup target : storages.getTargetStorages(t)) {
          if (matcher.match(cluster, ml.datanode, target.getDatanodeInfo())
              && dispatcher.isGoodBlockCandidate(source, target, t, db)) {
            final PendingMove pm = dispatcher.new PendingMove(db, source, target);
            if (pm.chooseProxySource()) {
              i.remove();
              target.incScheduledSize(ml.size);
              dispatcher.executePendingMove(pm);
              return true;
            }
          }
        }
      }
      return false;
    }
  }
  

  static class MLocation {
    final DatanodeInfo datanode;
    final StorageType storageType;
    final long size;
    
    MLocation(DatanodeInfo datanode, StorageType storageType, long size) {
      this.datanode = datanode;
      this.storageType = storageType;
      this.size = size;
    }
    
    static List<MLocation> toLocations(LocatedBlock lb) {
      final DatanodeInfo[] datanodeInfos = lb.getLocations();
      final StorageType[] storageTypes = lb.getStorageTypes();
      final long size = lb.getBlockSize();
      final List<MLocation> locations = new LinkedList<MLocation>();
      for(int i = 0; i < datanodeInfos.length; i++) {
        locations.add(new MLocation(datanodeInfos[i], storageTypes[i], size));
      }
      return locations;
    }
  }

  private static class StorageTypeDiff {
    final List<StorageType> expected;
    final List<StorageType> existing;

    StorageTypeDiff(List<StorageType> expected, StorageType[] existing) {
      this.expected = new LinkedList<StorageType>(expected);
      this.existing = new LinkedList<StorageType>(Arrays.asList(existing));
    }
    
    /**
     * Remove the overlap between the expected types and the existing types.
     * @return if the existing types is empty after removed the overlap.
     */
    boolean removeOverlap() { 
      for(Iterator<StorageType> i = existing.iterator(); i.hasNext(); ) {
        final StorageType t = i.next();
        if (expected.remove(t)) {
          i.remove();
        }
      }
      return existing.isEmpty();
    }
  }

  static int run(Collection<URI> namenodes, Configuration conf)
      throws IOException, InterruptedException {
    final long sleeptime = 2000*conf.getLong(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT);
    LOG.info("namenodes = " + namenodes);
    
    List<NameNodeConnector> connectors = Collections.emptyList();
    try {
      connectors = NameNodeConnector.newNameNodeConnectors(namenodes, 
            Mover.class.getSimpleName(), MOVER_ID_PATH, conf);
    
      while (true) {
        Collections.shuffle(connectors);
        for(NameNodeConnector nnc : connectors) {
          final Mover m = new Mover(nnc, conf);
          final ExitStatus r = m.run();

          if (r != ExitStatus.IN_PROGRESS) {
            //must be an error statue, return.
            return r.getExitCode();
          }
        }

        Thread.sleep(sleeptime);
      }
    } finally {
      for(NameNodeConnector nnc : connectors) {
        IOUtils.cleanup(LOG, nnc);
      }
    }
  }

  static class Cli extends Configured implements Tool {
    private static final String USAGE = "Usage: java "
        + Mover.class.getSimpleName();

    @Override
    public int run(String[] args) throws Exception {
      final long startTime = Time.monotonicNow();
      final Configuration conf = getConf();

      try {
        final Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
        return Mover.run(namenodes, conf);
      } catch (IOException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } catch (InterruptedException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.INTERRUPTED.getExitCode();
      } finally {
        System.out.format("%-24s ", DateFormat.getDateTimeInstance().format(new Date()));
        System.out.println("Mover took " + StringUtils.formatTime(Time.monotonicNow()-startTime));
      }
    }

    /**
     * Run a Mover in command line.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
      if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
        System.exit(0);
      }

      try {
        System.exit(ToolRunner.run(new HdfsConfiguration(), new Cli(), args));
      } catch (Throwable e) {
        LOG.error("Exiting " + Mover.class.getSimpleName()
            + " due to an exception", e);
        System.exit(-1);
      }
    }
  }
}