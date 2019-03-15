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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

/**
 * An abstract {@link InputFormat} that returns {@link CombineFileSplit}'s in
 * {@link InputFormat#getSplits(JobContext)} method.
 *
 * Splits are constructed from the files under the input paths. A split cannot
 * have files from different pools. Each split returned may contain blocks from
 * different files. If a maxSplitSize is specified, then blocks on the same node
 * are combined to form a single split. Blocks that are left over are then
 * combined with other blocks in the same rack. If maxSplitSize is not
 * specified, then blocks from the same rack are combined in a single split; no
 * attempt is made to create node-local splits. If the maxSplitSize is equal to
 * the block size, then this class is similar to the default splitting behavior
 * in Hadoop: each block is a locally processed split. Subclasses implement
 * {@link InputFormat#createRecordReader(InputSplit, TaskAttemptContext)} to
 * construct <code>RecordReader</code>'s for <code>CombineFileSplit</code>'s.
 *
 * @see CombineFileSplit
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CombineFileInputFormat<K, V>
    extends FileInputFormat<K, V> {

  private static final Logger LOG =
      LoggerFactory.getLogger(CombineFileInputFormat.class);

  public static final String SPLIT_MINSIZE_PERNODE =
      "mapreduce.input.fileinputformat.split.minsize.per.node";
  public static final String SPLIT_MINSIZE_PERRACK =
      "mapreduce.input.fileinputformat.split.minsize.per.rack";

  // ability to limit the size of a single split
  private long maxSplitSize = 0L;
  private long minSplitSizeNode = 0L;
  private long minSplitSizeRack = 0L;

  /**
   * A pool of input paths filters. A split cannot have blocks from files across
   * multiple pools.
   */
  private ArrayList<MultiPathFilter> pools = new ArrayList<>();

  /**
   * Specify the maximum size (in bytes) of each split. Each split is
   * approximately equal to the specified size.
   */
  protected void setMaxSplitSize(long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  /**
   * Specify the minimum size (in bytes) of each split per node. This applies to
   * data that is left over after combining data on a single node into splits
   * that are of maximum size specified by maxSplitSize. This leftover data will
   * be combined into its own split if its size exceeds minSplitSizeNode.
   */
  protected void setMinSplitSizeNode(long minSplitSizeNode) {
    this.minSplitSizeNode = minSplitSizeNode;
  }

  /**
   * Specify the minimum size (in bytes) of each split per rack. This applies to
   * data that is left over after combining data on a single rack into splits
   * that are of maximum size specified by maxSplitSize. This leftover data will
   * be combined into its own split if its size exceeds minSplitSizeRack.
   */
  protected void setMinSplitSizeRack(long minSplitSizeRack) {
    this.minSplitSizeRack = minSplitSizeRack;
  }

  /**
   * Create a new pool and add the filters to it. A split cannot have files from
   * different pools.
   */
  protected void createPool(List<PathFilter> filters) {
    pools.add(new MultiPathFilter(filters));
  }

  /**
   * Create a new pool and add the filters to it. A pathname can satisfy any one
   * of the specified filters. A split cannot have files from different pools.
   */
  protected void createPool(PathFilter... filters) {
    pools.add(new MultiPathFilter(Arrays.asList(filters)));
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    final CompressionCodec codec =
        new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

  /**
   * default constructor
   */
  public CombineFileInputFormat() {
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    long minSizeNode = 0L;
    long minSizeRack = 0L;
    long maxSize = 0L;
    Configuration conf = job.getConfiguration();

    // the values specified by setxxxSplitSize() takes precedence over the
    // values that might have been specified in the config
    if (minSplitSizeNode != 0L) {
      minSizeNode = minSplitSizeNode;
    } else {
      minSizeNode = conf.getLong(SPLIT_MINSIZE_PERNODE, 0L);
    }
    if (minSplitSizeRack != 0L) {
      minSizeRack = minSplitSizeRack;
    } else {
      minSizeRack = conf.getLong(SPLIT_MINSIZE_PERRACK, 0L);
    }

    if (maxSplitSize != 0L) {
      maxSize = maxSplitSize;
    } else {
      // A value of zero means that a single split will be generated per node.
      maxSize = conf.getLong(SPLIT_MAXSIZE, 0L);
    }

    Preconditions.checkState(
        minSizeNode == 0L || maxSize == 0L || minSizeNode <= maxSize,
        "Minimum split size per node " + minSizeNode
            + " cannot be larger than maximum split size " + maxSize);

    Preconditions.checkState(
        minSizeRack == 0L || maxSize == 0L || minSizeRack <= maxSize,
        "Minimum split size per rack " + minSizeRack
            + " cannot be larger than maximum split size " + maxSize);

    Preconditions.checkState(minSizeRack == 0L || minSizeNode <= minSizeRack,
        "Minimum split size per node " + minSizeNode
            + " cannot be larger than minimum split size per rack "
            + minSizeRack);

    // all the files in input set
    final Set<FileStatus> stats = new HashSet<>(listStatus(job));
    if (stats.isEmpty()) {
      return Collections.emptyList();
    }

    final List<InputSplit> splits = new ArrayList<>();

    // In one single iteration, process all the paths in a single pool.
    // Processing one pool at a time ensures that a split contains paths
    // from a single pool only.
    for (final MultiPathFilter onepool : pools) {
      final ArrayList<FileStatus> poolPaths = new ArrayList<>();

      // For each input path, if it matches all the filters in a pool, add it to
      // the output set.
      for (final FileStatus fileStatus : stats) {
        if (onepool.accept(fileStatus.getPath())) {
          poolPaths.add(fileStatus);
        }
      }

      // create splits for all files in this pool.
      getMoreSplits(job, poolPaths, maxSize, minSizeNode, minSizeRack, splits);

      stats.removeAll(poolPaths);
    }

    // create splits for all files that are not in any pool.
    getMoreSplits(job, stats, maxSize, minSizeNode, minSizeRack, splits);

    return splits;
  }

  /**
   * Return all the splits in the specified set of paths.
   */
  private void getMoreSplits(JobContext job, Collection<FileStatus> stats,
      long maxSize, long minSizeNode, long minSizeRack, List<InputSplit> splits)
      throws IOException {

    if (CollectionUtils.isEmpty(stats)) {
      return;
    }

    Configuration conf = job.getConfiguration();

    // A set of all the blocks to assign to splits
    // The blocks are kept in descending order based on block size
    Set<OneBlockInfo> allBlocksSet = new LinkedHashSet<>();

    // Mapping from a rack name to the list of blocks it has
    // Use LinkedHashMultimap: maintain insertion order and for fast removals
    // Results in blocks stored in descending order based on block size
    Multimap<String, OneBlockInfo> rackToBlocks = LinkedHashMultimap.create();

    // Mapping from a node to the list of blocks that it contains
    // Use LinkedHashMultimap: maintain insertion order and for fast removals
    // Results in blocks stored in descending order based on block size
    Multimap<String, OneBlockInfo> nodeToBlocks = LinkedHashMultimap.create();

    // Mapping from a rack name to the set of Nodes in the rack
    SetMultimap<String, String> rackToNodes = HashMultimap.create();

    final List<OneBlockInfo> allBlocksList = new ArrayList<>();
    for (final FileStatus stat : stats) {
      final OneFileInfo fileInfo = new OneFileInfo(stat, conf,
          isSplitable(job, stat.getPath()), maxSize);
      allBlocksList.addAll(fileInfo.blocks);
    }

    // Sort blocks by size in descending order. When building splits, each split
    // will start with one big block and then fill in with smaller blocks
    // behind it. Makes a greedy algorithm work better for grouping.

    Collections.sort(allBlocksList, new BlockComparator());
    allBlocksSet.addAll(allBlocksList);

    populateBlockInfo(allBlocksSet, nodeToBlocks, rackToBlocks, rackToNodes);

    createSplits(allBlocksSet, nodeToBlocks, rackToBlocks, rackToNodes, maxSize,
        minSizeNode, minSizeRack, splits);
  }

  /**
   * Filling each split with blocks is essentially the 0-1 knapsack problem.
   * This greedy algorithm will create a knapsack by scanning the entire list of
   * blocks, inspecting each block, and if the block fits, it will be added. If
   * the knapsack is exactly full, the search will stop, otherwise, every item
   * is inspected to see if it will fit. The only exception is that if a block
   * is larger than the capacity, it will be returned as the only block in the
   * split.
   *
   * @param capacity The maximum size (bytes) in each split; Less then or equal
   *          to zero means unlimited capacity.
   * @param items The collection of every available blocks that can be put into
   *          the split
   * @return A BlockGroup containing all of the blocks that were able to be
   *         placed into the split without going over the capacity.
   */
  private BlockGroup getKnapsack(final long capacity,
      final Collection<OneBlockInfo> items) {

    final Collection<OneBlockInfo> knapsack = new ArrayList<>();
    long totalSize = 0L;

    final Iterator<OneBlockInfo> it = items.iterator();

    if (capacity <= 0L) {
      while (it.hasNext()) {
        final OneBlockInfo blockInfo = it.next();
        if (blockInfo.isAssigned) {
          it.remove();
          continue;
        }
        knapsack.add(blockInfo);
        totalSize += blockInfo.length;
      }
    } else {
      long currentCapacity = capacity;
      boolean isFirstBlock = true;
      while (it.hasNext() && currentCapacity > 0L) {
        final OneBlockInfo blockInfo = it.next();
        if (blockInfo.isAssigned) {
          it.remove();
          continue;
        }
        if (blockInfo.length <= currentCapacity || isFirstBlock) {
          isFirstBlock = false;
          knapsack.add(blockInfo);
          currentCapacity -= blockInfo.length;
          totalSize += blockInfo.length;
        }
      }
    }

    return new BlockGroup(totalSize, knapsack);
  }

  /**
   * Process all the nodes and create splits that are local to a node. Generate
   * one split per node iteration, and walk over nodes multiple times to
   * distribute the splits across nodes.
   * <p>
   * Note: The order in which nodes are processes are not guaranteed.
   * </p>
   *
   * @param allBlocks A collection of all blocks being grouped into splits
   * @param nodeToBlocks Mapping from a node to the list of blocks that it
   *          contains.
   * @param rackToBlocks Mapping from a rack name to the list of blocks it has.
   * @param rackToNodes Mapping or racks to nodes.
   * @param maxSize Max size of each split. If set to 0, disable smoothing load.
   * @param minSizeNode Minimum split size per node.
   * @param minSizeRack Minimum split size per rack.
   * @param splits Splits created by this method are added to the Collection.
   */
  @VisibleForTesting
  void createSplits(final Collection<OneBlockInfo> allBlocks,
      final Multimap<String, OneBlockInfo> nodeToBlocks,
      final Multimap<String, OneBlockInfo> rackToBlocks,
      final SetMultimap<String, String> rackToNodes, final long maxSize,
      final long minSizeNode, final long minSizeRack,
      final Collection<InputSplit> splits) {

    // ***** Node-Local Splits ***** //
    final Queue<String> nodeQueue = new ArrayDeque<>(nodeToBlocks.keySet());

    while (!nodeQueue.isEmpty()) {
      final String nodeName = nodeQueue.remove();
      final Collection<OneBlockInfo> blocks = nodeToBlocks.get(nodeName);

      final BlockGroup packedBlocks = getKnapsack(maxSize, blocks);
      LOG.trace("Create node-local split: {}", packedBlocks);

      if (minSizeNode != 0L && packedBlocks.getTotalSize() >= minSizeNode) {
        // create an input split and add it to the splits array
        addCreatedSplit(splits, Collections.singleton(nodeName),
            packedBlocks.getBlocks());

        packedBlocks.markAssigned();

        allBlocks.removeAll(packedBlocks.getBlocks());
        blocks.removeAll(packedBlocks.getBlocks());

        nodeQueue.add(nodeName);
      }
    }

    if (allBlocks.isEmpty()) {
      LOG.info("All splits assigned to node-local");
      return;
    }

    // ***** Rack-Local Splits ***** //
    final Queue<String> rackQueue = new ArrayDeque<>(rackToBlocks.keySet());

    while (!rackQueue.isEmpty()) {
      final String rackName = rackQueue.remove();
      final Collection<OneBlockInfo> blocks = rackToBlocks.get(rackName);

      final BlockGroup packedBlocks = getKnapsack(maxSize, blocks);
      LOG.trace("Create rack-local split: {}", packedBlocks);

      if (minSizeRack != 0L && packedBlocks.getTotalSize() >= minSizeRack) {
        // create an input split and add it to the splits array
        addCreatedSplit(splits,
            getHosts(rackToNodes, Collections.singleton(rackName)),
            packedBlocks.getBlocks());

        packedBlocks.markAssigned();

        allBlocks.removeAll(packedBlocks.getBlocks());
        blocks.removeAll(packedBlocks.getBlocks());

        rackQueue.add(rackName);
      }
    }

    if (allBlocks.isEmpty()) {
      LOG.info("All splits assigned to rack-local");
      return;
    }

    // ***** Any Remaining Blocks ***** //
    LOG.debug("All remaining blocks {}", allBlocks);

    while (!allBlocks.isEmpty()) {
      final BlockGroup packedBlocks = getKnapsack(maxSize, allBlocks);
      LOG.trace("Create non-local split: {}", packedBlocks);

      Preconditions.checkState(!packedBlocks.getBlocks().isEmpty());

      final String rackName = getRackMaxData(packedBlocks.getBlocks());

      // create an input split and add it to the splits array
      addCreatedSplit(splits,
          getHosts(rackToNodes, Collections.singleton(rackName)),
          packedBlocks.getBlocks());

      packedBlocks.markAssigned();

      allBlocks.removeAll(packedBlocks.getBlocks());
    }

    LOG.info("Was unable to apply locality to all blocks");
    return;
  }

  /**
   * Given an arbitrary collection of blocks, return the name of the rack
   * containing the most data.
   */
  private String getRackMaxData(final Collection<OneBlockInfo> blocks) {
    final Map<String, LongAdder> rackBlockSizeMap = new HashMap<>();

    // Generate a list of all the racks in this group of blocks and how much
    // data is contained within each rack
    for (final OneBlockInfo block : blocks) {
      for (final String rackName : block.racks) {
        rackBlockSizeMap.computeIfAbsent(rackName, rn -> new LongAdder())
            .add(block.length);
      }
    }

    if (rackBlockSizeMap.isEmpty()) {
      // No rack information present so return no rack name
      return "";
    }

    // Sort the entries by total length
    final List<Map.Entry<String, LongAdder>> list =
        new ArrayList<>(rackBlockSizeMap.entrySet());

    final Entry<String, LongAdder> maxRack =
        Collections.max(list, new Comparator<Map.Entry<String, LongAdder>>() {
          public int compare(Map.Entry<String, LongAdder> o1,
              Map.Entry<String, LongAdder> o2) {
            return Long.compare(o1.getValue().sum(), o2.getValue().sum());
          }
        });

    LOG.debug("Found rack with most data: {}", maxRack);

    return maxRack.getKey();
  }

  /**
   * Create a single split from the list of blocks.
   */
  private void addCreatedSplit(Collection<InputSplit> splitList,
      Collection<String> locations, Collection<OneBlockInfo> blocks) {

    // create an input split
    Path[] fl = new Path[blocks.size()];
    long[] offset = new long[blocks.size()];
    long[] length = new long[blocks.size()];

    int i = 0;
    for (OneBlockInfo block : blocks) {
      fl[i] = block.onepath;
      offset[i] = block.offset;
      length[i] = block.length;
      i++;
    }

    // add this split to the list that is returned
    CombineFileSplit thisSplit = new CombineFileSplit(fl, offset, length,
        locations.toArray(new String[0]));

    LOG.debug("Adding a new combined file split: {}", thisSplit);

    splitList.add(thisSplit);
  }

  @VisibleForTesting
  static void populateBlockInfo(final Collection<OneBlockInfo> allBlocks,
      final Multimap<String, OneBlockInfo> nodeToBlocks,
      final Multimap<String, OneBlockInfo> rackToBlocks,
      final SetMultimap<String, String> rackToNodes) {

    for (OneBlockInfo block : allBlocks) {

      for (final String host : block.hosts) {
        nodeToBlocks.put(host, block);
      }

      for (final String rack : block.racks) {
        rackToBlocks.put(rack, block);
      }

      // For blocks that do not have host/rack information,
      // assign to default rack.
      final String[] racks;
      if (block.hosts.length == 0) {
        racks = new String[] { NetworkTopology.DEFAULT_RACK };
      } else {
        racks = block.racks;
      }

      // add this block to the rack --> block map
      for (int j = 0; j < racks.length; j++) {
        if (!NetworkTopology.DEFAULT_RACK.equals(racks[j])) {
          // Add this host to rackToNodes map
          rackToNodes.put(racks[j], block.hosts[j]);
        }
      }
    }
  }

  @Override
  public abstract RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException;

  /**
   * information about one file from the File System
   */
  @VisibleForTesting
  static class OneFileInfo {

    /** Size of the file. */
    private long fileSize;

    /** All blocks in this file. */
    private final List<OneBlockInfo> blocks;

    OneFileInfo(FileStatus stat, Configuration conf, boolean isSplitable,
        long maxSize) throws IOException {

      this.fileSize = 0;

      // get block locations from file system
      BlockLocation[] locations;
      if (stat instanceof LocatedFileStatus) {
        locations = ((LocatedFileStatus) stat).getBlockLocations();
      } else {
        FileSystem fs = stat.getPath().getFileSystem(conf);
        locations = fs.getFileBlockLocations(stat, 0L, stat.getLen());
      }

      // create a list of all block and their locations
      if (locations == null) {
        blocks = Collections.emptyList();
      } else {

        if (locations.length == 0 && !stat.isDirectory()) {
          locations = new BlockLocation[] { new BlockLocation() };
        }

        if (!isSplitable) {
          // if the file is not splitable, just create the one block with
          // full file length
          LOG.debug("File is not splittable so no parallelization "
              + "is possible: {}", stat.getPath());

          fileSize = stat.getLen();
          blocks = Collections
              .singletonList(new OneBlockInfo(stat.getPath(), 0L, fileSize,
                  locations[0].getHosts(), locations[0].getTopologyPaths()));
        } else {
          blocks = new ArrayList<>(locations.length);

          for (final BlockLocation blockLocation : locations) {
            fileSize += blockLocation.getLength();

            // each split can be a maximum of maxSize
            long left = blockLocation.getLength();
            long myOffset = blockLocation.getOffset();
            long myLength = 0L;
            do {
              if (maxSize <= 0L) {
                myLength = left;
              } else {
                if (left > maxSize && left < Math.multiplyExact(2L, maxSize)) {
                  // if remainder is between max and 2*max - then
                  // instead of creating splits of size max, left-max we
                  // create splits of size left/2 and left/2. This is
                  // a heuristic to avoid creating really really small
                  // splits.
                  myLength = left / 2L;
                } else {
                  myLength = Math.min(maxSize, left);
                }
              }
              OneBlockInfo oneblock = new OneBlockInfo(stat.getPath(), myOffset,
                  myLength, blockLocation.getHosts(),
                  blockLocation.getTopologyPaths());
              left -= myLength;
              myOffset += myLength;

              blocks.add(oneblock);
            } while (left > 0L);
          }
        }
      }
    }

    long getLength() {
      return fileSize;
    }

    Collection<OneBlockInfo> getBlocks() {
      return blocks;
    }
  }

  /**
   * Comparator which sorts by descending block size, by path, by offset
   */
  private static class BlockComparator implements Comparator<OneBlockInfo> {
    @Override
    public int compare(OneBlockInfo o1, OneBlockInfo o2) {
      int c = Long.compare(o2.length, o1.length);
      if (c == 0) {
        c = o1.onepath.compareTo(o2.onepath);
        if (c == 0) {
          c = Long.compare(o1.offset, o2.offset);
        }
      }
      return c;
    }
  }

  /**
   * Information about one block from the File System.
   */
  @VisibleForTesting
  static class OneBlockInfo {
    Path onepath; // name of this file
    long offset; // offset in file
    long length; // length of this block
    String[] hosts; // nodes on which this block resides
    String[] racks; // network topology of hosts
    boolean isAssigned = false;

    OneBlockInfo(Path path, long offset, long len, String[] hosts,
        String[] topologyPaths) {
      this.onepath = path;
      this.offset = offset;
      this.hosts = hosts;
      this.length = len;
      assert (hosts.length == topologyPaths.length
          || topologyPaths.length == 0);

      // if the file system does not have any rack information, then
      // use dummy rack location.
      if (topologyPaths.length == 0) {
        topologyPaths = new String[hosts.length];
        for (int i = 0; i < topologyPaths.length; i++) {
          topologyPaths[i] =
              new NodeBase(hosts[i], NetworkTopology.DEFAULT_RACK).toString();
        }
      }

      // The topology paths have the host name included as the last
      // component. Strip it.
      this.racks = new String[topologyPaths.length];
      for (int i = 0; i < topologyPaths.length; i++) {
        this.racks[i] = new NodeBase(topologyPaths[i]).getNetworkLocation();
      }
    }

    @Override
    public String toString() {
      return "OneBlockInfo [onepath=" + onepath + ", offset=" + offset
          + ", length=" + length + ", hosts=" + Arrays.toString(hosts)
          + ", racks=" + Arrays.toString(racks) + ", isAssigned=" + isAssigned
          + "]";
    }
  }

  protected BlockLocation[] getFileBlockLocations(FileSystem fs,
      FileStatus stat) throws IOException {
    if (stat instanceof LocatedFileStatus) {
      return ((LocatedFileStatus) stat).getBlockLocations();
    }
    return fs.getFileBlockLocations(stat, 0L, stat.getLen());
  }

  private Set<String> getHosts(final SetMultimap<String, String> rackToNodes,
      final Set<String> racks) {
    final Set<String> hosts = new HashSet<>();
    for (final String rack : racks) {
      hosts.addAll(rackToNodes.get(rack));
    }
    return hosts;
  }

  private static class BlockGroup {
    private final long totalSize;
    private final Collection<OneBlockInfo> blocks;

    public BlockGroup(long totalSize, Collection<OneBlockInfo> blocks) {
      this.totalSize = totalSize;
      this.blocks = blocks;
    }

    public long getTotalSize() {
      return totalSize;
    }

    public Collection<OneBlockInfo> getBlocks() {
      return blocks;
    }

    public void markAssigned() {
      this.blocks.forEach(b -> b.isAssigned = true);
    }

    @Override
    public String toString() {
      return "BlockGroup [totalSize=" + totalSize + ", blocks=" + blocks + "]";
    }
  }

  /**
   * Accept a path only if any one of filters given in the constructor do.
   */
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (filter.accept(path)) {
          return true;
        }
      }
      return false;
    }

    public String toString() {
      return '[' + filters.stream().map(f -> f.toString())
          .collect(Collectors.joining(",")) + ']';
    }
  }
}
