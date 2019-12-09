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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * A base class for file-based {@link InputFormat}.
 * 
 * <p><code>FileInputFormat</code> is the base class for all file-based 
 * <code>InputFormat</code>s. This provides a generic implementation of
 * {@link #getSplits(JobConf, int)}.
 *
 * Implementations of <code>FileInputFormat</code> can also override the
 * {@link #isSplitable(FileSystem, Path)} method to prevent input files
 * from being split-up in certain situations. Implementations that may
 * deal with non-splittable files <i>must</i> override this method, since
 * the default implementation assumes splitting is always possible.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileInputFormat<K, V> implements InputFormat<K, V> {

  public static final Logger LOG =
      LoggerFactory.getLogger(FileInputFormat.class);
  
  @Deprecated
  public enum Counter {
    BYTES_READ
  }

  public static final String NUM_INPUT_FILES =
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.NUM_INPUT_FILES;

  public static final String INPUT_DIR_RECURSIVE = 
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_RECURSIVE;

  public static final String INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS =
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS;


  private static final double SPLIT_SLOP = 1.1;   // 10% slop

  private long minSplitSize = 1;
  private static final PathFilter hiddenFileFilter = new PathFilter(){
      public boolean accept(Path p){
        String name = p.getName(); 
        return !name.startsWith("_") && !name.startsWith("."); 
      }
    }; 
  protected void setMinSplitSize(long minSplitSize) {
    this.minSplitSize = minSplitSize;
  }

  /**
   * Proxy PathFilter that accepts a path only if all filters given in the
   * constructor do. Used by the listPaths() to apply the built-in
   * hiddenFileFilter together with a user provided one (if any).
   */
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Is the given filename splittable? Usually, true, but if the file is
   * stream compressed, it will not be.
   *
   * The default implementation in <code>FileInputFormat</code> always returns
   * true. Implementations that may deal with non-splittable files <i>must</i>
   * override this method.
   *
   * <code>FileInputFormat</code> implementations can override this and return
   * <code>false</code> to ensure that individual input files are never split-up
   * so that {@link Mapper}s process entire files.
   * 
   * @param fs the file system that the file is on
   * @param filename the file name to check
   * @return is this file splitable?
   */
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return true;
  }
  
  public abstract RecordReader<K, V> getRecordReader(InputSplit split,
                                               JobConf job,
                                               Reporter reporter)
    throws IOException;

  /**
   * Set a PathFilter to be applied to the input paths for the map-reduce job.
   *
   * @param filter the PathFilter class use for filtering the input paths.
   */
  public static void setInputPathFilter(JobConf conf,
                                        Class<? extends PathFilter> filter) {
    conf.setClass(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.PATHFILTER_CLASS, filter, PathFilter.class);
  }

  /**
   * Get a PathFilter instance of the filter set for the input paths.
   *
   * @return the PathFilter instance set for the job, NULL if none has been set.
   */
  public static PathFilter getInputPathFilter(JobConf conf) {
    Class<? extends PathFilter> filterClass = conf.getClass(
	  org.apache.hadoop.mapreduce.lib.input.FileInputFormat.PATHFILTER_CLASS,
	  null, PathFilter.class);
    return (filterClass != null) ?
        ReflectionUtils.newInstance(filterClass, conf) : null;
  }

  /**
   * Add files in the input path recursively into the results.
   * @param result
   *          The List to store all files.
   * @param fs
   *          The FileSystem.
   * @param path
   *          The input path.
   * @param inputFilter
   *          The input filter that can be used to filter files/dirs. 
   * @throws IOException
   */
  protected void addInputPathRecursively(List<FileStatus> result,
      FileSystem fs, Path path, PathFilter inputFilter) 
      throws IOException {
    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
    while (iter.hasNext()) {
      LocatedFileStatus stat = iter.next();
      if (inputFilter.accept(stat.getPath())) {
        if (stat.isDirectory()) {
          addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
        } else {
          result.add(stat);
        }
      }
    }
  }
  
  /** List input directories.
   * Subclasses may override to, e.g., select only files matching a regular
   * expression. 
   * 
   * @param job the job to list input paths for
   * @return array of FileStatus objects
   * @throws IOException if zero items.
   */
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    // get tokens for all the required FileSystems..
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job);
    
    // Whether we need to recursive look into the directory structure
    boolean recursive = job.getBoolean(INPUT_DIR_RECURSIVE, false);

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);

    FileStatus[] result;
    int numThreads = job
        .getInt(
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.LIST_STATUS_NUM_THREADS,
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.DEFAULT_LIST_STATUS_NUM_THREADS);
    
    StopWatch sw = new StopWatch().start();
    if (numThreads == 1) {
      List<FileStatus> locatedFiles = singleThreadedListStatus(job, dirs, inputFilter, recursive); 
      result = locatedFiles.toArray(new FileStatus[locatedFiles.size()]);
    } else {
      Iterable<FileStatus> locatedFiles = null;
      try {
        
        LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(
            job, dirs, recursive, inputFilter, false);
        locatedFiles = locatedFileStatusFetcher.getFileStatuses();
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while getting file statuses");
      }
      result = Iterables.toArray(locatedFiles, FileStatus.class);
    }

    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time taken to get FileStatuses: "
          + sw.now(TimeUnit.MILLISECONDS));
    }
    LOG.info("Total input files to process : " + result.length);
    return result;
  }
  
  private List<FileStatus> singleThreadedListStatus(JobConf job, Path[] dirs,
      PathFilter inputFilter, boolean recursive) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    List<IOException> errors = new ArrayList<IOException>();
    for (Path p: dirs) {
      FileSystem fs = p.getFileSystem(job); 
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          if (globStat.isDirectory()) {
            RemoteIterator<LocatedFileStatus> iter =
                fs.listLocatedStatus(globStat.getPath());
            while (iter.hasNext()) {
              LocatedFileStatus stat = iter.next();
              if (inputFilter.accept(stat.getPath())) {
                if (recursive && stat.isDirectory()) {
                  addInputPathRecursively(result, fs, stat.getPath(),
                      inputFilter);
                } else {
                  result.add(stat);
                }
              }
            }
          } else {
            result.add(globStat);
          }
        }
      }
    }
    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    return result;
  }

  /**
   * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
  protected FileSplit makeSplit(Path file, long start, long length, 
                                String[] hosts) {
    return new FileSplit(file, start, length, hosts);
  }
  
  /**
   * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
  protected FileSplit makeSplit(Path file, long start, long length, 
                                String[] hosts, String[] inMemoryHosts) {
    return new FileSplit(file, start, length, hosts, inMemoryHosts);
  }

  /** Splits files returned by {@link #listStatus(JobConf)} when
   * they're too big.*/ 
  public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    StopWatch sw = new StopWatch().start();
    FileStatus[] stats = listStatus(job);

    // Save the number of input files for metrics/loadgen
    job.setLong(NUM_INPUT_FILES, stats.length);
    long totalSize = 0;                           // compute total size
    boolean ignoreDirs = !job.getBoolean(INPUT_DIR_RECURSIVE, false)
      && job.getBoolean(INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS, false);

    List<FileStatus> files = new ArrayList<>(stats.length);
    for (FileStatus file: stats) {                // check we have valid files
      if (file.isDirectory()) {
        if (!ignoreDirs) {
          throw new IOException("Not a file: "+ file.getPath());
        }
      } else {
        files.add(file);
        totalSize += file.getLen();
      }
    }

    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

    // generate splits
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        FileSystem fs = path.getFileSystem(job);
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        if (isSplitable(fs, path)) {
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(goalSize, minSize, blockSize);

          long bytesRemaining = length;
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                length-bytesRemaining, splitSize, clusterMap);
            splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                splitHosts[0], splitHosts[1]));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
                - bytesRemaining, bytesRemaining, clusterMap);
            splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                splitHosts[0], splitHosts[1]));
          }
        } else {
          if (LOG.isDebugEnabled()) {
            // Log only if the file is big enough to be splitted
            if (length > Math.min(file.getBlockSize(), minSize)) {
              LOG.debug("File is not splittable so no parallelization "
                  + "is possible: " + file.getPath());
            }
          }
          String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,0,length,clusterMap);
          splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
        }
      } else { 
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
          + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
    }
    return splits.toArray(new FileSplit[splits.size()]);
  }

  protected long computeSplitSize(long goalSize, long minSize,
                                       long blockSize) {
    return Math.max(minSize, Math.min(goalSize, blockSize));
  }

  protected int getBlockIndex(BlockLocation[] blkLocations, 
                              long offset) {
    for (int i = 0 ; i < blkLocations.length; i++) {
      // is the offset inside this block?
      if ((blkLocations[i].getOffset() <= offset) &&
          (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())){
        return i;
      }
    }
    BlockLocation last = blkLocations[blkLocations.length -1];
    long fileLength = last.getOffset() + last.getLength() -1;
    throw new IllegalArgumentException("Offset " + offset + 
                                       " is outside of file (0.." +
                                       fileLength + ")");
  }

  /**
   * Sets the given comma separated paths as the list of inputs 
   * for the map-reduce job.
   * 
   * @param conf Configuration of the job
   * @param commaSeparatedPaths Comma separated paths to be set as 
   *        the list of inputs for the map-reduce job.
   */
  public static void setInputPaths(JobConf conf, String commaSeparatedPaths) {
    setInputPaths(conf, StringUtils.stringToPath(
                        getPathStrings(commaSeparatedPaths)));
  }

  /**
   * Add the given comma separated paths to the list of inputs for
   *  the map-reduce job.
   * 
   * @param conf The configuration of the job 
   * @param commaSeparatedPaths Comma separated paths to be added to
   *        the list of inputs for the map-reduce job.
   */
  public static void addInputPaths(JobConf conf, String commaSeparatedPaths) {
    for (String str : getPathStrings(commaSeparatedPaths)) {
      addInputPath(conf, new Path(str));
    }
  }

  /**
   * Set the array of {@link Path}s as the list of inputs
   * for the map-reduce job.
   * 
   * @param conf Configuration of the job. 
   * @param inputPaths the {@link Path}s of the input directories/files 
   * for the map-reduce job.
   */ 
  public static void setInputPaths(JobConf conf, Path... inputPaths) {
    Path path = new Path(conf.getWorkingDirectory(), inputPaths[0]);
    StringBuffer str = new StringBuffer(StringUtils.escapeString(path.toString()));
    for(int i = 1; i < inputPaths.length;i++) {
      str.append(StringUtils.COMMA_STR);
      path = new Path(conf.getWorkingDirectory(), inputPaths[i]);
      str.append(StringUtils.escapeString(path.toString()));
    }
    conf.set(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.INPUT_DIR, str.toString());
  }

  /**
   * Add a {@link Path} to the list of inputs for the map-reduce job.
   * 
   * @param conf The configuration of the job 
   * @param path {@link Path} to be added to the list of inputs for 
   *            the map-reduce job.
   */
  public static void addInputPath(JobConf conf, Path path ) {
    path = new Path(conf.getWorkingDirectory(), path);
    String dirStr = StringUtils.escapeString(path.toString());
    String dirs = conf.get(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.INPUT_DIR);
    conf.set(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.INPUT_DIR, dirs == null ? dirStr :
      dirs + StringUtils.COMMA_STR + dirStr);
  }
         
  // This method escapes commas in the glob pattern of the given paths.
  private static String[] getPathStrings(String commaSeparatedPaths) {
    int length = commaSeparatedPaths.length();
    int curlyOpen = 0;
    int pathStart = 0;
    boolean globPattern = false;
    List<String> pathStrings = new ArrayList<String>();
    
    for (int i=0; i<length; i++) {
      char ch = commaSeparatedPaths.charAt(i);
      switch(ch) {
        case '{' : {
          curlyOpen++;
          if (!globPattern) {
            globPattern = true;
          }
          break;
        }
        case '}' : {
          curlyOpen--;
          if (curlyOpen == 0 && globPattern) {
            globPattern = false;
          }
          break;
        }
        case ',' : {
          if (!globPattern) {
            pathStrings.add(commaSeparatedPaths.substring(pathStart, i));
            pathStart = i + 1 ;
          }
          break;
        }
        default:
          continue; // nothing special to do for this character
      }
    }
    pathStrings.add(commaSeparatedPaths.substring(pathStart, length));
    
    return pathStrings.toArray(new String[0]);
  }
  
  /**
   * Get the list of input {@link Path}s for the map-reduce job.
   * 
   * @param conf The configuration of the job 
   * @return the list of input {@link Path}s for the map-reduce job.
   */
  public static Path[] getInputPaths(JobConf conf) {
    String dirs = conf.get(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.INPUT_DIR, "");
    String [] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }
  

  private void sortInDescendingOrder(List<NodeInfo> mylist) {
    Collections.sort(mylist, new Comparator<NodeInfo> () {
      public int compare(NodeInfo obj1, NodeInfo obj2) {

        if (obj1 == null || obj2 == null)
          return -1;

        if (obj1.getValue() == obj2.getValue()) {
          return 0;
        }
        else {
          return ((obj1.getValue() < obj2.getValue()) ? 1 : -1);
        }
      }
    }
    );
  }

  /** 
   * This function identifies and returns the hosts that contribute 
   * most for a given split. For calculating the contribution, rack
   * locality is treated on par with host locality, so hosts from racks
   * that contribute the most are preferred over hosts on racks that 
   * contribute less
   * @param blkLocations The list of block locations
   * @param offset 
   * @param splitSize 
   * @return an array of hosts that contribute most to this split
   * @throws IOException
   */
  protected String[] getSplitHosts(BlockLocation[] blkLocations, 
      long offset, long splitSize, NetworkTopology clusterMap) throws IOException {
    return getSplitHostsAndCachedHosts(blkLocations, offset, splitSize,
        clusterMap)[0];
  }
  
  /** 
   * This function identifies and returns the hosts that contribute 
   * most for a given split. For calculating the contribution, rack
   * locality is treated on par with host locality, so hosts from racks
   * that contribute the most are preferred over hosts on racks that 
   * contribute less
   * @param blkLocations The list of block locations
   * @param offset 
   * @param splitSize 
   * @return two arrays - one of hosts that contribute most to this split, and
   *    one of hosts that contribute most to this split that have the data
   *    cached on them
   * @throws IOException
   */
  private String[][] getSplitHostsAndCachedHosts(BlockLocation[] blkLocations, 
      long offset, long splitSize, NetworkTopology clusterMap)
  throws IOException {

    int startIndex = getBlockIndex(blkLocations, offset);

    long bytesInThisBlock = blkLocations[startIndex].getOffset() + 
                          blkLocations[startIndex].getLength() - offset;

    //If this is the only block, just return
    if (bytesInThisBlock >= splitSize) {
      return new String[][] { blkLocations[startIndex].getHosts(),
          blkLocations[startIndex].getCachedHosts() };
    }

    long bytesInFirstBlock = bytesInThisBlock;
    int index = startIndex + 1;
    splitSize -= bytesInThisBlock;

    while (splitSize > 0) {
      bytesInThisBlock =
        Math.min(splitSize, blkLocations[index++].getLength());
      splitSize -= bytesInThisBlock;
    }

    long bytesInLastBlock = bytesInThisBlock;
    int endIndex = index - 1;
    
    Map <Node,NodeInfo> hostsMap = new IdentityHashMap<Node,NodeInfo>();
    Map <Node,NodeInfo> racksMap = new IdentityHashMap<Node,NodeInfo>();
    String [] allTopos = new String[0];

    // Build the hierarchy and aggregate the contribution of 
    // bytes at each level. See TestGetSplitHosts.java 

    for (index = startIndex; index <= endIndex; index++) {

      // Establish the bytes in this block
      if (index == startIndex) {
        bytesInThisBlock = bytesInFirstBlock;
      }
      else if (index == endIndex) {
        bytesInThisBlock = bytesInLastBlock;
      }
      else {
        bytesInThisBlock = blkLocations[index].getLength();
      }
      
      allTopos = blkLocations[index].getTopologyPaths();

      // If no topology information is available, just
      // prefix a fakeRack
      if (allTopos.length == 0) {
        allTopos = fakeRacks(blkLocations, index);
      }

      // NOTE: This code currently works only for one level of
      // hierarchy (rack/host). However, it is relatively easy
      // to extend this to support aggregation at different
      // levels 
      
      for (String topo: allTopos) {

        Node node, parentNode;
        NodeInfo nodeInfo, parentNodeInfo;

        node = clusterMap.getNode(topo);

        if (node == null) {
          node = new NodeBase(topo);
          clusterMap.add(node);
        }
        
        nodeInfo = hostsMap.get(node);
        
        if (nodeInfo == null) {
          nodeInfo = new NodeInfo(node);
          hostsMap.put(node,nodeInfo);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
          if (parentNodeInfo == null) {
            parentNodeInfo = new NodeInfo(parentNode);
            racksMap.put(parentNode,parentNodeInfo);
          }
          parentNodeInfo.addLeaf(nodeInfo);
        }
        else {
          nodeInfo = hostsMap.get(node);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
        }

        nodeInfo.addValue(index, bytesInThisBlock);
        parentNodeInfo.addValue(index, bytesInThisBlock);

      } // for all topos
    
    } // for all indices

    // We don't yet support cached hosts when bytesInThisBlock > splitSize
    return new String[][] { identifyHosts(allTopos.length, racksMap),
        new String[0]};
  }
  
  private String[] identifyHosts(int replicationFactor, 
                                 Map<Node,NodeInfo> racksMap) {
    
    String [] retVal = new String[replicationFactor];
   
    List <NodeInfo> rackList = new LinkedList<NodeInfo>(); 

    rackList.addAll(racksMap.values());
    
    // Sort the racks based on their contribution to this split
    sortInDescendingOrder(rackList);
    
    boolean done = false;
    int index = 0;
    
    // Get the host list for all our aggregated items, sort
    // them and return the top entries
    for (NodeInfo ni: rackList) {

      Set<NodeInfo> hostSet = ni.getLeaves();

      List<NodeInfo>hostList = new LinkedList<NodeInfo>();
      hostList.addAll(hostSet);
    
      // Sort the hosts in this rack based on their contribution
      sortInDescendingOrder(hostList);

      for (NodeInfo host: hostList) {
        // Strip out the port number from the host name
        retVal[index++] = host.node.getName().split(":")[0];
        if (index == replicationFactor) {
          done = true;
          break;
        }
      }
      
      if (done == true) {
        break;
      }
    }
    return retVal;
  }
  
  private String[] fakeRacks(BlockLocation[] blkLocations, int index) 
  throws IOException {
    String[] allHosts = blkLocations[index].getHosts();
    String[] allTopos = new String[allHosts.length];
    for (int i = 0; i < allHosts.length; i++) {
      allTopos[i] = NetworkTopology.DEFAULT_RACK + "/" + allHosts[i];
    }
    return allTopos;
  }


  private static class NodeInfo {
    final Node node;
    final Set<Integer> blockIds;
    final Set<NodeInfo> leaves;

    private long value;
    
    NodeInfo(Node node) {
      this.node = node;
      blockIds = new HashSet<Integer>();
      leaves = new HashSet<NodeInfo>();
    }

    long getValue() {return value;}

    void addValue(int blockIndex, long value) {
      if (blockIds.add(blockIndex) == true) {
        this.value += value;
      }
    }

    Set<NodeInfo> getLeaves() { return leaves;}

    void addLeaf(NodeInfo nodeInfo) {
      leaves.add(nodeInfo);
    }
  }
}
