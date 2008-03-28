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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.util.ReflectionUtils;

/** 
 * A base class for file-based {@link InputFormat}.
 * 
 * <p><code>FileInputFormat</code> is the base class for all file-based 
 * <code>InputFormat</code>s. This provides generic implementations of
 * {@link #validateInput(JobConf)} and {@link #getSplits(JobConf, int)}.
 * Implementations fo <code>FileInputFormat</code> can also override the 
 * {@link #isSplitable(FileSystem, Path)} method to ensure input-files are
 * not split-up and are processed as a whole by {@link Mapper}s.
 */
public abstract class FileInputFormat<K, V> implements InputFormat<K, V> {

  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.mapred.FileInputFormat");

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
   * Is the given filename splitable? Usually, true, but if the file is
   * stream compressed, it will not be.
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
    conf.setClass("mapred.input.pathFilter.class", filter, PathFilter.class);
  }

  /**
   * Get a PathFilter instance of the filter set for the input paths.
   *
   * @return the PathFilter instance set for the job, NULL if none has been set.
   */
  public static PathFilter getInputPathFilter(JobConf conf) {
    Class filterClass = conf.getClass("mapred.input.pathFilter.class", null,
        PathFilter.class);
    return (filterClass != null) ?
        (PathFilter) ReflectionUtils.newInstance(filterClass, conf) : null;
  }

  /** List input directories.
   * Subclasses may override to, e.g., select only files matching a regular
   * expression. 
   * 
   * @param job the job to list input paths for
   * @return array of Path objects
   * @throws IOException if zero items.
   */
  protected Path[] listPaths(JobConf job)
    throws IOException {
    Path[] dirs = job.getInputPaths();
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    List<Path> result = new ArrayList<Path>();

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);

    for (Path p: dirs) {
      FileSystem fs = p.getFileSystem(job); 
      Path[] matches =
        fs.listPaths(fs.globPaths(p, inputFilter), inputFilter);
      for (Path match: matches) {
        result.add(fs.makeQualified(match));
      }
    }

    return result.toArray(new Path[result.size()]);
  }

  public void validateInput(JobConf job) throws IOException {
    Path[] inputDirs = job.getInputPaths();
    if (inputDirs.length == 0) {
      throw new IOException("No input paths specified in input"); 
    }
    
    List<IOException> result = new ArrayList<IOException>();
    int totalFiles = 0; 
    for (Path p: inputDirs) {
      FileSystem fs = p.getFileSystem(job);
      if (fs.exists(p)) {
        // make sure all paths are files to avoid exception
        // while generating splits
        for (Path subPath : fs.listPaths(p, hiddenFileFilter)) {
          FileSystem subFS = subPath.getFileSystem(job); 
          if (!subFS.exists(subPath)) {
            result.add(new IOException(
                                       "Input path does not exist: " + subPath)); 
          } else {
            totalFiles++; 
          }
        }
      } else {
        Path [] paths = fs.globPaths(p, hiddenFileFilter); 
        if (paths.length == 0) {
          result.add(
                     new IOException("Input Pattern " + p + " matches 0 files")); 
        } else {
          // validate globbed paths 
          for (Path gPath : paths) {
            FileSystem gPathFS = gPath.getFileSystem(job); 
            if (!gPathFS.exists(gPath)) {
              result.add(
                         new FileNotFoundException(
                                                   "Input path doesnt exist : " + gPath)); 
            }
          }
          totalFiles += paths.length; 
        }
      }
    }
    if (!result.isEmpty()) {
      throw new InvalidInputException(result);
    }
    // send output to client. 
    LOG.info("Total input paths to process : " + totalFiles); 
  }

  /** Splits files returned by {@link #listPaths(JobConf)} when
   * they're too big.*/ 
  public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    Path[] files = listPaths(job);
    long totalSize = 0;                           // compute total size
    for (int i = 0; i < files.length; i++) {      // check we have valid files
      Path file = files[i];
      FileSystem fs = file.getFileSystem(job);
      if (fs.isDirectory(file) || !fs.exists(file)) {
        throw new IOException("Not a file: "+files[i]);
      }
      totalSize += fs.getLength(files[i]);
    }

    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    long minSize = Math.max(job.getLong("mapred.min.split.size", 1),
                            minSplitSize);

    // generate splits
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    for (int i = 0; i < files.length; i++) {
      Path file = files[i];
      FileSystem fs = file.getFileSystem(job);
      long length = fs.getLength(file);
      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
      if ((length != 0) && isSplitable(fs, file)) { 
        long blockSize = fs.getBlockSize(file);
        long splitSize = computeSplitSize(goalSize, minSize, blockSize);

        long bytesRemaining = length;
        while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
          int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
          splits.add(new FileSplit(file, length-bytesRemaining, splitSize, 
                                   blkLocations[blkIndex].getHosts()));
          bytesRemaining -= splitSize;
        }
        
        if (bytesRemaining != 0) {
          splits.add(new FileSplit(file, length-bytesRemaining, bytesRemaining, 
                     blkLocations[blkLocations.length-1].getHosts()));
        }
      } else if (length != 0) {
        splits.add(new FileSplit(file, 0, length, blkLocations[0].getHosts()));
      } else { 
        //Create empty hosts array for zero length files
        splits.add(new FileSplit(file, 0, length, new String[0]));
      }
    }
    LOG.debug("Total # of splits: " + splits.size());
    return splits.toArray(new FileSplit[splits.size()]);
  }

  protected long computeSplitSize(long goalSize, long minSize,
                                       long blockSize) {
    return Math.max(minSize, Math.min(goalSize, blockSize));
  }

  protected int getBlockIndex(BlockLocation[] blkLocations, 
                              long offset) {
    for (int i = 0 ; i < blkLocations.length; i++) {
      if ((blkLocations[i].getOffset() <= offset) &&
        ((blkLocations[i].getOffset() + blkLocations[i].getLength()) >= 
        offset))
          return i;
    }
    return 0;
  }
}
