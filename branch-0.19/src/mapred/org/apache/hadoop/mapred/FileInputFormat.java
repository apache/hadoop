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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/** 
 * A base class for file-based {@link InputFormat}.
 * 
 * <p><code>FileInputFormat</code> is the base class for all file-based 
 * <code>InputFormat</code>s. This provides a generic implementation of
 * {@link #getSplits(JobConf, int)}.
 * Subclasses of <code>FileInputFormat</code> can also override the 
 * {@link #isSplitable(FileSystem, Path)} method to ensure input-files are
 * not split-up and are processed as a whole by {@link Mapper}s.
 */
public abstract class FileInputFormat<K, V> implements InputFormat<K, V> {

  public static final Log LOG =
    LogFactory.getLog(FileInputFormat.class);

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
    Class<? extends PathFilter> filterClass = conf.getClass(
	"mapred.input.pathFilter.class", null, PathFilter.class);
    return (filterClass != null) ?
        ReflectionUtils.newInstance(filterClass, conf) : null;
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

    List<FileStatus> result = new ArrayList<FileStatus>();
    List<IOException> errors = new ArrayList<IOException>();
    
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
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          if (globStat.isDir()) {
            for(FileStatus stat: fs.listStatus(globStat.getPath(),
                inputFilter)) {
              result.add(stat);
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
    LOG.info("Total input paths to process : " + result.size()); 
    return result.toArray(new FileStatus[result.size()]);
  }

  /** Splits files returned by {@link #listStatus(JobConf)} when
   * they're too big.*/ 
  @SuppressWarnings("deprecation")
  public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    FileStatus[] files = listStatus(job);
    
    long totalSize = 0;                           // compute total size
    for (FileStatus file: files) {                // check we have valid files
      if (file.isDir()) {
        throw new IOException("Not a file: "+ file.getPath());
      }
      totalSize += file.getLen();
    }

    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    long minSize = Math.max(job.getLong("mapred.min.split.size", 1),
                            minSplitSize);

    // generate splits
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    for (FileStatus file: files) {
      Path path = file.getPath();
      FileSystem fs = path.getFileSystem(job);
      long length = file.getLen();
      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
      if ((length != 0) && isSplitable(fs, path)) { 
        long blockSize = file.getBlockSize();
        long splitSize = computeSplitSize(goalSize, minSize, blockSize);

        long bytesRemaining = length;
        while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
          int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
          splits.add(new FileSplit(path, length-bytesRemaining, splitSize, 
                                   blkLocations[blkIndex].getHosts()));
          bytesRemaining -= splitSize;
        }
        
        if (bytesRemaining != 0) {
          splits.add(new FileSplit(path, length-bytesRemaining, bytesRemaining, 
                     blkLocations[blkLocations.length-1].getHosts()));
        }
      } else if (length != 0) {
        splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
      } else { 
        //Create empty hosts array for zero length files
        splits.add(new FileSplit(path, 0, length, new String[0]));
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
    conf.set("mapred.input.dir", str.toString());
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
    String dirs = conf.get("mapred.input.dir");
    conf.set("mapred.input.dir", dirs == null ? dirStr :
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
    String dirs = conf.get("mapred.input.dir", "");
    String [] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }

}
