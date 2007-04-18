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

/** 
 * A base class for {@link InputFormat}. 
 * 
 */
public abstract class FileInputFormat implements InputFormat {

  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.mapred.FileInputFormat");

  private static final double SPLIT_SLOP = 1.1;   // 10% slop

  private long minSplitSize = 1;
  private static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept( Path p ){
      String name = p.getName(); 
      return !name.startsWith("_") && !name.startsWith("."); 
    }
  }; 
  protected void setMinSplitSize(long minSplitSize) {
    this.minSplitSize = minSplitSize;
  }

  /**
   * Is the given filename splitable? Usually, true, but if the file is
   * stream compressed, it will not be.
   * @param fs the file system that the file is on
   * @param filename the file name to check
   * @return is this file splitable?
   */
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return true;
  }
  
  public abstract RecordReader getRecordReader(InputSplit split,
                                               JobConf job,
                                               Reporter reporter)
    throws IOException;

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
    for (Path p: dirs) {
      FileSystem fs = p.getFileSystem(job); 
      Path[] matches =
        fs.listPaths(fs.globPaths(p, hiddenFileFilter),hiddenFileFilter);
      for (Path match: matches) {
        result.add(fs.makeQualified(match));
      }
    }

    return (Path[])result.toArray(new Path[result.size()]);
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
          totalFiles += paths.length ; 
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
      if (isSplitable(fs, file)) { 
        long blockSize = fs.getBlockSize(file);
        long splitSize = computeSplitSize(goalSize, minSize, blockSize);

        long bytesRemaining = length;
        while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
          splits.add(new FileSplit(file, length-bytesRemaining, splitSize,
                                   job));
          bytesRemaining -= splitSize;
        }
        
        if (bytesRemaining != 0) {
          splits.add(new FileSplit(file, length-bytesRemaining, 
                                   bytesRemaining, job));
        }
      } else {
        if (length != 0) {
          splits.add(new FileSplit(file, 0, length, job));
        }
      }
    }
    LOG.debug( "Total # of splits: " + splits.size() );
    return splits.toArray(new FileSplit[splits.size()]);
  }

  private static long computeSplitSize(long goalSize, long minSize,
                                       long blockSize) {
    return Math.max(minSize, Math.min(goalSize, blockSize));
  }
}
