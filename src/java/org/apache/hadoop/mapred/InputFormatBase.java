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

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** A base class for {@link InputFormat}. */
public abstract class InputFormatBase implements InputFormat {

  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.mapred.InputFormatBase");

  private static final double SPLIT_SLOP = 1.1;   // 10% slop

  private long minSplitSize = 1;

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
  
  public abstract RecordReader getRecordReader(FileSystem fs,
                                               FileSplit split,
                                               JobConf job,
                                               Reporter reporter)
    throws IOException;

  /** List input directories.
   * Subclasses may override to, e.g., select only files matching a regular
   * expression. 
   * 
   * <p>Property <code>mapred.input.subdir</code>, if set, names a subdirectory
   * that is appended to all input dirs specified by job, and if the given fs
   * lists those too, each is added to the returned array of Path.
   *
   * @param fs
   * @param job
   * @return array of Path objects, never zero length.
   * @throws IOException if zero items.
   */
  protected Path[] listPaths(FileSystem ignored, JobConf job)
    throws IOException {
    Path[] dirs = job.getInputPaths();
    String subdir = job.get("mapred.input.subdir");
    ArrayList result = new ArrayList();
    for (int i = 0; i < dirs.length; i++) {
      FileSystem fs = dirs[i].getFileSystem(job);
      Path[] dir = fs.listPaths(dirs[i]);
      if (dir != null) {
        for (int j = 0; j < dir.length; j++) {
          Path file = dir[j];
          if (subdir != null) {
            Path[] subFiles = fs.listPaths(new Path(file, subdir));
            if (subFiles != null) {
              for (int k = 0; k < subFiles.length; k++) {
                result.add(fs.makeQualified(subFiles[k]));
              }
            }
          } else {
            result.add(fs.makeQualified(file));
          }
        }
      }
    }

    if (result.size() == 0) {
      throw new IOException("No input directories specified in: "+job);
    }
    return (Path[])result.toArray(new Path[result.size()]);
  }

  // NOTE: should really pass a Configuration here, not a FileSystem
  public boolean[] areValidInputDirectories(FileSystem fs, Path[] inputDirs)
    throws IOException {
    boolean[] result = new boolean[inputDirs.length];
    for(int i=0; i < inputDirs.length; ++i) {
      result[i] =
        inputDirs[i].getFileSystem(fs.getConf()).isDirectory(inputDirs[i]);
    }
    return result;
  }

  /** Splits files returned by {@link #listPaths(FileSystem,JobConf)} when
   * they're too big.*/ 
  public FileSplit[] getSplits(FileSystem ignored, JobConf job, int numSplits)
    throws IOException {

    Path[] files = listPaths(ignored, job);

    long totalSize = 0;                           // compute total size
    for (int i = 0; i < files.length; i++) {      // check we have valid files
      Path file = files[i];
      FileSystem fs = file.getFileSystem(job);
      if (fs.isDirectory(file) || !fs.exists(file)) {
        throw new IOException("Not a file: "+files[i]);
      }
      totalSize += fs.getLength(files[i]);
    }

    long goalSize = totalSize / numSplits;   // start w/ desired num splits

    long minSize = Math.max(job.getLong("mapred.min.split.size", 1),
                            minSplitSize);

    ArrayList splits = new ArrayList(numSplits);  // generate splits
    for (int i = 0; i < files.length; i++) {
      Path file = files[i];
      FileSystem fs = file.getFileSystem(job);
      long length = fs.getLength(file);
      if (isSplitable(fs, file)) {
        long blockSize = fs.getBlockSize(file);
        long splitSize = computeSplitSize(goalSize, minSize, blockSize);

        long bytesRemaining = length;
        while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
          splits.add(new FileSplit(file, length-bytesRemaining, splitSize));
          bytesRemaining -= splitSize;
        }
        
        if (bytesRemaining != 0) {
          splits.add(new FileSplit(file, length-bytesRemaining, bytesRemaining));
        }
      } else {
        if (length != 0) {
          splits.add(new FileSplit(file, 0, length));
        }
      }
    }
    //LOG.info( "Total # of splits: " + splits.size() );
    return (FileSplit[])splits.toArray(new FileSplit[splits.size()]);
  }

  private static long computeSplitSize(long goalSize, long minSize,
                                       long blockSize) {
    return Math.max(minSize, Math.min(goalSize, blockSize));
  }
}

