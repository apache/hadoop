/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.File;                              // deprecated

import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.LogFormatter;

/** A base class for {@link InputFormat}. */
public abstract class InputFormatBase implements InputFormat {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.hadoop.mapred.InputFormatBase");

  private static final double SPLIT_SLOP = 1.1;   // 10% slop

  private long minSplitSize = 1;

  protected void setMinSplitSize(long minSplitSize) {
    this.minSplitSize = minSplitSize;
  }

  public abstract RecordReader getRecordReader(FileSystem fs,
                                               FileSplit split,
                                               JobConf job,
                                               Reporter reporter)
    throws IOException;

  /** @deprecated Call {@link #listFiles(FileSystem,JobConf)} instead. */
  protected File[] listFiles(FileSystem fs, JobConf job)
    throws IOException {
    Path[] paths = listPaths(fs, job);
    File[] result = new File[paths.length];
    for (int i = 0 ; i < paths.length; i++) {
      result[i] = new File(paths[i].toString());
    }
    return result;
  }

  /** List input directories.
   * Subclasses may override to, e.g., select only files matching a regular
   * expression.
   * Property mapred.input.subdir, if set, names a subdirectory that
   * is appended to all input dirs specified by job, and if the given fs
   * lists those too, each is added to the returned array of Path.
   * @param fs
   * @param job
   * @return array of Path objects, never zero length.
   * @throws IOException if zero items.
   */
  protected Path[] listPaths(FileSystem fs, JobConf job)
    throws IOException {
    Path[] dirs = job.getInputPaths();
    String subdir = job.get("mapred.input.subdir");
    ArrayList result = new ArrayList();
    for (int i = 0; i < dirs.length; i++) {
      Path[] dir = fs.listPaths(dirs[i]);
      if (dir != null) {
        for (int j = 0; j < dir.length; j++) {
          Path file = dir[j];
          if (subdir != null) {
            Path[] subFiles = fs.listPaths(new Path(file, subdir));
            if (subFiles != null) {
              for (int k = 0; k < subFiles.length; k++) {
                result.add(subFiles[k]);
              }
            }
          } else {
            result.add(file);
          }
        }
      }
    }

    if (result.size() == 0) {
      throw new IOException("No input directories specified in: "+job);
    }
    return (Path[])result.toArray(new Path[result.size()]);
  }

  /** Splits files returned by {#listPaths(FileSystem,JobConf) when
   * they're too big.*/ 
  public FileSplit[] getSplits(FileSystem fs, JobConf job, int numSplits)
    throws IOException {

    Path[] files = listPaths(fs, job);

    for (int i = 0; i < files.length; i++) {      // check we have valid files
      Path file = files[i];
      if (fs.isDirectory(file) || !fs.exists(file)) {
        throw new IOException("Not a file: "+files[i]);
      }
    }

    long totalSize = 0;                           // compute total size
    for (int i = 0; i < files.length; i++) {
      totalSize += fs.getLength(files[i]);
    }

    long goalSize = totalSize / numSplits;   // start w/ desired num splits

    long minSize = Math.max(job.getLong("mapred.min.split.size", 1),
                            minSplitSize);

    ArrayList splits = new ArrayList(numSplits);  // generate splits
    for (int i = 0; i < files.length; i++) {
      Path file = files[i];
      long length = fs.getLength(file);
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
      //LOG.info( "Generating splits for " + i + "th file: " + file.getName() );
    }
    //LOG.info( "Total # of splits: " + splits.size() );
    return (FileSplit[])splits.toArray(new FileSplit[splits.size()]);
  }

  private static long computeSplitSize(long goalSize, long minSize,
                                       long blockSize) {
    return Math.max(minSize, Math.min(goalSize, blockSize));
  }
}

