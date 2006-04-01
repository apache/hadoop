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
import java.io.File;

import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.LogFormatter;

/** A base class for {@link InputFormat}. */
public abstract class InputFormatBase implements InputFormat {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.hadoop.mapred.InputFormatBase");

  private static final double SPLIT_SLOP = 0.1;   // 10% slop

  private long minSplitSize = 1;

  protected void setMinSplitSize(long minSplitSize) {
    this.minSplitSize = minSplitSize;
  }

  public abstract RecordReader getRecordReader(FileSystem fs,
                                               FileSplit split,
                                               JobConf job,
                                               Reporter reporter)
    throws IOException;

  /** List input directories.
   * Subclasses may override to, e.g., select only files matching a regular
   * expression.
   * Property mapred.input.subdir, if set, names a subdirectory that
   * is appended to all input dirs specified by job, and if the given fs
   * lists those too, each is added to the returned array of File.
   * @param fs
   * @param job
   * @return array of File objects, never zero length.
   * @throws IOException if zero items.
   */
  protected File[] listFiles(FileSystem fs, JobConf job)
    throws IOException {
    File[] dirs = job.getInputDirs();
    String workDir = job.getWorkingDirectory();
    String subdir = job.get("mapred.input.subdir");
    ArrayList result = new ArrayList();
    for (int i = 0; i < dirs.length; i++) {
      // if it is relative, make it absolute using the directory from the 
      // JobConf
      if (workDir != null && !fs.isAbsolute(dirs[i])) {
        dirs[i] = new File(workDir, dirs[i].toString());
      }
      File[] dir = fs.listFiles(dirs[i]);
      if (dir != null) {
        for (int j = 0; j < dir.length; j++) {
          File file = dir[j];
          if (subdir != null) {
            File[] subFiles = fs.listFiles(new File(file, subdir));
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
    return (File[])result.toArray(new File[result.size()]);
  }

  /** Splits files returned by {#listFiles(FileSystem,JobConf) when
   * they're too big.*/ 
  public FileSplit[] getSplits(FileSystem fs, JobConf job, int numSplits)
    throws IOException {

    File[] files = listFiles(fs, job);

    for (int i = 0; i < files.length; i++) {      // check we have valid files
      File file = files[i];
      if (fs.isDirectory(file) || !fs.exists(file)) {
        throw new IOException("Not a file: "+files[i]);
      }
    }

    long totalSize = 0;                           // compute total size
    for (int i = 0; i < files.length; i++) {
      totalSize += fs.getLength(files[i]);
    }

    long bytesPerSplit = totalSize / numSplits;   // start w/ desired num splits

    long fsBlockSize = fs.getBlockSize();
    if (bytesPerSplit > fsBlockSize) {            // no larger than fs blocks
      bytesPerSplit = fsBlockSize;
    }

    long configuredMinSplitSize = job.getLong("mapred.min.split.size", 0);
    if( configuredMinSplitSize < minSplitSize )
    	configuredMinSplitSize = minSplitSize;
    if (bytesPerSplit < configuredMinSplitSize) { // no smaller than min size
      bytesPerSplit = configuredMinSplitSize;
    }

    long maxPerSplit = bytesPerSplit + (long)(bytesPerSplit*SPLIT_SLOP);

    //LOG.info("bytesPerSplit = " + bytesPerSplit);
    //LOG.info("maxPerSplit = " + maxPerSplit);

    ArrayList splits = new ArrayList(numSplits);  // generate splits
    for (int i = 0; i < files.length; i++) {
      File file = files[i];
      long length = fs.getLength(file);

      long bytesRemaining = length;
      while (bytesRemaining >= maxPerSplit) {
        splits.add(new FileSplit(file, length-bytesRemaining, bytesPerSplit));
        bytesRemaining -= bytesPerSplit;
      }
      
      if (bytesRemaining != 0) {
        splits.add(new FileSplit(file, length-bytesRemaining, bytesRemaining));
      }
      //LOG.info( "Generating splits for " + i + "th file: " + file.getName() );
    }
    //LOG.info( "Total # of splits: " + splits.size() );
    return (FileSplit[])splits.toArray(new FileSplit[splits.size()]);
  }

}

