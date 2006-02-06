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

import org.apache.hadoop.fs.FileSystem;

/** An input data format.  Input files are stored in a {@link FileSystem}.
 * The processing of an input file may be split across multiple machines.
 * Files are processed as sequences of records, implementing {@link
 * RecordReader}.  Files must thus be split on record boundaries. */
public interface InputFormat {

  /** Splits a set of input files.  One split is created per map task.
   *
   * @param fs the filesystem containing the files to be split
   * @param job the job whose input files are to be split
   * @param numSplits the desired number of splits
   * @return the splits
   */
  FileSplit[] getSplits(FileSystem fs, JobConf job, int numSplits)
    throws IOException;

  /** Construct a {@link RecordReader} for a {@link FileSplit}.
   *
   * @param fs the {@link FileSystem}
   * @param split the {@link FileSplit}
   * @param job the job that this split belongs to
   * @return a {@link RecordReader}
   */
  RecordReader getRecordReader(FileSystem fs, FileSplit split,
                               JobConf job, Reporter reporter)
    throws IOException;
}

