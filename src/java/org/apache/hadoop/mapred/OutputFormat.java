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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;

/** An output data format.  Output files are stored in a {@link
 * FileSystem}. */
public interface OutputFormat {

  /** Construct a {@link RecordWriter} with Progressable.
   *
   * @param fs the file system to write to
   * @param job the job whose output is being written
   * @param name the unique name for this part of the output
   * @param progress mechanism for reporting progress while writing to file
   * @return a {@link RecordWriter}
   */
  RecordWriter getRecordWriter(FileSystem fs, JobConf job, String name,
          Progressable progress)
    throws IOException;

  /** Check whether the output specification for a job is appropriate.  Called
   * when a job is submitted.  Typically checks that it does not already exist,
   * throwing an exception when it already exists, so that output is not
   * overwritten.
   *
   * @param job the job whose output will be written
   * @throws IOException when output should not be attempted
   */
  void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException;
}

