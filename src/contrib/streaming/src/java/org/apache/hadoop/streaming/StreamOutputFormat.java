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

package org.apache.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.mapred.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.util.Progressable;

/** Similar to org.apache.hadoop.mapred.TextOutputFormat, 
 * but delimits key and value with a TAB.
 * @author Michel Tourn
 */
public class StreamOutputFormat implements OutputFormat {

  public RecordWriter getRecordWriter(FileSystem fs, JobConf job, String name, Progressable progr) throws IOException {

    Path file = new Path(job.getOutputPath(), name);

    final FSDataOutputStream out = fs.create(file);

    return new RecordWriter() {

      public synchronized void write(WritableComparable key, Writable value) throws IOException {
        out.write(key.toString().getBytes("UTF-8"));
        out.writeByte('\t');
        out.write(value.toString().getBytes("UTF-8"));
        out.writeByte('\n');
      }

      public synchronized void close(Reporter reporter) throws IOException {
        out.close();
      }
    };
  }

  /** Check whether the output specification for a job is appropriate.  Called
   * when a job is submitted.  Typically checks that it does not already exist,
   * throwing an exception when it already exists, so that output is not
   * overwritten.
   *
   * @param job the job whose output will be written
   * @throws IOException when output should not be attempted
   */
  public void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException {
    // allow existing data (for app-level restartability)
  }

}
