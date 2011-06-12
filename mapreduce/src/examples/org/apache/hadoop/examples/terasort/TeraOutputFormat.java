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

package org.apache.hadoop.examples.terasort;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Progressable;

/**
 * An output format that writes the key and value appended together.
 */
public class TeraOutputFormat extends FileOutputFormat<Text,Text> {
  static final String FINAL_SYNC_ATTRIBUTE = "terasort.final.sync";

  /**
   * Set the requirement for a final sync before the stream is closed.
   */
  public static void setFinalSync(JobConf conf, boolean newValue) {
    conf.setBoolean(FINAL_SYNC_ATTRIBUTE, newValue);
  }

  /**
   * Does the user want a final sync at close?
   */
  public static boolean getFinalSync(JobConf conf) {
    return conf.getBoolean(FINAL_SYNC_ATTRIBUTE, false);
  }

  static class TeraRecordWriter implements RecordWriter<Text,Text> {
    private boolean finalSync = false;
    private FSDataOutputStream out;

    public TeraRecordWriter(FSDataOutputStream out,
                            JobConf conf) {
      finalSync = getFinalSync(conf);
      this.out = out;
    }

    public synchronized void write(Text key, 
                                   Text value) throws IOException {
      out.write(key.getBytes(), 0, key.getLength());
      out.write(value.getBytes(), 0, value.getLength());
    }
    
    public void close(Reporter reporter) throws IOException {
      if (finalSync) {
        out.sync();
      }
      out.close();
    }
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, 
                               JobConf job
                              ) throws InvalidJobConfException, IOException {
    // Ensure that the output directory is set and not already there
    Path outDir = getOutputPath(job);
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set in JobConf.");
    }
  }

  public RecordWriter<Text,Text> getRecordWriter(FileSystem ignored,
                                                 JobConf job,
                                                 String name,
                                                 Progressable progress
                                                 ) throws IOException {
    Path dir = getWorkOutputPath(job);
    FileSystem fs = dir.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
    return new TeraRecordWriter(fileOut, job);
  }
  
  public static class TeraOutputCommitter extends FileOutputCommitter {

    @Override
    public void cleanupJob(JobContext jobContext) {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return taskContext.getTaskAttemptID().getTaskID().getTaskType() ==
               TaskType.REDUCE;
    }

    @Override
    public void setupJob(JobContext jobContext) {
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) {
    }
  }
}
