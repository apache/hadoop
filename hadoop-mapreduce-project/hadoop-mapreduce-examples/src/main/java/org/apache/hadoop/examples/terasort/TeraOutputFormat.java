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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An output format that writes the key and value appended together.
 */
public class TeraOutputFormat extends FileOutputFormat<Text,Text> {
  private static final Logger LOG =
      LoggerFactory.getLogger(TeraOutputFormat.class);

  /**
   * Set the requirement for a final sync before the stream is closed.
   */
  static void setFinalSync(JobContext job, boolean newValue) {
    job.getConfiguration().setBoolean(
        TeraSortConfigKeys.FINAL_SYNC_ATTRIBUTE.key(), newValue);
  }

  /**
   * Does the user want a final sync at close?
   */
  public static boolean getFinalSync(JobContext job) {
    return job.getConfiguration().getBoolean(
        TeraSortConfigKeys.FINAL_SYNC_ATTRIBUTE.key(),
        TeraSortConfigKeys.DEFAULT_FINAL_SYNC_ATTRIBUTE);
  }

  static class TeraRecordWriter extends RecordWriter<Text,Text> {
    private boolean finalSync = false;
    private FSDataOutputStream out;

    public TeraRecordWriter(FSDataOutputStream out,
                            JobContext job) {
      finalSync = getFinalSync(job);
      this.out = out;
    }

    public synchronized void write(Text key, 
                                   Text value) throws IOException {
      out.write(key.getBytes(), 0, key.getLength());
      out.write(value.getBytes(), 0, value.getLength());
    }

    public void close(TaskAttemptContext context) throws IOException {
      if (finalSync) {
        try {
          out.hsync();
        } catch (UnsupportedOperationException e) {
          /*
           * Currently, hsync operation on striping file with erasure code
           * policy is not supported yet. So this is a workaround to make
           * teragen and terasort to support directory with striping files. In
           * future, if the hsync operation is supported on striping file, this
           * workaround should be removed.
           */
          LOG.info("Operation hsync is not supported so far on path with " +
                  "erasure code policy set");
        }
      }
      out.close();
    }
  }

  @Override
  public void checkOutputSpecs(JobContext job
                              ) throws InvalidJobConfException, IOException {
    // Ensure that the output directory is set
    Path outDir = getOutputPath(job);
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set in JobConf.");
    }

    final Configuration jobConf = job.getConfiguration();

    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(job.getCredentials(),
        new Path[] { outDir }, jobConf);

    final FileSystem fs = outDir.getFileSystem(jobConf);

    try {
      // existing output dir is considered empty iff its only content is the
      // partition file.
      //
      final FileStatus[] outDirKids = fs.listStatus(outDir);
      boolean empty = false;
      if (outDirKids != null && outDirKids.length == 1) {
        final FileStatus st = outDirKids[0];
        final String fname = st.getPath().getName();
        empty =
          !st.isDirectory() && TeraInputFormat.PARTITION_FILENAME.equals(fname);
      }
      if (TeraSort.getUseSimplePartitioner(job) || !empty) {
        throw new FileAlreadyExistsException("Output directory " + outDir
            + " already exists");
      }
    } catch (FileNotFoundException ignored) {
    }
  }

  public RecordWriter<Text,Text> getRecordWriter(TaskAttemptContext job
                                                 ) throws IOException {
    Path file = getDefaultWorkFile(job, "");
    FileSystem fs = file.getFileSystem(job.getConfiguration());
     FSDataOutputStream fileOut = fs.create(file);
    return new TeraRecordWriter(fileOut, job);
  }
  
}
