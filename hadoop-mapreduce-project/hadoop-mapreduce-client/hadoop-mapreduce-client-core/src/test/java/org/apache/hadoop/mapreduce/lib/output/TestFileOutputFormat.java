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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TestFileOutputFormat {

  @Test
  public void testSetOutputPathException() throws Exception {
    Job job = Job.getInstance();
    try {
      // Give it an invalid filesystem so it'll throw an exception
      FileOutputFormat.setOutputPath(job, new Path("foo:///bar"));
      fail("Should have thrown a RuntimeException with an IOException inside");
    }
    catch (RuntimeException re) {
      assertTrue(re.getCause() instanceof IOException);
    }
  }

  @Test
  public void testCheckOutputSpecsException() throws Exception {
    Job job = Job.getInstance();
    Path outDir = new Path(System.getProperty("test.build.data", "/tmp"),
            "output");
    FileSystem fs = outDir.getFileSystem(new Configuration());
    // Create the output dir so it already exists and set it for the job
    fs.mkdirs(outDir);
    FileOutputFormat.setOutputPath(job, outDir);
    // We don't need a "full" implementation of FileOutputFormat for this test
    FileOutputFormat fof = new FileOutputFormat() {
      @Override
        public RecordWriter getRecordWriter(TaskAttemptContext job)
              throws IOException, InterruptedException {
          return null;
        }
    };
    try {
      try {
        // This should throw a FileAlreadyExistsException because the outputDir
        // already exists
        fof.checkOutputSpecs(job);
        fail("Should have thrown a FileAlreadyExistsException");
      }
      catch (FileAlreadyExistsException re) {
        // correct behavior
      }
    }
    finally {
      // Cleanup
      if (fs.exists(outDir)) {
        fs.delete(outDir, true);
      }
    }
  }
}
