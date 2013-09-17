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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.mapreduce.task.annotation.Checkpointable;
import org.junit.Test;
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;

public class TestPreemptableFileOutputCommitter {

  @Test
  public void testPartialOutputCleanup()
      throws FileNotFoundException, IllegalArgumentException, IOException {

    Configuration conf = new Configuration(false);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
    TaskAttemptID tid0 =
      new TaskAttemptID("1363718006656", 1, TaskType.REDUCE, 14, 3);

    Path p = spy(new Path("/user/hadoop/out"));
    Path a = new Path("hdfs://user/hadoop/out");
    Path p0 = new Path(a, "_temporary/1/attempt_1363718006656_0001_r_000014_0");
    Path p1 = new Path(a, "_temporary/1/attempt_1363718006656_0001_r_000014_1");
    Path p2 = new Path(a, "_temporary/1/attempt_1363718006656_0001_r_000013_0");
    // (p3 does not exist)
    Path p3 = new Path(a, "_temporary/1/attempt_1363718006656_0001_r_000014_2");

    FileStatus[] fsa = new FileStatus[3];
    fsa[0] = new FileStatus();
    fsa[0].setPath(p0);
    fsa[1] = new FileStatus();
    fsa[1].setPath(p1);
    fsa[2] = new FileStatus();
    fsa[2].setPath(p2);

    final FileSystem fs = mock(FileSystem.class);
    when(fs.exists(eq(p0))).thenReturn(true);
    when(fs.exists(eq(p1))).thenReturn(true);
    when(fs.exists(eq(p2))).thenReturn(true);
    when(fs.exists(eq(p3))).thenReturn(false);
    when(fs.delete(eq(p0), eq(true))).thenReturn(true);
    when(fs.delete(eq(p1), eq(true))).thenReturn(true);
    doReturn(fs).when(p).getFileSystem(any(Configuration.class));
    when(fs.makeQualified(eq(p))).thenReturn(a);

    TaskAttemptContext context = mock(TaskAttemptContext.class);
    when(context.getTaskAttemptID()).thenReturn(tid0);
    when(context.getConfiguration()).thenReturn(conf);

    PartialFileOutputCommitter foc = new TestPFOC(p, context, fs);

    foc.cleanUpPartialOutputForTask(context);
    verify(fs).delete(eq(p0), eq(true));
    verify(fs).delete(eq(p1), eq(true));
    verify(fs, never()).delete(eq(p3), eq(true));
    verify(fs, never()).delete(eq(p2), eq(true));
  }

  @Checkpointable
  static class TestPFOC extends PartialFileOutputCommitter {
    final FileSystem fs;
    TestPFOC(Path outputPath, TaskAttemptContext ctxt, FileSystem fs)
        throws IOException {
      super(outputPath, ctxt);
      this.fs = fs;
    }
    @Override
    FileSystem fsFor(Path p, Configuration conf) {
      return fs;
    }
  }

}
