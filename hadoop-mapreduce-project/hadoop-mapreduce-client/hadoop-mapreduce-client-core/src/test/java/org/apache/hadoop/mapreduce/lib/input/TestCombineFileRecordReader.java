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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.FileWriter;
import java.io.IOException;
import java.io.File;

import org.junit.Assert;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapred.Task.TaskReporter;

import org.mockito.Mockito;

import org.junit.Test;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestCombineFileRecordReader {

  private static Path outDir = new Path(System.getProperty("test.build.data",
            "/tmp"), TestCombineFileRecordReader.class.getName());
  private static class TextRecordReaderWrapper
    extends CombineFileRecordReaderWrapper<LongWritable,Text> {
    // this constructor signature is required by CombineFileRecordReader
    public TextRecordReaderWrapper(org.apache.hadoop.mapreduce.lib.input.CombineFileSplit split,
      TaskAttemptContext context, Integer idx)
        throws IOException, InterruptedException {
      super(new TextInputFormat(), split, context, idx);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testProgressIsReportedIfInputASeriesOfEmptyFiles() throws IOException, InterruptedException {
    JobConf conf = new JobConf();
    Path[] paths = new Path[3];
    File[] files = new File[3];
    long[] fileLength = new long[3];

    try {
      for(int i=0;i<3;i++){
        File dir = new File(outDir.toString());
        dir.mkdir();
        files[i] = new File(dir,"testfile"+i);
        FileWriter fileWriter = new FileWriter(files[i]);
        fileWriter.flush();
        fileWriter.close();
        fileLength[i] = i;
        paths[i] = new Path(outDir+"/testfile"+i);
      }

      CombineFileSplit combineFileSplit = new CombineFileSplit(paths, fileLength);
      TaskAttemptID taskAttemptID = Mockito.mock(TaskAttemptID.class);
      TaskReporter reporter = Mockito.mock(TaskReporter.class);
      TaskAttemptContextImpl taskAttemptContext =
        new TaskAttemptContextImpl(conf, taskAttemptID,reporter);

      CombineFileRecordReader cfrr = new CombineFileRecordReader(combineFileSplit,
        taskAttemptContext, TextRecordReaderWrapper.class);

      cfrr.initialize(combineFileSplit,taskAttemptContext);

      verify(reporter).progress();
      Assert.assertFalse(cfrr.nextKeyValue());
      verify(reporter, times(3)).progress();
    } finally {
      FileUtil.fullyDelete(new File(outDir.toString()));
    }
  }
}

