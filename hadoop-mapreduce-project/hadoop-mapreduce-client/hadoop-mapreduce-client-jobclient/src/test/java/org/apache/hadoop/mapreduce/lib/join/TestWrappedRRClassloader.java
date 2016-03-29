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
package org.apache.hadoop.mapreduce.lib.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.MapReduceTestUtil.Fake_RR;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestWrappedRRClassloader {
  /**
   * Tests the class loader set by 
   * {@link Configuration#setClassLoader(ClassLoader)}
   * is inherited by any {@link WrappedRecordReader}s created by
   * {@link CompositeRecordReader}
   */
  @Test
  public void testClassLoader() throws Exception {
    Configuration conf = new Configuration();
    Fake_ClassLoader classLoader = new Fake_ClassLoader();
    conf.setClassLoader(classLoader);
    assertTrue(conf.getClassLoader() instanceof Fake_ClassLoader);

    FileSystem fs = FileSystem.get(conf);
    Path testdir = new Path(System.getProperty("test.build.data", "/tmp"))
        .makeQualified(fs);

    Path base = new Path(testdir, "/empty");
    Path[] src = { new Path(base, "i0"), new Path("i1"), new Path("i2") };
    conf.set(CompositeInputFormat.JOIN_EXPR, 
      CompositeInputFormat.compose("outer", IF_ClassLoaderChecker.class, src));

    CompositeInputFormat<NullWritable> inputFormat = 
      new CompositeInputFormat<NullWritable>();
    // create dummy TaskAttemptID
    TaskAttemptID tid = new TaskAttemptID("jt", 1, TaskType.MAP, 0, 0);
    conf.set(MRJobConfig.TASK_ATTEMPT_ID, tid.toString());
    inputFormat.createRecordReader
      (inputFormat.getSplits(Job.getInstance(conf)).get(0), 
       new TaskAttemptContextImpl(conf, tid));
  }

  public static class Fake_ClassLoader extends ClassLoader {
  }

  public static class IF_ClassLoaderChecker<K, V> 
      extends MapReduceTestUtil.Fake_IF<K, V> {

    public IF_ClassLoaderChecker() {
    }

    public RecordReader<K, V> createRecordReader(InputSplit ignored, 
        TaskAttemptContext context) {
      return new RR_ClassLoaderChecker<K, V>(context.getConfiguration());
    }
  }

  public static class RR_ClassLoaderChecker<K, V> extends Fake_RR<K, V> {

    @SuppressWarnings("unchecked")
    public RR_ClassLoaderChecker(Configuration conf) {
      assertTrue("The class loader has not been inherited from "
          + CompositeRecordReader.class.getSimpleName(),
          conf.getClassLoader() instanceof Fake_ClassLoader);

    }
  }
}
