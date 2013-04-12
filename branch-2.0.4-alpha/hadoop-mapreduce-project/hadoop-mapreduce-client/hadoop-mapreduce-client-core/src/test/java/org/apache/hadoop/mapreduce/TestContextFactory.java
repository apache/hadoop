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
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.junit.Before;
import org.junit.Test;

public class TestContextFactory {

  JobID jobId;
  Configuration conf;
  JobContext jobContext;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    jobId = new JobID("test", 1);
    jobContext = new JobContextImpl(conf, jobId);
  }
  
  @Test
  public void testCloneContext() throws Exception {
    ContextFactory.cloneContext(jobContext, conf);
  }

  @Test
  public void testCloneMapContext() throws Exception {
    TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
    TaskAttemptID taskAttemptid = new TaskAttemptID(taskId, 0);
    MapContext<IntWritable, IntWritable, IntWritable, IntWritable> mapContext =
    new MapContextImpl<IntWritable, IntWritable, IntWritable, IntWritable>(
        conf, taskAttemptid, null, null, null, null, null);
    Mapper<IntWritable, IntWritable, IntWritable, IntWritable>.Context mapperContext = 
      new WrappedMapper<IntWritable, IntWritable, IntWritable, IntWritable>().getMapContext(
          mapContext);
    ContextFactory.cloneMapContext(mapperContext, conf, null, null);
  }

  @Before
  public void tearDown() throws Exception {
    
  }
}
