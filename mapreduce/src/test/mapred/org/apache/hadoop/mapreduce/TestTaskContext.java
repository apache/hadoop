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

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HadoopTestCase;

/**
 * Tests context api. 
 */
public class TestTaskContext extends HadoopTestCase {
  public TestTaskContext() throws IOException {
    super(HadoopTestCase.CLUSTER_MR , HadoopTestCase.LOCAL_FS, 1, 1);
  }

  static String myStatus = "my status";
  static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void setup(Context context) throws IOException {
      context.setStatus(myStatus);
      assertEquals(myStatus, context.getStatus());
    }
  }

  /**
   * Tests context.setStatus method.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public void testContextStatus()
      throws IOException, InterruptedException, ClassNotFoundException {
    int numMaps = 1;
    Job job = MapReduceTestUtil.createJob(createJobConf(), new Path("in"),
        new Path("out"), numMaps, 0);
    job.setMapperClass(MyMapper.class);
    job.waitForCompletion(true);
    assertTrue("Job failed", job.isSuccessful());
    TaskReport[] reports = job.getTaskReports(TaskType.MAP);
    assertEquals(numMaps, reports.length);
    assertEquals(myStatus + " > sort", reports[0].getState());
  }
}
