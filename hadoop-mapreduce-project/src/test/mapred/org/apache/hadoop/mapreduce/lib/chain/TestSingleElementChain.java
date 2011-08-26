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
package org.apache.hadoop.mapreduce.lib.chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;

/**
 * Runs wordcount by adding single mapper and single reducer to chain
 */
public class TestSingleElementChain extends HadoopTestCase {

  private static String localPathRoot = System.getProperty("test.build.data",
      "/tmp");

  public TestSingleElementChain() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  // test chain mapper and reducer by adding single mapper and reducer to chain
  public void testNoChain() throws Exception {
    Path inDir = new Path(localPathRoot, "testing/chain/input");
    Path outDir = new Path(localPathRoot, "testing/chain/output");
    String input = "a\nb\na\n";
    String expectedOutput = "a\t2\nb\t1\n";

    Configuration conf = createJobConf();

    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 1, 1, input);
    job.setJobName("chain");

    ChainMapper.addMapper(job, TokenCounterMapper.class, Object.class,
        Text.class, Text.class, IntWritable.class, null);

    ChainReducer.setReducer(job, IntSumReducer.class, Text.class,
        IntWritable.class, Text.class, IntWritable.class, null);

    job.waitForCompletion(true);
    assertTrue("Job failed", job.isSuccessful());
    assertEquals("Outputs doesn't match", expectedOutput, MapReduceTestUtil
        .readOutput(outDir, conf));
  }

}
