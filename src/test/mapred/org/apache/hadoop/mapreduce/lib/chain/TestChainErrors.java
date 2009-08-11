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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TestChainErrors extends HadoopTestCase {

  private static String localPathRoot = 
    System.getProperty("test.build.data", "/tmp");

  public TestChainErrors() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }
  private Path inDir = new Path(localPathRoot, "testing/chain/input");
  private Path outDir = new Path(localPathRoot, "testing/chain/output");
  private String input = "a\nb\nc\nd\n";

  public void testChainSubmission() throws Exception {

    Configuration conf = createJobConf();

    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 1, 1, input);
    job.setJobName("chain");

    Throwable th = null;
    try {
      ChainMapper.addMapper(job, CMap.class, LongWritable.class, Text.class,
        IntWritable.class, Text.class, null);
      ChainMapper.addMapper(job, BMap.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, null);
    } catch (IllegalArgumentException iae) {
      th = iae;
    }
    assertTrue(th != null);

    th = null;
    try {
      ChainReducer.setReducer(job, CReduce.class, LongWritable.class,
        Text.class, IntWritable.class, Text.class, null);
      ChainMapper.addMapper(job, AMap.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, null);
    } catch (IllegalArgumentException iae) {
      th = iae;
    }
    assertTrue(th != null);
  }
  
  public void testChainFail() throws Exception {

    Configuration conf = createJobConf();

    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 1, 0, input);
    job.setJobName("chain");

    ChainMapper.addMapper(job, BMap.class, LongWritable.class, Text.class,
      LongWritable.class, Text.class, null);
   
    ChainMapper.addMapper(job, CMap.class, LongWritable.class, Text.class,
      IntWritable.class, Text.class, null);
    
    job.waitForCompletion(true);
    assertTrue("Job Not failed", !job.isSuccessful());
  }

  public void testChainNoOuptut() throws Exception {
    Configuration conf = createJobConf();
    String expectedOutput = "";

    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 1, 0, input);
    job.setJobName("chain");

    ChainMapper.addMapper(job, AMap.class, IntWritable.class, Text.class,
      LongWritable.class, Text.class, null);

    ChainMapper.addMapper(job, BMap.class, LongWritable.class, Text.class,
      LongWritable.class, Text.class, null);
   
    job.waitForCompletion(true);
    assertTrue("Job failed", job.isSuccessful());
    assertEquals("Outputs doesn't match", expectedOutput,
      MapReduceTestUtil.readOutput(outDir, conf));
  }

 
  // this map consumes all the input and output nothing
  public static class AMap 
      extends Mapper<LongWritable, Text, LongWritable, Text> {
    public void map(LongWritable key, Text value, 
        Context context) throws IOException, InterruptedException {
    }
  }

  public static class BMap 
      extends Mapper<LongWritable, Text, LongWritable, Text> {
  }

  // this map throws Exception for input value "b" 
  public static class CMap 
      extends Mapper<LongWritable, Text, IntWritable, Text> {
    protected void map(LongWritable key, Text value, 
        Context context) throws IOException, InterruptedException {
      if (value.toString().equals("b")) {
        throw new IOException();
      }
    }
  }
  
  public static class CReduce 
      extends Reducer<LongWritable, Text, IntWritable, Text> {
  }

}
