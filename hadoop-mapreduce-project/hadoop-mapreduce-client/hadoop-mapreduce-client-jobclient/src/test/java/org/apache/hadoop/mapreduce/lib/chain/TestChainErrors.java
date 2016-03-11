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

import java.io.IOException;

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
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Tests error conditions in ChainMapper/ChainReducer.
 */
public class TestChainErrors extends HadoopTestCase {

  private static String localPathRoot = System.getProperty("test.build.data",
      "/tmp");

  public TestChainErrors() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  private Path inDir = new Path(localPathRoot, "testing/chain/input");
  private Path outDir = new Path(localPathRoot, "testing/chain/output");
  private String input = "a\nb\nc\nd\n";

  /**
   * Tests errors during submission.
   * 
   * @throws Exception
   */
  @Test
  public void testChainSubmission() throws Exception {

    Configuration conf = createJobConf();

    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 0, 0, input);
    job.setJobName("chain");

    Throwable th = null;
    // output key,value classes of first map are not same as that of second map
    try {
      ChainMapper.addMapper(job, Mapper.class, LongWritable.class, Text.class,
          IntWritable.class, Text.class, null);
      ChainMapper.addMapper(job, Mapper.class, LongWritable.class, Text.class,
          LongWritable.class, Text.class, null);
    } catch (IllegalArgumentException iae) {
      th = iae;
    }
    assertTrue(th != null);

    th = null;
    // output key,value classes of reducer are not
    // same as that of mapper in the chain
    try {
      ChainReducer.setReducer(job, Reducer.class, LongWritable.class,
          Text.class, IntWritable.class, Text.class, null);
      ChainMapper.addMapper(job, Mapper.class, LongWritable.class, Text.class,
          LongWritable.class, Text.class, null);
    } catch (IllegalArgumentException iae) {
      th = iae;
    }
    assertTrue(th != null);
  }

  /**
   * Tests one of the mappers throwing exception.
   * 
   * @throws Exception
   */
  @Test
  public void testChainFail() throws Exception {

    Configuration conf = createJobConf();

    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 1, 0, input);
    job.setJobName("chain");

    ChainMapper.addMapper(job, Mapper.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, null);

    ChainMapper.addMapper(job, FailMap.class, LongWritable.class, Text.class,
        IntWritable.class, Text.class, null);

    ChainMapper.addMapper(job, Mapper.class, IntWritable.class, Text.class,
        LongWritable.class, Text.class, null);

    job.waitForCompletion(true);
    assertTrue("Job Not failed", !job.isSuccessful());
  }

  /**
   * Tests Reducer throwing exception.
   * 
   * @throws Exception
   */
  @Test
  public void testReducerFail() throws Exception {

    Configuration conf = createJobConf();

    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 1, 1, input);
    job.setJobName("chain");

    ChainMapper.addMapper(job, Mapper.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, null);

    ChainReducer.setReducer(job, FailReduce.class, LongWritable.class,
        Text.class, LongWritable.class, Text.class, null);

    ChainReducer.addMapper(job, Mapper.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, null);

    job.waitForCompletion(true);
    assertTrue("Job Not failed", !job.isSuccessful());
  }

  /**
   * Tests one of the maps consuming output.
   * 
   * @throws Exception
   */
  @Test
  public void testChainMapNoOuptut() throws Exception {
    Configuration conf = createJobConf();
    String expectedOutput = "";

    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 1, 0, input);
    job.setJobName("chain");

    ChainMapper.addMapper(job, ConsumeMap.class, IntWritable.class, Text.class,
        LongWritable.class, Text.class, null);

    ChainMapper.addMapper(job, Mapper.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, null);

    job.waitForCompletion(true);
    assertTrue("Job failed", job.isSuccessful());
    assertEquals("Outputs doesn't match", expectedOutput, MapReduceTestUtil
        .readOutput(outDir, conf));
  }

  /**
   * Tests reducer consuming output.
   * 
   * @throws Exception
   */
  @Test
  public void testChainReduceNoOuptut() throws Exception {
    Configuration conf = createJobConf();
    String expectedOutput = "";

    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 1, 1, input);
    job.setJobName("chain");

    ChainMapper.addMapper(job, Mapper.class, IntWritable.class, Text.class,
        LongWritable.class, Text.class, null);

    ChainReducer.setReducer(job, ConsumeReduce.class, LongWritable.class,
        Text.class, LongWritable.class, Text.class, null);

    ChainReducer.addMapper(job, Mapper.class, LongWritable.class, Text.class,
        LongWritable.class, Text.class, null);

    job.waitForCompletion(true);
    assertTrue("Job failed", job.isSuccessful());
    assertEquals("Outputs doesn't match", expectedOutput, MapReduceTestUtil
        .readOutput(outDir, conf));
  }

  // this map consumes all the input and output nothing
  public static class ConsumeMap extends
      Mapper<LongWritable, Text, LongWritable, Text> {
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
    }
  }

  // this reduce consumes all the input and output nothing
  public static class ConsumeReduce extends
      Reducer<LongWritable, Text, LongWritable, Text> {
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
    }
  }

  // this map throws IOException for input value "b"
  public static class FailMap extends
      Mapper<LongWritable, Text, IntWritable, Text> {
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      if (value.toString().equals("b")) {
        throw new IOException();
      }
    }
  }

  // this reduce throws IOEexception for any input
  public static class FailReduce extends
      Reducer<LongWritable, Text, LongWritable, Text> {
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      throw new IOException();
    }
  }
}
