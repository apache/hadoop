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
package org.apache.hadoop.mapred.nativetask.combinertest;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.nativetask.combinertest.WordCount.IntSumReducer;
import org.apache.hadoop.mapred.nativetask.combinertest.WordCount.TokenizerMapper;
import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Test;

public class CombinerTest {
  private FileSystem fs;
  private String inputpath;
  private String nativeoutputpath;
  private String hadoopoutputpath;

  @Test
  public void testWordCountCombiner() {
    try {

      final Configuration nativeConf = ScenarioConfiguration.getNativeConfiguration();
      nativeConf.addResource(TestConstants.COMBINER_CONF_PATH);
      final Job nativejob = getJob("nativewordcount", nativeConf, inputpath, nativeoutputpath);

      final Configuration commonConf = ScenarioConfiguration.getNormalConfiguration();
      commonConf.addResource(TestConstants.COMBINER_CONF_PATH);

      final Job normaljob = getJob("normalwordcount", commonConf, inputpath, hadoopoutputpath);

      nativejob.waitForCompletion(true);
            
      Counter nativeReduceGroups = nativejob.getCounters().findCounter(Task.Counter.REDUCE_INPUT_RECORDS);
      
      normaljob.waitForCompletion(true);
      Counter normalReduceGroups = normaljob.getCounters().findCounter(Task.Counter.REDUCE_INPUT_RECORDS);
       
      assertEquals(true, ResultVerifier.verify(nativeoutputpath, hadoopoutputpath));
      assertEquals("Native Reduce reduce group counter should equal orignal reduce group counter", 
          nativeReduceGroups.getValue(), normalReduceGroups.getValue());
      
    } catch (final Exception e) {
      e.printStackTrace();
      assertEquals("run exception", true, false);
    }
  }

  @Before
  public void startUp() throws Exception {
    final ScenarioConfiguration conf = new ScenarioConfiguration();
    conf.addcombinerConf();

    this.fs = FileSystem.get(conf);

    this.inputpath = conf.get(TestConstants.NATIVETASK_TEST_COMBINER_INPUTPATH_KEY,
        TestConstants.NATIVETASK_TEST_COMBINER_INPUTPATH_DEFAULTV) + "/wordcount";

    if (!fs.exists(new Path(inputpath))) {
      new TestInputFile(
          conf.getInt(TestConstants.NATIVETASK_COMBINER_WORDCOUNT_FILESIZE, 1000000),
          Text.class.getName(),
          Text.class.getName(), conf).createSequenceTestFile(inputpath, 1, (byte)('a'));
    }

    this.nativeoutputpath = conf.get(TestConstants.NATIVETASK_TEST_COMBINER_OUTPUTPATH,
        TestConstants.NATIVETASK_TEST_COMBINER_OUTPUTPATH_DEFAULTV) + "/nativewordcount";
    this.hadoopoutputpath = conf.get(TestConstants.NORMAL_TEST_COMBINER_OUTPUTPATH,
        TestConstants.NORMAL_TEST_COMBINER_OUTPUTPATH_DEFAULTV) + "/normalwordcount";
  }

  protected static Job getJob(String jobname, Configuration inputConf, String inputpath, String outputpath)
      throws Exception {
    final Configuration conf = new Configuration(inputConf);
    conf.set("fileoutputpath", outputpath);
    final FileSystem fs = FileSystem.get(conf);
    if (fs.exists(new Path(outputpath))) {
      fs.delete(new Path(outputpath));
    }
    fs.close();
    final Job job = new Job(conf, jobname);
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(inputpath));
    FileOutputFormat.setOutputPath(job, new Path(outputpath));
    return job;
  }
}
