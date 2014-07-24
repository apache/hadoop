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
package org.apache.hadoop.mapred.nativetask.nonsorttest;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;

public class NonSortTest {

  @Test
  public void nonSortTest() throws Exception {
    Configuration nativeConf = ScenarioConfiguration.getNativeConfiguration();
    nativeConf.addResource(TestConstants.NONSORT_TEST_CONF);
    nativeConf.set(TestConstants.NATIVETASK_MAP_OUTPUT_SORT, "false");
    String inputpath = nativeConf.get(TestConstants.NONSORT_TEST_INPUTDIR);
    String outputpath = nativeConf.get(TestConstants.NONSORT_TEST_NATIVE_OUTPUT);
    final Job nativeNonSort = getJob(nativeConf, "NativeNonSort", inputpath, outputpath);
    nativeNonSort.waitForCompletion(true);

    Configuration normalConf = ScenarioConfiguration.getNormalConfiguration();
    normalConf.addResource(TestConstants.NONSORT_TEST_CONF);
    inputpath = normalConf.get(TestConstants.NONSORT_TEST_INPUTDIR);
    outputpath = normalConf.get(TestConstants.NONSORT_TEST_NORMAL_OUTPUT);
    final Job hadoopWithSort = getJob(normalConf, "NormalJob", inputpath, outputpath);
    hadoopWithSort.waitForCompletion(true);

    final boolean compareRet = ResultVerifier.verify(nativeConf.get(TestConstants.NONSORT_TEST_NATIVE_OUTPUT),
        normalConf.get(TestConstants.NONSORT_TEST_NORMAL_OUTPUT));
    assertEquals("file compare result: if they are the same ,then return true", true, compareRet);
  }

  @Before
  public void startUp() throws Exception {
    final ScenarioConfiguration configuration = new ScenarioConfiguration();
    configuration.addNonSortTestConf();
    final FileSystem fs = FileSystem.get(configuration);
    final Path path = new Path(configuration.get(TestConstants.NONSORT_TEST_INPUTDIR));
    if (!fs.exists(path)) {
      new TestInputFile(configuration.getInt("nativetask.nonsorttest.filesize", 10000000), Text.class.getName(),
          Text.class.getName(), configuration).createSequenceTestFile(path.toString());
    }
    fs.close();
  }

  private Job getJob(Configuration conf, String jobName, String inputpath, String outputpath) throws IOException {
    final FileSystem fs = FileSystem.get(conf);
    if (fs.exists(new Path(outputpath))) {
      fs.delete(new Path(outputpath), true);
    }
    fs.close();
    final Job job = new Job(conf, jobName);
    job.setJarByClass(NonSortTestMR.class);
    job.setMapperClass(NonSortTestMR.Map.class);
    job.setReducerClass(NonSortTestMR.KeyHashSumReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputValueClass(LongWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(inputpath));
    FileOutputFormat.setOutputPath(job, new Path(outputpath));
    return job;
  }

}
