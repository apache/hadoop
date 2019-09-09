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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.junit.AfterClass;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class OldAPICombinerTest {
  private FileSystem fs;
  private String inputpath;

  @Test
  public void testWordCountCombinerWithOldAPI() throws Exception {
    final Configuration nativeConf = ScenarioConfiguration.getNativeConfiguration();
    nativeConf.addResource(TestConstants.COMBINER_CONF_PATH);
    final String nativeoutput = TestConstants.NATIVETASK_OLDAPI_COMBINER_TEST_NATIVE_OUTPUTPATH;
    final JobConf nativeJob = getOldAPIJobconf(nativeConf, "nativeCombinerWithOldAPI",
                                               inputpath, nativeoutput);
    RunningJob nativeRunning = JobClient.runJob(nativeJob);

    Counter nativeReduceGroups = nativeRunning.getCounters().findCounter(
      TaskCounter.REDUCE_INPUT_RECORDS);

    final Configuration normalConf = ScenarioConfiguration.getNormalConfiguration();
    normalConf.addResource(TestConstants.COMBINER_CONF_PATH);
    final String normaloutput = TestConstants.NATIVETASK_OLDAPI_COMBINER_TEST_NORMAL_OUTPUTPATH;
    final JobConf normalJob = getOldAPIJobconf(normalConf, "normalCombinerWithOldAPI",
                                               inputpath, normaloutput);

    RunningJob normalRunning = JobClient.runJob(normalJob);
    Counter normalReduceGroups = normalRunning.getCounters().findCounter(
      TaskCounter.REDUCE_INPUT_RECORDS);

    final boolean compareRet = ResultVerifier.verify(nativeoutput, normaloutput);
    assertThat(compareRet)
        .withFailMessage(
            "file compare result: if they are the same ,then return true")
        .isTrue();

    assertThat(nativeReduceGroups.getValue())
        .withFailMessage("The input reduce record count must be same")
        .isEqualTo(normalReduceGroups.getValue());
  }

  @Before
  public void startUp() throws Exception {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    Assume.assumeTrue(NativeRuntime.isNativeLibraryLoaded());
    final ScenarioConfiguration conf = new ScenarioConfiguration();
    conf.addcombinerConf();
    this.fs = FileSystem.get(conf);
    this.inputpath = TestConstants.NATIVETASK_COMBINER_TEST_INPUTDIR + "/wordcount";

    if (!fs.exists(new Path(inputpath))) {
      new TestInputFile(conf.getInt(TestConstants.NATIVETASK_COMBINER_WORDCOUNT_FILESIZE, 1000000),
                        Text.class.getName(),
          Text.class.getName(), conf).createSequenceTestFile(inputpath, 1, (byte)('a'));
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    final FileSystem fs = FileSystem.get(new ScenarioConfiguration());
    fs.delete(new Path(TestConstants.NATIVETASK_COMBINER_TEST_DIR), true);
    fs.close();
  }

  private static JobConf getOldAPIJobconf(Configuration configuration, String name,
                                          String input, String output)
      throws Exception {
    final JobConf jobConf = new JobConf(configuration);
    final FileSystem fs = FileSystem.get(configuration);
    if (fs.exists(new Path(output))) {
      fs.delete(new Path(output), true);
    }
    fs.close();
    jobConf.setJobName(name);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(IntWritable.class);
    jobConf.setMapperClass(WordCountWithOldAPI.TokenizerMapperWithOldAPI.class);
    jobConf.setCombinerClass(WordCountWithOldAPI.IntSumReducerWithOldAPI.class);
    jobConf.setReducerClass(WordCountWithOldAPI.IntSumReducerWithOldAPI.class);

    jobConf.setInputFormat(SequenceFileInputFormat.class);
    jobConf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(jobConf, new Path(input));
    FileOutputFormat.setOutputPath(jobConf, new Path(output));
    return jobConf;
  }
}
