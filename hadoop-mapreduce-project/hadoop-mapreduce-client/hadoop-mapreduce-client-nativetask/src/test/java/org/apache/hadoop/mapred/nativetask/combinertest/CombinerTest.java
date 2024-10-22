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
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.combinertest.WordCount.IntSumReducer;
import org.apache.hadoop.mapred.nativetask.combinertest.WordCount.TokenizerMapper;
import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class CombinerTest {
  private FileSystem fs;
  private String inputpath;
  private String nativeoutputpath;
  private String hadoopoutputpath;

  @Test
  void testWordCountCombiner() throws Exception {
    final Configuration nativeConf = ScenarioConfiguration.getNativeConfiguration();
    nativeConf.addResource(TestConstants.COMBINER_CONF_PATH);
    final Job nativejob = getJob("nativewordcount", nativeConf, inputpath, nativeoutputpath);

    final Configuration commonConf = ScenarioConfiguration.getNormalConfiguration();
    commonConf.addResource(TestConstants.COMBINER_CONF_PATH);
    final Job normaljob = getJob("normalwordcount", commonConf, inputpath, hadoopoutputpath);

    assertThat(nativejob.waitForCompletion(true)).isTrue();
    assertThat(normaljob.waitForCompletion(true)).isTrue();
    assertThat(ResultVerifier.verify(nativeoutputpath, hadoopoutputpath))
        .isTrue();
    ResultVerifier.verifyCounters(normaljob, nativejob, true);
  }

  @BeforeEach
  public void startUp() throws Exception {
    Assumptions.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    Assumptions.assumeTrue(NativeRuntime.isNativeLibraryLoaded());
    final ScenarioConfiguration conf = new ScenarioConfiguration();
    conf.addcombinerConf();

    this.fs = FileSystem.get(conf);

    this.inputpath = TestConstants.NATIVETASK_COMBINER_TEST_INPUTDIR + "/wordcount";

    if (!fs.exists(new Path(inputpath))) {
      new TestInputFile(
          conf.getInt(TestConstants.NATIVETASK_COMBINER_WORDCOUNT_FILESIZE, 1000000),
          Text.class.getName(),
          Text.class.getName(), conf).createSequenceTestFile(inputpath, 1, (byte)('a'));
    }

    this.nativeoutputpath = TestConstants.NATIVETASK_COMBINER_TEST_NATIVE_OUTPUTDIR +
      "/nativewordcount";
    this.hadoopoutputpath = TestConstants.NATIVETASK_COMBINER_TEST_NORMAL_OUTPUTDIR +
      "/normalwordcount";
  }

  @AfterAll
  public static void cleanUp() throws IOException {
    final FileSystem fs = FileSystem.get(new ScenarioConfiguration());
    fs.delete(new Path(TestConstants.NATIVETASK_COMBINER_TEST_DIR), true);
    fs.close();
  }

  protected static Job getJob(String jobname, Configuration inputConf,
                              String inputpath, String outputpath)
      throws Exception {
    final Configuration conf = new Configuration(inputConf);
    conf.set("fileoutputpath", outputpath);
    final FileSystem fs = FileSystem.get(conf);
    if (fs.exists(new Path(outputpath))) {
      fs.delete(new Path(outputpath), true);
    }
    fs.close();
    final Job job = Job.getInstance(conf, jobname);
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
