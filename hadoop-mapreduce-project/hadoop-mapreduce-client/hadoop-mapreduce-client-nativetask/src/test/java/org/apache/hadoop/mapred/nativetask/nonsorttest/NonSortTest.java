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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.AfterClass;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class NonSortTest {

  @Test
  public void nonSortTest() throws Exception {
    Configuration nativeConf = ScenarioConfiguration.getNativeConfiguration();
    nativeConf.addResource(TestConstants.NONSORT_TEST_CONF);
    nativeConf.set(TestConstants.NATIVETASK_MAP_OUTPUT_SORT, "false");
    final Job nativeNonSort = getJob(nativeConf, "NativeNonSort",
      TestConstants.NATIVETASK_NONSORT_TEST_INPUTDIR,
      TestConstants.NATIVETASK_NONSORT_TEST_NATIVE_OUTPUT);
    assertThat(nativeNonSort.waitForCompletion(true)).isTrue();

    Configuration normalConf = ScenarioConfiguration.getNormalConfiguration();
    normalConf.addResource(TestConstants.NONSORT_TEST_CONF);
    final Job hadoopWithSort = getJob(normalConf, "NormalJob",
      TestConstants.NATIVETASK_NONSORT_TEST_INPUTDIR,
      TestConstants.NATIVETASK_NONSORT_TEST_NORMAL_OUTPUT);
    assertThat(hadoopWithSort.waitForCompletion(true)).isTrue();

    final boolean compareRet = ResultVerifier.verify(
      TestConstants.NATIVETASK_NONSORT_TEST_NATIVE_OUTPUT,
      TestConstants.NATIVETASK_NONSORT_TEST_NORMAL_OUTPUT);
    assertThat(compareRet)
        .withFailMessage(
            "file compare result: if they are the same ,then return true")
        .isTrue();
    ResultVerifier.verifyCounters(hadoopWithSort, nativeNonSort);
  }

  @Before
  public void startUp() throws Exception {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    Assume.assumeTrue(NativeRuntime.isNativeLibraryLoaded());
    final ScenarioConfiguration conf = new ScenarioConfiguration();
    conf.addNonSortTestConf();
    final FileSystem fs = FileSystem.get(conf);
    final Path path = new Path(TestConstants.NATIVETASK_NONSORT_TEST_INPUTDIR);
    if (!fs.exists(path)) {
      int filesize = conf.getInt(TestConstants.NATIVETASK_NONSORTTEST_FILESIZE, 10000000);
      new TestInputFile(filesize, Text.class.getName(),
          Text.class.getName(), conf).createSequenceTestFile(path.toString());
    }
    fs.close();
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    final FileSystem fs = FileSystem.get(new ScenarioConfiguration());
    fs.delete(new Path(TestConstants.NATIVETASK_NONSORT_TEST_DIR), true);
    fs.close();
  }


  private Job getJob(Configuration conf, String jobName,
                     String inputpath, String outputpath) throws IOException {
    final FileSystem fs = FileSystem.get(conf);
    if (fs.exists(new Path(outputpath))) {
      fs.delete(new Path(outputpath), true);
    }
    fs.close();
    final Job job = Job.getInstance(conf, jobName);
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
