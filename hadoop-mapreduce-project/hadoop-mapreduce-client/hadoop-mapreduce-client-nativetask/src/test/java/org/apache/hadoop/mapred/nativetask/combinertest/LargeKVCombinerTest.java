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
import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LargeKVCombinerTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(LargeKVCombinerTest.class);

  @BeforeEach
  public void startUp() throws Exception {
    Assumptions.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    Assumptions.assumeTrue(NativeRuntime.isNativeLibraryLoaded());
  }

  @Test
  void testLargeValueCombiner() throws Exception {
    final Configuration normalConf = ScenarioConfiguration.getNormalConfiguration();
    final Configuration nativeConf = ScenarioConfiguration.getNativeConfiguration();
    normalConf.addResource(TestConstants.COMBINER_CONF_PATH);
    nativeConf.addResource(TestConstants.COMBINER_CONF_PATH);
    final int deafult_KVSize_Maximum = 1 << 22; // 4M
    final int KVSize_Maximum = normalConf.getInt(TestConstants.NATIVETASK_KVSIZE_MAX_LARGEKV_TEST,
        deafult_KVSize_Maximum);
    final String inputPath = TestConstants.NATIVETASK_COMBINER_TEST_INPUTDIR + "/largeKV";
    final String nativeOutputPath = TestConstants.NATIVETASK_COMBINER_TEST_NATIVE_OUTPUTDIR
        + "/nativeLargeKV";
    final String hadoopOutputPath = TestConstants.NATIVETASK_COMBINER_TEST_NORMAL_OUTPUTDIR
        + "/normalLargeKV";
    final FileSystem fs = FileSystem.get(normalConf);
    for (int i = 65536; i <= KVSize_Maximum; i *= 4) {

      int max = i;
      int min = Math.max(i / 4, max - 10);

      LOG.info("===KV Size Test: min size: " + min + ", max size: " + max);

      normalConf.set(TestConstants.NATIVETASK_KVSIZE_MIN, String.valueOf(min));
      normalConf.set(TestConstants.NATIVETASK_KVSIZE_MAX, String.valueOf(max));
      nativeConf.set(TestConstants.NATIVETASK_KVSIZE_MIN, String.valueOf(min));
      nativeConf.set(TestConstants.NATIVETASK_KVSIZE_MAX, String.valueOf(max));
      fs.delete(new Path(inputPath), true);
      new TestInputFile(normalConf.getInt(TestConstants.NATIVETASK_COMBINER_WORDCOUNT_FILESIZE,
              1000000), IntWritable.class.getName(),
          Text.class.getName(), normalConf).createSequenceTestFile(inputPath, 1);

      final Job normaljob = CombinerTest.getJob("normalwordcount", normalConf,
          inputPath, hadoopOutputPath);
      final Job nativejob = CombinerTest.getJob("nativewordcount", nativeConf,
          inputPath, nativeOutputPath);

      assertThat(nativejob.waitForCompletion(true)).isTrue();

      assertThat(normaljob.waitForCompletion(true)).isTrue();

      final boolean compareRet = ResultVerifier.verify(nativeOutputPath, hadoopOutputPath);

      final String reason = "LargeKVCombinerTest failed with, min size: " + min +
          ", max size: " + max + ", normal out: " + hadoopOutputPath +
          ", native Out: " + nativeOutputPath;

      assertThat(compareRet).withFailMessage(reason).isTrue();
      ResultVerifier.verifyCounters(normaljob, nativejob, true);
    }
    fs.close();
  }

  @AfterAll
  public static void cleanUp() throws IOException {
    final FileSystem fs = FileSystem.get(new ScenarioConfiguration());
    fs.delete(new Path(TestConstants.NATIVETASK_COMBINER_TEST_DIR), true);
    fs.close();
  }
  
}
