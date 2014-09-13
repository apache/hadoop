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
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.junit.AfterClass;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class LargeKVCombinerTest {
  private static final Log LOG = LogFactory.getLog(LargeKVCombinerTest.class);

  @Before
  public void startUp() throws Exception {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    Assume.assumeTrue(NativeRuntime.isNativeLibraryLoaded());
  }

  @Test
  public void testLargeValueCombiner() throws Exception {
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

      assertTrue(nativejob.waitForCompletion(true));

      assertTrue(normaljob.waitForCompletion(true));

      final boolean compareRet = ResultVerifier.verify(nativeOutputPath, hadoopOutputPath);

      final String reason = "LargeKVCombinerTest failed with, min size: " + min +
        ", max size: " + max + ", normal out: " + hadoopOutputPath +
        ", native Out: " + nativeOutputPath;

      assertEquals(reason, true, compareRet);
      ResultVerifier.verifyCounters(normaljob, nativejob, true);
    }
    fs.close();
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    final FileSystem fs = FileSystem.get(new ScenarioConfiguration());
    fs.delete(new Path(TestConstants.NATIVETASK_COMBINER_TEST_DIR), true);
    fs.close();
  }
  
}
