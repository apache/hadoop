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
import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class LargeKVCombinerTest {

  @Test
  public void testLargeValueCombiner(){
    final Configuration normalConf = ScenarioConfiguration.getNormalConfiguration();
    final Configuration nativeConf = ScenarioConfiguration.getNativeConfiguration();
    normalConf.addResource(TestConstants.COMBINER_CONF_PATH);
    nativeConf.addResource(TestConstants.COMBINER_CONF_PATH);
    final int deafult_KVSize_Maximum = 1 << 22; // 4M
    final int KVSize_Maximu = normalConf.getInt(TestConstants.NATIVETASK_KVSIZE_MAX_LARGEKV_TEST,
        deafult_KVSize_Maximum);
    final String inputPath = normalConf.get(TestConstants.NATIVETASK_TEST_COMBINER_INPUTPATH_KEY,
        TestConstants.NATIVETASK_TEST_COMBINER_INPUTPATH_DEFAULTV) + "/largeKV";
    final String nativeOutputPath = normalConf.get(TestConstants.NATIVETASK_TEST_COMBINER_OUTPUTPATH,
        TestConstants.NATIVETASK_TEST_COMBINER_OUTPUTPATH_DEFAULTV) + "/nativeLargeKV";
    final String hadoopOutputPath = normalConf.get(TestConstants.NORMAL_TEST_COMBINER_OUTPUTPATH,
        TestConstants.NORMAL_TEST_COMBINER_OUTPUTPATH_DEFAULTV) + "/normalLargeKV";
    try {
      final FileSystem fs = FileSystem.get(normalConf);
      for (int i = 65536; i <= KVSize_Maximu; i *= 4) {
        
        int max = i;
        int min = Math.max(i / 4, max - 10);
        
        System.out.println("===KV Size Test: min size: " + min + ", max size: " + max);
        
        normalConf.set(TestConstants.NATIVETASK_KVSIZE_MIN, String.valueOf(min));
        normalConf.set(TestConstants.NATIVETASK_KVSIZE_MAX, String.valueOf(max));
        nativeConf.set(TestConstants.NATIVETASK_KVSIZE_MIN, String.valueOf(min));
        nativeConf.set(TestConstants.NATIVETASK_KVSIZE_MAX, String.valueOf(max));
        fs.delete(new Path(inputPath), true);
        new TestInputFile(normalConf.getInt(TestConstants.NATIVETASK_COMBINER_WORDCOUNT_FILESIZE,
            1000000), IntWritable.class.getName(),
            Text.class.getName(), normalConf).createSequenceTestFile(inputPath, 1);
        
        final Job normaljob = CombinerTest.getJob("normalwordcount", normalConf, inputPath, hadoopOutputPath);
        final Job nativejob = CombinerTest.getJob("nativewordcount", nativeConf, inputPath, nativeOutputPath);
        
        nativejob.waitForCompletion(true);
        Counter nativeReduceGroups = nativejob.getCounters().findCounter(Task.Counter.REDUCE_INPUT_RECORDS);
        
        normaljob.waitForCompletion(true);
        Counter normalReduceGroups = normaljob.getCounters().findCounter(Task.Counter.REDUCE_INPUT_RECORDS);
        
        final boolean compareRet = ResultVerifier.verify(nativeOutputPath, hadoopOutputPath);
        
        final String reason = "LargeKVCombinerTest failed with, min size: " + min
            + ", max size: " + max + ", normal out: " + hadoopOutputPath + ", native Out: " + nativeOutputPath;
        
        assertEquals(reason, true, compareRet);
//        assertEquals("Native Reduce reduce group counter should equal orignal reduce group counter", 
//            nativeReduceGroups.getValue(), normalReduceGroups.getValue());
      }
      fs.close();
    } catch (final Exception e) {
      e.printStackTrace();
      assertEquals("run exception", true, false);
    }
  }
  
}
