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
package org.apache.hadoop.mapred.nativetask.kvtest;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.junit.Test;

public class LargeKVTest {

  @Test
  public void testKeySize() {
    runKVSizeTests(Text.class, IntWritable.class);
  }

  @Test
  public void testValueSize() {
    runKVSizeTests(IntWritable.class, Text.class);
  }

  private static Configuration nativeConf = ScenarioConfiguration.getNativeConfiguration();
  private static Configuration normalConf = ScenarioConfiguration.getNormalConfiguration();
  static {
    nativeConf.addResource(TestConstants.KVTEST_CONF_PATH);
    nativeConf.set(TestConstants.NATIVETASK_KVTEST_CREATEFILE, "true");
    normalConf.addResource(TestConstants.KVTEST_CONF_PATH);
    normalConf.set(TestConstants.NATIVETASK_KVTEST_CREATEFILE, "false");
  }

  public void runKVSizeTests(Class<?> keyClass, Class<?> valueClass) {
    if (!keyClass.equals(Text.class) && !valueClass.equals(Text.class)) {
      return;
    }
    final int deafult_KVSize_Maximum = 1 << 22; // 4M
    final int KVSize_Maximu = normalConf.getInt(TestConstants.NATIVETASK_KVSIZE_MAX_LARGEKV_TEST,
        deafult_KVSize_Maximum);
    try {

      for (int i = 65536; i <= KVSize_Maximu; i *= 4) {
        int min = i / 4;
        int max = i;
        nativeConf.set(TestConstants.NATIVETASK_KVSIZE_MIN, String.valueOf(min));
        nativeConf.set(TestConstants.NATIVETASK_KVSIZE_MAX, String.valueOf(max));
        normalConf.set(TestConstants.NATIVETASK_KVSIZE_MIN, String.valueOf(min));
        normalConf.set(TestConstants.NATIVETASK_KVSIZE_MAX, String.valueOf(max));

        System.out.println("===KV Size Test: min size: " + min + ", max size: " + max + ", keyClass: "
            + keyClass.getName() + ", valueClass: " + valueClass.getName());

        final String nativeOutPut = runNativeLargeKVTest("Test Large Value Size:" + String.valueOf(i), keyClass,
            valueClass, nativeConf);
        final String normalOutPut = this.runNormalLargeKVTest("Test Large Key Size:" + String.valueOf(i), keyClass,
            valueClass, normalConf);
        final boolean compareRet = ResultVerifier.verify(normalOutPut, nativeOutPut);
        final String reason = "keytype: " + keyClass.getName() + ", valuetype: " + valueClass.getName()
            + ", failed with " + (keyClass.equals(Text.class) ? "key" : "value") + ", min size: " + min
            + ", max size: " + max + ", normal out: " + normalOutPut + ", native Out: " + nativeOutPut;
        assertEquals(reason, true, compareRet);
      }
    } catch (final Exception e) {
      // TODO: handle exception
      // assertEquals("test run exception:", null, e);
      e.printStackTrace();
    }
  }

  private String runNativeLargeKVTest(String jobname, Class<?> keyclass, Class<?> valueclass, Configuration conf)
      throws Exception {
    final String inputpath = conf.get(TestConstants.NATIVETASK_KVTEST_INPUTDIR) + "/LargeKV/" + keyclass.getName()
        + "/" + valueclass.getName();
    final String outputpath = conf.get(TestConstants.NATIVETASK_KVTEST_OUTPUTDIR) + "/LargeKV/" + keyclass.getName()
        + "/" + valueclass.getName();
    // if output file exists ,then delete it
    final FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(outputpath), true);
    fs.close();
    try {
      final KVJob keyJob = new KVJob(jobname, conf, keyclass, valueclass, inputpath, outputpath);
      keyJob.runJob();
    } catch (final Exception e) {
      return "normal testcase run time error.";
    }
    return outputpath;
  }

  private String runNormalLargeKVTest(String jobname, Class<?> keyclass, Class<?> valueclass, Configuration conf)
      throws IOException {
    final String inputpath = conf.get(TestConstants.NATIVETASK_KVTEST_INPUTDIR) + "/LargeKV/" + keyclass.getName()
        + "/" + valueclass.getName();
    final String outputpath = conf.get(TestConstants.NATIVETASK_KVTEST_NORMAL_OUTPUTDIR) + "/LargeKV/"
        + keyclass.getName() + "/" + valueclass.getName();
    // if output file exists ,then delete it
    final FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(outputpath), true);
    fs.close();
    try {
      final KVJob keyJob = new KVJob(jobname, conf, keyclass, valueclass, inputpath, outputpath);
      keyJob.runJob();
    } catch (final Exception e) {
      return "normal testcase run time error.";
    }
    return outputpath;
  }
}
