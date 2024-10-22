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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LargeKVTest {
  private static final Logger LOG = LoggerFactory.getLogger(LargeKVTest.class);

  @BeforeEach
  public void startUp() throws Exception {
    Assumptions.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    Assumptions.assumeTrue(NativeRuntime.isNativeLibraryLoaded());
  }

  private static Configuration nativeConf = ScenarioConfiguration.getNativeConfiguration();
  private static Configuration normalConf = ScenarioConfiguration.getNormalConfiguration();
  static {
    nativeConf.addResource(TestConstants.KVTEST_CONF_PATH);
    nativeConf.set(TestConstants.NATIVETASK_KVTEST_CREATEFILE, "true");
    normalConf.addResource(TestConstants.KVTEST_CONF_PATH);
    normalConf.set(TestConstants.NATIVETASK_KVTEST_CREATEFILE, "false");
  }

  @Test
  void testKeySize() throws Exception {
    runKVSizeTests(Text.class, IntWritable.class);
  }

  @Test
  void testValueSize() throws Exception {
    runKVSizeTests(IntWritable.class, Text.class);
  }

  @AfterAll
  public static void cleanUp() throws IOException {
    final FileSystem fs = FileSystem.get(new ScenarioConfiguration());
    fs.delete(new Path(TestConstants.NATIVETASK_KVTEST_DIR), true);
    fs.close();
  }

  public void runKVSizeTests(Class<?> keyClass, Class<?> valueClass) throws Exception {
    if (!keyClass.equals(Text.class) && !valueClass.equals(Text.class)) {
      return;
    }
    final int deafultKVSizeMaximum = 1 << 22; // 4M
    final int kvSizeMaximum = normalConf.getInt(
        TestConstants.NATIVETASK_KVSIZE_MAX_LARGEKV_TEST,
        deafultKVSizeMaximum);
    final FileSystem fs = FileSystem.get(normalConf);

    for (int i = 65536; i <= kvSizeMaximum; i *= 4) {
      int min = i / 4;
      int max = i;
      nativeConf.set(TestConstants.NATIVETASK_KVSIZE_MIN, String.valueOf(min));
      nativeConf.set(TestConstants.NATIVETASK_KVSIZE_MAX, String.valueOf(max));
      normalConf.set(TestConstants.NATIVETASK_KVSIZE_MIN, String.valueOf(min));
      normalConf.set(TestConstants.NATIVETASK_KVSIZE_MAX, String.valueOf(max));

      LOG.info("===KV Size Test: min size: " + min + ", max size: " + max
          + ", keyClass: " + keyClass.getName() + ", valueClass: "
          + valueClass.getName());

      final String inputPath = TestConstants.NATIVETASK_KVTEST_INPUTDIR
          + "/LargeKV/" + keyClass.getName() + "/" + valueClass.getName();

      final String nativeOutputPath = TestConstants.NATIVETASK_KVTEST_NATIVE_OUTPUTDIR
          + "/LargeKV/" + keyClass.getName() + "/" + valueClass.getName();
      // if output file exists ,then delete it
      fs.delete(new Path(nativeOutputPath), true);
      final KVJob nativeJob = new KVJob("Test Large Value Size:"
          + String.valueOf(i), nativeConf, keyClass, valueClass, inputPath,
          nativeOutputPath);
      assertTrue(nativeJob.runJob(), "job should complete successfully");

      final String normalOutputPath = TestConstants.NATIVETASK_KVTEST_NORMAL_OUTPUTDIR
          + "/LargeKV/" + keyClass.getName() + "/" + valueClass.getName();
      // if output file exists ,then delete it
      fs.delete(new Path(normalOutputPath), true);
      final KVJob normalJob = new KVJob("Test Large Key Size:" + String.valueOf(i),
          normalConf, keyClass, valueClass, inputPath, normalOutputPath);
      assertTrue(normalJob.runJob(), "job should complete successfully");

      final boolean compareRet = ResultVerifier.verify(normalOutputPath,
          nativeOutputPath);
      final String reason = "keytype: " + keyClass.getName() + ", valuetype: "
          + valueClass.getName() + ", failed with "
          + (keyClass.equals(Text.class) ? "key" : "value") + ", min size: " + min
          + ", max size: " + max + ", normal out: " + normalOutputPath
          + ", native Out: " + nativeOutputPath;
      assertEquals(true, compareRet, reason);
      ResultVerifier.verifyCounters(normalJob.job, nativeJob.job);
    }
    fs.close();
  }
}
