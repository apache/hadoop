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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.junit.AfterClass;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class LargeKVTest {
  private static final Log LOG = LogFactory.getLog(LargeKVTest.class);

  @Before
  public void startUp() throws Exception {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    Assume.assumeTrue(NativeRuntime.isNativeLibraryLoaded());
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
  public void testKeySize() throws Exception {
    runKVSizeTests(Text.class, IntWritable.class);
  }

  @Test
  public void testValueSize() throws Exception {
    runKVSizeTests(IntWritable.class, Text.class);
  }

  @AfterClass
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
      assertTrue("job should complete successfully", nativeJob.runJob());

      final String normalOutputPath = TestConstants.NATIVETASK_KVTEST_NORMAL_OUTPUTDIR
          + "/LargeKV/" + keyClass.getName() + "/" + valueClass.getName();
      // if output file exists ,then delete it
      fs.delete(new Path(normalOutputPath), true);
      final KVJob normalJob = new KVJob("Test Large Key Size:" + String.valueOf(i),
          normalConf, keyClass, valueClass, inputPath, normalOutputPath);
      assertTrue("job should complete successfully", normalJob.runJob());

      final boolean compareRet = ResultVerifier.verify(normalOutputPath,
          nativeOutputPath);
      final String reason = "keytype: " + keyClass.getName() + ", valuetype: "
          + valueClass.getName() + ", failed with "
          + (keyClass.equals(Text.class) ? "key" : "value") + ", min size: " + min
          + ", max size: " + max + ", normal out: " + normalOutputPath
          + ", native Out: " + nativeOutputPath;
      assertEquals(reason, true, compareRet);
      ResultVerifier.verifyCounters(normalJob.job, nativeJob.job);
    }
    fs.close();
  }
}
