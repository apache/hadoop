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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.junit.AfterClass;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class KVTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(KVTest.class);

  private static Configuration nativekvtestconf = ScenarioConfiguration.getNativeConfiguration();
  private static Configuration hadoopkvtestconf = ScenarioConfiguration.getNormalConfiguration();
  static {
    nativekvtestconf.addResource(TestConstants.KVTEST_CONF_PATH);
    hadoopkvtestconf.addResource(TestConstants.KVTEST_CONF_PATH);
  }

  private static List<Class<?>> parseClassNames(String spec) {
    List<Class<?>> ret = Lists.newArrayList();
      Iterable<String> classNames = Splitter.on(';').trimResults()
        .omitEmptyStrings().split(spec);
    for (String className : classNames) {
      try {
        ret.add(Class.forName(className));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return ret;
  }

  /**
   * Parameterize the test with the specified key and value types.
   */
  @Parameters(name = "key:{0}\nvalue:{1}")
  public static Iterable<Class<?>[]> data() throws Exception {
    // Parse the config.
    final String valueClassesStr = nativekvtestconf
        .get(TestConstants.NATIVETASK_KVTEST_VALUECLASSES);
    LOG.info("Parameterizing with value classes: " + valueClassesStr);
    List<Class<?>> valueClasses = parseClassNames(valueClassesStr);
    
    final String keyClassesStr = nativekvtestconf.get(
        TestConstants.NATIVETASK_KVTEST_KEYCLASSES);
    LOG.info("Parameterizing with key classes: " + keyClassesStr);
    List<Class<?>> keyClasses = parseClassNames(keyClassesStr);

    // Generate an entry for each key type.
    List<Class<?>[]> pairs = Lists.newArrayList();
    for (Class<?> keyClass : keyClasses) {
      pairs.add(new Class<?>[]{ keyClass, LongWritable.class });
    }
    // ...and for each value type.
    for (Class<?> valueClass : valueClasses) {
      pairs.add(new Class<?>[]{ LongWritable.class, valueClass });
    }
    return pairs;
  }

  private final Class<?> keyclass;
  private final Class<?> valueclass;

  public KVTest(Class<?> keyclass, Class<?> valueclass) {
    this.keyclass = keyclass;
    this.valueclass = valueclass;
  }

  @Before
  public void startUp() throws Exception {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    Assume.assumeTrue(NativeRuntime.isNativeLibraryLoaded());
  }

  @Test
  public void testKVCompability() throws Exception {
    final FileSystem fs = FileSystem.get(nativekvtestconf);
    final String jobName = "Test:" + keyclass.getSimpleName() + "--"
        + valueclass.getSimpleName();
    final String inputPath = TestConstants.NATIVETASK_KVTEST_INPUTDIR + "/"
        + keyclass.getName() + "/" + valueclass.getName();
    final String nativeOutputPath = TestConstants.NATIVETASK_KVTEST_NATIVE_OUTPUTDIR
        + "/" + keyclass.getName() + "/" + valueclass.getName();
    // if output file exists ,then delete it
    fs.delete(new Path(nativeOutputPath), true);
    nativekvtestconf.set(TestConstants.NATIVETASK_KVTEST_CREATEFILE, "true");
    final KVJob nativeJob = new KVJob(jobName, nativekvtestconf, keyclass,
        valueclass, inputPath, nativeOutputPath);
    assertThat(nativeJob.runJob())
        .withFailMessage("job should complete successfully")
        .isTrue();

    final String normalOutputPath = TestConstants.NATIVETASK_KVTEST_NORMAL_OUTPUTDIR
        + "/" + keyclass.getName() + "/" + valueclass.getName();
    // if output file exists ,then delete it
    fs.delete(new Path(normalOutputPath), true);
    hadoopkvtestconf.set(TestConstants.NATIVETASK_KVTEST_CREATEFILE, "false");
    final KVJob normalJob = new KVJob(jobName, hadoopkvtestconf, keyclass,
        valueclass, inputPath, normalOutputPath);
    assertThat(normalJob.runJob())
        .withFailMessage("job should complete successfully")
        .isTrue();

    final boolean compareRet = ResultVerifier.verify(normalOutputPath,
        nativeOutputPath);
    assertThat(compareRet).withFailMessage("job output not the same").isTrue();
    ResultVerifier.verifyCounters(normalJob.job, nativeJob.job);
    fs.close();
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    final FileSystem fs = FileSystem.get(new ScenarioConfiguration());
    fs.delete(new Path(TestConstants.NATIVETASK_KVTEST_DIR), true);
    fs.close();
  }
}
