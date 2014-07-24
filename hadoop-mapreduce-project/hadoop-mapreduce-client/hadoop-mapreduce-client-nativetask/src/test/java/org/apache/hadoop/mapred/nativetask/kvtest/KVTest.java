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
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class KVTest {
  private static Class<?>[] keyclasses = null;
  private static Class<?>[] valueclasses = null;
  private static String[] keyclassNames = null;
  private static String[] valueclassNames = null;

  private static Configuration nativekvtestconf = ScenarioConfiguration.getNativeConfiguration();
  private static Configuration hadoopkvtestconf = ScenarioConfiguration.getNormalConfiguration();
  static {
    nativekvtestconf.addResource(TestConstants.KVTEST_CONF_PATH);
    hadoopkvtestconf.addResource(TestConstants.KVTEST_CONF_PATH);
  }

  @Parameters(name = "key:{0}\nvalue:{1}")
  public static Iterable<Class<?>[]> data() {
    final String valueclassesStr = nativekvtestconf
        .get(TestConstants.NATIVETASK_KVTEST_VALUECLASSES);
    System.out.println(valueclassesStr);
    valueclassNames = valueclassesStr.replaceAll("\\s", "").split(";");// delete
    // " "
    final ArrayList<Class<?>> tmpvalueclasses = new ArrayList<Class<?>>();
    for (int i = 0; i < valueclassNames.length; i++) {
      try {
        if (valueclassNames[i].equals("")) {
          continue;
        }
        tmpvalueclasses.add(Class.forName(valueclassNames[i]));
      } catch (final ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
    valueclasses = tmpvalueclasses.toArray(new Class[tmpvalueclasses.size()]);
    final String keyclassesStr = nativekvtestconf.get(TestConstants.NATIVETASK_KVTEST_KEYCLASSES);
    System.out.println(keyclassesStr);
    keyclassNames = keyclassesStr.replaceAll("\\s", "").split(";");// delete
    // " "
    final ArrayList<Class<?>> tmpkeyclasses = new ArrayList<Class<?>>();
    for (int i = 0; i < keyclassNames.length; i++) {
      try {
        if (keyclassNames[i].equals("")) {
          continue;
        }
        tmpkeyclasses.add(Class.forName(keyclassNames[i]));
      } catch (final ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
    keyclasses = tmpkeyclasses.toArray(new Class[tmpkeyclasses.size()]);
    final Class<?>[][] kvgroup = new Class<?>[keyclassNames.length * valueclassNames.length][2];
    for (int i = 0; i < keyclassNames.length; i++) {
      final int tmpindex = i * valueclassNames.length;
      for (int j = 0; j < valueclassNames.length; j++) {
        kvgroup[tmpindex + j][0] = keyclasses[i];
        kvgroup[tmpindex + j][1] = valueclasses[j];
      }
    }
    return Arrays.asList(kvgroup);
  }

  private final Class<?> keyclass;
  private final Class<?> valueclass;

  public KVTest(Class<?> keyclass, Class<?> valueclass) {
    this.keyclass = keyclass;
    this.valueclass = valueclass;

  }

  @Test
  public void testKVCompability() {

    try {
      final String nativeoutput = this.runNativeTest(
          "Test:" + keyclass.getSimpleName() + "--" + valueclass.getSimpleName(), keyclass, valueclass);
      final String normaloutput = this.runNormalTest(
          "Test:" + keyclass.getSimpleName() + "--" + valueclass.getSimpleName(), keyclass, valueclass);
      final boolean compareRet = ResultVerifier.verify(normaloutput, nativeoutput);
      final String input = nativekvtestconf.get(TestConstants.NATIVETASK_KVTEST_INPUTDIR) + "/"
          + keyclass.getName()
          + "/" + valueclass.getName();
      if(compareRet){
        final FileSystem fs = FileSystem.get(hadoopkvtestconf);
        fs.delete(new Path(nativeoutput), true);
        fs.delete(new Path(normaloutput), true);
        fs.delete(new Path(input), true);
        fs.close();
      }
      assertEquals("file compare result: if they are the same ,then return true", true, compareRet);
    } catch (final IOException e) {
      assertEquals("test run exception:", null, e);
    } catch (final Exception e) {
      assertEquals("test run exception:", null, e);
    }
  }

  @Before
  public void startUp() {

  }

  private String runNativeTest(String jobname, Class<?> keyclass, Class<?> valueclass) throws IOException {
    final String inputpath = nativekvtestconf.get(TestConstants.NATIVETASK_KVTEST_INPUTDIR) + "/"
        + keyclass.getName()
        + "/" + valueclass.getName();
    final String outputpath = nativekvtestconf.get(TestConstants.NATIVETASK_KVTEST_OUTPUTDIR) + "/"
        + keyclass.getName() + "/" + valueclass.getName();
    // if output file exists ,then delete it
    final FileSystem fs = FileSystem.get(nativekvtestconf);
    fs.delete(new Path(outputpath));
    fs.close();
    nativekvtestconf.set(TestConstants.NATIVETASK_KVTEST_CREATEFILE, "true");
    try {
      final KVJob keyJob = new KVJob(jobname, nativekvtestconf, keyclass, valueclass, inputpath, outputpath);
      keyJob.runJob();
    } catch (final Exception e) {
      return "native testcase run time error.";
    }
    return outputpath;
  }

  private String runNormalTest(String jobname, Class<?> keyclass, Class<?> valueclass) throws IOException {
    final String inputpath = hadoopkvtestconf.get(TestConstants.NATIVETASK_KVTEST_INPUTDIR) + "/"
        + keyclass.getName()
        + "/" + valueclass.getName();
    final String outputpath = hadoopkvtestconf
        .get(TestConstants.NATIVETASK_KVTEST_NORMAL_OUTPUTDIR)
        + "/"
        + keyclass.getName() + "/" + valueclass.getName();
    // if output file exists ,then delete it
    final FileSystem fs = FileSystem.get(hadoopkvtestconf);
    fs.delete(new Path(outputpath));
    fs.close();
    hadoopkvtestconf.set(TestConstants.NATIVETASK_KVTEST_CREATEFILE, "false");
    try {
      final KVJob keyJob = new KVJob(jobname, hadoopkvtestconf, keyclass, valueclass, inputpath, outputpath);
      keyJob.runJob();
    } catch (final Exception e) {
      return "normal testcase run time error.";
    }
    return outputpath;
  }

}
