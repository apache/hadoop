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
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.mapreduce.security.IntermediateEncryptedStream;
import org.apache.hadoop.mapreduce.security.SpillCallBackPathsFinder;
import org.apache.hadoop.mapreduce.util.MRJobConfUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * check for the job submission options of
 * -jt local -libjars
 */
public class TestLocalJobSubmission {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestLocalJobSubmission.class);

  private static File testRootDir;

  @Rule
  public TestName unitTestName = new TestName();
  private File unitTestDir;
  private Path jarPath;
  private Configuration config;

  @BeforeClass
  public static void setupClass() throws Exception {
    // setup the test root directory
    testRootDir =
        GenericTestUtils.setupTestRootDir(TestLocalJobSubmission.class);
  }

  @Before
  public void setup() throws IOException {
    unitTestDir = new File(testRootDir, unitTestName.getMethodName());
    unitTestDir.mkdirs();
    config = createConfig();
    jarPath = makeJar(new Path(unitTestDir.getAbsolutePath(), "test.jar"));
  }

  private Configuration createConfig() {
    // Set the temp directories a subdir of the test directory.
    Configuration conf =
        MRJobConfUtil.setLocalDirectoriesConfigForTesting(null, unitTestDir);
    conf.set(MRConfig.FRAMEWORK_NAME, "local");
    return conf;
  }

  /**
   * Test the local job submission options of -jt local -libjars.
   *
   * @throws IOException thrown if there's an error creating the JAR file
   */
  @Test
  public void testLocalJobLibjarsOption() throws IOException {
    testLocalJobLibjarsOption(config);
    config.setBoolean(Job.USE_WILDCARD_FOR_LIBJARS, false);
    testLocalJobLibjarsOption(config);
  }

  /**
   * Test the local job submission options of -jt local -libjars.
   *
   * @param conf the {@link Configuration} to use
   * @throws IOException thrown if there's an error creating the JAR file
   */
  private void testLocalJobLibjarsOption(Configuration conf)
      throws IOException {
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:9000");
    conf.set(MRConfig.FRAMEWORK_NAME, "local");
    final String[] args = {
        "-jt" , "local", "-libjars", jarPath.toString(),
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"
    };
    int res = -1;
    try {
      res = ToolRunner.run(conf, new SleepJob(), args);
    } catch (Exception e) {
      LOG.error("Job failed with {}", e.getLocalizedMessage(), e);
      fail("Job failed");
    }
    assertEquals("dist job res is not 0:", 0, res);
  }

  /**
   * test the local job submission with
   * intermediate data encryption enabled.
   * @throws IOException
   */
  @Test
  public void testLocalJobEncryptedIntermediateData() throws IOException {
    config = MRJobConfUtil.initEncryptedIntermediateConfigsForTesting(config);
    final String[] args = {
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"
    };
    int res = -1;
    try {
      SpillCallBackPathsFinder spillInjector =
          (SpillCallBackPathsFinder) IntermediateEncryptedStream
              .setSpillCBInjector(new SpillCallBackPathsFinder());
      res = ToolRunner.run(config, new SleepJob(), args);
      Assert.assertTrue("No spill occurred",
          spillInjector.getEncryptedSpilledFiles().size() > 0);
    } catch (Exception e) {
      LOG.error("Job failed with {}", e.getLocalizedMessage(), e);
      fail("Job failed");
    }
    assertEquals("dist job res is not 0:", 0, res);
  }

  /**
   * test JOB_MAX_MAP configuration.
   * @throws Exception
   */
  @Test
  public void testJobMaxMapConfig() throws Exception {
    config.setInt(MRJobConfig.JOB_MAX_MAP, 0);
    final String[] args = {
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"
    };
    int res = -1;
    try {
      res = ToolRunner.run(config, new SleepJob(), args);
      fail("Job should fail");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getLocalizedMessage().contains(
          "The number of map tasks 1 exceeded limit"));
    }
  }

  /**
   * Test local job submission with a file option.
   *
   * @throws IOException
   */
  @Test
  public void testLocalJobFilesOption() throws IOException {
    config.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:9000");
    final String[] args = {
        "-jt", "local", "-files", jarPath.toString(),
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"
    };
    int res = -1;
    try {
      res = ToolRunner.run(config, new SleepJob(), args);
    } catch (Exception e) {
      LOG.error("Job failed with {}", e.getLocalizedMessage(), e);
      fail("Job failed");
    }
    assertEquals("dist job res is not 0:", 0, res);
  }

  /**
   * Test local job submission with an archive option.
   *
   * @throws IOException
   */
  @Test
  public void testLocalJobArchivesOption() throws IOException {
    config.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:9000");
    final String[] args =
        {"-jt", "local", "-archives", jarPath.toString(), "-m", "1", "-r",
            "1", "-mt", "1", "-rt", "1"};
    int res = -1;
    try {
      res = ToolRunner.run(config, new SleepJob(), args);
    } catch (Exception e) {
      LOG.error("Job failed with {}" + e.getLocalizedMessage(), e);
      fail("Job failed");
    }
    assertEquals("dist job res is not 0:", 0, res);
  }

  private Path makeJar(Path p) throws IOException {
    FileOutputStream fos = new FileOutputStream(p.toString());
    JarOutputStream jos = new JarOutputStream(fos);
    ZipEntry ze = new ZipEntry("test.jar.inside");
    jos.putNextEntry(ze);
    jos.write(("inside the jar!").getBytes());
    jos.closeEntry();
    jos.close();
    return p;
  }
}
