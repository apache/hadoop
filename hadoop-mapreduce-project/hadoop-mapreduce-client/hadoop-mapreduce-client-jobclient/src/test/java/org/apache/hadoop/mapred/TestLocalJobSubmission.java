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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * check for the job submission options of
 * -jt local -libjars
 */
public class TestLocalJobSubmission {
  private static Path TEST_ROOT_DIR =
      new Path(System.getProperty("test.build.data","/tmp"));

  @Before
  public void configure() throws Exception {
  }

  @After
  public void cleanup() {
  }

  /**
   * test the local job submission options of
   * -jt local -libjars.
   * @throws IOException
   */
  @Test
  public void testLocalJobLibjarsOption() throws IOException {
    Path jarPath = makeJar(new Path(TEST_ROOT_DIR, "test.jar"));

    Configuration conf = new Configuration();
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
      System.out.println("Job failed with " + e.getLocalizedMessage());
      e.printStackTrace(System.out);
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
    Configuration conf = new Configuration();
    conf.set(MRConfig.FRAMEWORK_NAME, "local");
    conf.setBoolean(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA, true);
    final String[] args = {
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"
    };
    int res = -1;
    try {
      res = ToolRunner.run(conf, new SleepJob(), args);
    } catch (Exception e) {
      System.out.println("Job failed with " + e.getLocalizedMessage());
      e.printStackTrace(System.out);
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
    Configuration conf = new Configuration();
    conf.set(MRConfig.FRAMEWORK_NAME, "local");
    conf.setInt(MRJobConfig.JOB_MAX_MAP, 0);
    final String[] args = {
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"
    };
    int res = -1;
    try {
      res = ToolRunner.run(conf, new SleepJob(), args);
      fail("Job should fail");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getLocalizedMessage().contains(
          "The number of map tasks 1 exceeded limit"));
    }
  }

  private Path makeJar(Path p) throws IOException {
    FileOutputStream fos = new FileOutputStream(new File(p.toString()));
    JarOutputStream jos = new JarOutputStream(fos);
    ZipEntry ze = new ZipEntry("test.jar.inside");
    jos.putNextEntry(ze);
    jos.write(("inside the jar!").getBytes());
    jos.closeEntry();
    jos.close();
    return p;
  }
}
