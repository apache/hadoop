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
package org.apache.hadoop.mapreduce.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapred.RunningJob;

import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class TestEncryptedShuffle {

  private static File testRootDir;

  @BeforeClass
  public static void setUp() throws Exception {
    testRootDir =
        GenericTestUtils.setupTestRootDir(TestEncryptedShuffle.class);
  }

  @Before
  public void createCustomYarnClasspath() throws Exception {
    classpathDir = KeyStoreTestUtil.getClasspathDir(TestEncryptedShuffle.class);
    new File(classpathDir, "core-site.xml").delete();
    dfsFolder = new File(testRootDir, String.format("dfs-%d",
        Time.monotonicNow()));
  }

  @After
  public void cleanUpMiniClusterSpecialConfig() throws Exception {
    new File(classpathDir, "core-site.xml").delete();
    String keystoresDir = testRootDir.getAbsolutePath();
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, classpathDir);
  }

  private String classpathDir;
  private MiniDFSCluster dfsCluster = null;
  private MiniMRClientCluster mrCluster = null;
  private File dfsFolder;

  private void startCluster(Configuration  conf) throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      System.setProperty("hadoop.log.dir", testRootDir.getAbsolutePath());
    }
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions", "true");
    conf.set("hadoop.security.authentication", "simple");
    String cp = conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        StringUtils.join(",",
            YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH))
        + File.pathSeparator + classpathDir;
    conf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, cp);
    dfsCluster = new MiniDFSCluster.Builder(conf, dfsFolder).build();
    FileSystem fileSystem = dfsCluster.getFileSystem();
    fileSystem.mkdirs(new Path("/tmp"));
    fileSystem.mkdirs(new Path("/user"));
    fileSystem.mkdirs(new Path("/hadoop/mapred/system"));
    fileSystem.setPermission(
      new Path("/tmp"), FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(
      new Path("/user"), FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(
      new Path("/hadoop/mapred/system"), FsPermission.valueOf("-rwx------"));
    FileSystem.setDefaultUri(conf, fileSystem.getUri());
    mrCluster = MiniMRClientClusterFactory.create(this.getClass(), 1, conf);

    // so the minicluster conf is avail to the containers.
    Writer writer = new FileWriter(classpathDir + "/core-site.xml");
    mrCluster.getConfig().writeXml(writer);
    writer.close();
  }

  private void stopCluster() throws Exception {
    if (mrCluster != null) {
      mrCluster.stop();
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  protected JobConf getJobConf() throws IOException {
    return new JobConf(mrCluster.getConfig());
  }

  private void encryptedShuffleWithCerts(boolean useClientCerts)
    throws Exception {
    try {
      Configuration conf = new Configuration();
      String keystoresDir = testRootDir.getAbsolutePath();
      String sslConfsDir =
        KeyStoreTestUtil.getClasspathDir(TestEncryptedShuffle.class);
      KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfsDir, conf,
                                      useClientCerts);
      conf.setBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY, true);
      startCluster(conf);
      FileSystem fs = FileSystem.get(getJobConf());
      Path inputDir = new Path("input");
      fs.mkdirs(inputDir);
      Writer writer =
        new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
      writer.write("hello");
      writer.close();

      Path outputDir = new Path("output", "output");

      JobConf jobConf = new JobConf(getJobConf());
      jobConf.setInt("mapred.map.tasks", 1);
      jobConf.setInt("mapred.map.max.attempts", 1);
      jobConf.setInt("mapred.reduce.max.attempts", 1);
      jobConf.set("mapred.input.dir", inputDir.toString());
      jobConf.set("mapred.output.dir", outputDir.toString());
      JobClient jobClient = new JobClient(jobConf);
      RunningJob runJob = jobClient.submitJob(jobConf);
      runJob.waitForCompletion();
      Assert.assertTrue(runJob.isComplete());
      Assert.assertTrue(runJob.isSuccessful());
    } finally {
      stopCluster();
    }
  }

  @Test
  public void encryptedShuffleWithClientCerts() throws Exception {
    encryptedShuffleWithCerts(true);
  }

  @Test
  public void encryptedShuffleWithoutClientCerts() throws Exception {
    encryptedShuffleWithCerts(false);
  }

}

