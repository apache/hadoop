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

package org.apache.hadoop.mapreduce.v2;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.CustomOutputCommitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestMRAppWithCombiner {

  protected static MiniMRYarnCluster mrCluster;
  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  private static final Log LOG = LogFactory.getLog(TestMRAppWithCombiner.class);

  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  @BeforeClass
  public static void setup() throws IOException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniMRYarnCluster(TestMRJobs.class.getName(), 3);
      Configuration conf = new Configuration();
      mrCluster.init(conf);
      mrCluster.start();
    }

    // Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
    // workaround the absent public discache.
    localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR),
        TestMRJobs.APP_JAR);
    localFs.setPermission(TestMRJobs.APP_JAR, new FsPermission("700"));
  }

  @AfterClass
  public static void tearDown() {
    if (mrCluster != null) {
      mrCluster.stop();
      mrCluster = null;
    }
  }

  @Test
  public void testCombinerShouldUpdateTheReporter() throws Exception {
    JobConf conf = new JobConf(mrCluster.getConfig());
    int numMaps = 5;
    int numReds = 2;
    Path in = new Path(mrCluster.getTestWorkDir().getAbsolutePath(),
        "testCombinerShouldUpdateTheReporter-in");
    Path out = new Path(mrCluster.getTestWorkDir().getAbsolutePath(),
        "testCombinerShouldUpdateTheReporter-out");
    createInputOutPutFolder(in, out, numMaps);
    conf.setJobName("test-job-with-combiner");
    conf.setMapperClass(IdentityMapper.class);
    conf.setCombinerClass(MyCombinerToCheckReporter.class);
    //conf.setJarByClass(MyCombinerToCheckReporter.class);
    conf.setReducerClass(IdentityReducer.class);
    DistributedCache.addFileToClassPath(TestMRJobs.APP_JAR, conf);
    conf.setOutputCommitter(CustomOutputCommitter.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(conf, in);
    FileOutputFormat.setOutputPath(conf, out);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReds);
    
    runJob(conf);
  }

  static void createInputOutPutFolder(Path inDir, Path outDir, int numMaps)
      throws Exception {
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    if (!fs.exists(inDir)) {
      fs.mkdirs(inDir);
    }
    String input = "The quick brown fox\n" + "has many silly\n"
        + "red fox sox\n";
    for (int i = 0; i < numMaps; ++i) {
      DataOutputStream file = fs.create(new Path(inDir, "part-" + i));
      file.writeBytes(input);
      file.close();
    }
  }

  static boolean runJob(JobConf conf) throws Exception {
    JobClient jobClient = new JobClient(conf);
    RunningJob job = jobClient.submitJob(conf);
    return jobClient.monitorAndPrintJob(conf, job);
  }

  class MyCombinerToCheckReporter<K, V> extends IdentityReducer<K, V> {
    public void reduce(K key, Iterator<V> values, OutputCollector<K, V> output,
        Reporter reporter) throws IOException {
      if (Reporter.NULL == reporter) {
        Assert.fail("A valid Reporter should have been used but, Reporter.NULL is used");
      }
    }
  }

}
