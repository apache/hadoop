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

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.extensions.TestSetup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TestMapCollection.FakeIF;
import org.apache.hadoop.mapred.TestMapCollection.FakeSplit;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import static org.apache.hadoop.mapred.Task.FileSystemCounter.HDFS_WRITE;
import static org.apache.hadoop.mapred.Task.FileSystemCounter.LOCAL_READ;

public class TestReduceFetch extends TestCase {

  private static MiniMRCluster mrCluster = null;
  private static MiniDFSCluster dfsCluster = null;
  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestReduceFetch.class)) {
      protected void setUp() throws Exception {
        Configuration conf = new Configuration();
        dfsCluster = new MiniDFSCluster(conf, 2, true, null);
        mrCluster = new MiniMRCluster(2,
            dfsCluster.getFileSystem().getUri().toString(), 1);
      }
      protected void tearDown() throws Exception {
        if (dfsCluster != null) { dfsCluster.shutdown(); }
        if (mrCluster != null) { mrCluster.shutdown(); }
      }
    };
    return setup;
  }

  public static class MapMB
      implements Mapper<NullWritable,NullWritable,Text,Text> {

    public void map(NullWritable nk, NullWritable nv,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      Text key = new Text();
      Text val = new Text();
      key.set("KEYKEYKEYKEYKEYKEYKEYKEY");
      byte[] b = new byte[1024];
      Arrays.fill(b, (byte)'V');
      val.set(b);
      b = null;
      for (int i = 0; i < 1024; ++i) {
        output.collect(key, val);
      }
    }
    public void configure(JobConf conf) { }
    public void close() throws IOException { }
  }

  public static Counters runJob(JobConf conf) throws Exception {
    conf.setMapperClass(MapMB.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setNumMapTasks(3);
    conf.setNumReduceTasks(1);
    conf.setInputFormat(FakeIF.class);
    FileInputFormat.setInputPaths(conf, new Path("/in"));
    final Path outp = new Path("/out");
    FileOutputFormat.setOutputPath(conf, outp);
    SkipBadRecords.setEnabled(conf, false);
    RunningJob job = null;
    try {
      job = JobClient.runJob(conf);
      assertTrue(job.isSuccessful());
    } finally {
      FileSystem fs = dfsCluster.getFileSystem();
      if (fs.exists(outp)) {
        fs.delete(outp, true);
      }
    }
    return job.getCounters();
  }

  public void testReduceFromDisk() throws Exception {
    JobConf job = mrCluster.createJobConf();
    job.set("mapred.job.reduce.input.buffer.percent", "0.0");
    Counters c = runJob(job);
    assertTrue(c.findCounter(HDFS_WRITE).getCounter() <=
               c.findCounter(LOCAL_READ).getCounter());
  }

  public void testReduceFromPartialMem() throws Exception {
    JobConf job = mrCluster.createJobConf();
    job.setInt("mapred.inmem.merge.threshold", 2);
    job.set("mapred.job.reduce.input.buffer.percent", "1.0");
    Counters c = runJob(job);
    assertTrue(c.findCounter(HDFS_WRITE).getCounter() >=
               c.findCounter(LOCAL_READ).getCounter() + 1024 * 1024);
  }

  public void testReduceFromMem() throws Exception {
    JobConf job = mrCluster.createJobConf();
    job.set("mapred.job.reduce.input.buffer.percent", "1.0");
    Counters c = runJob(job);
    assertTrue(c.findCounter(LOCAL_READ).getCounter() == 0);
  }

}
