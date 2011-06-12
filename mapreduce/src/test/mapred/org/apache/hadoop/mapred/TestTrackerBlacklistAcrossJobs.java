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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;

public class TestTrackerBlacklistAcrossJobs extends TestCase {
  private static final String hosts[] = new String[] {
    "host1.rack.com", "host2.rack.com", "host3.rack.com"
  };

  public static class FailOnHostMapper extends MapReduceBase
    implements Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {
    String hostname = "";
    
    public void configure(JobConf job) {
      this.hostname = job.get(TTConfig.TT_HOST_NAME);
    }
    
    public void map(NullWritable key, NullWritable value,
                    OutputCollector<NullWritable, NullWritable> output,
                    Reporter reporter)
    throws IOException {
      if (this.hostname.equals(hosts[0])) {
        // fail here
        throw new IOException("failing on host: " + hosts[0]);
      }
    }
  }

  public void testBlacklistAcrossJobs() throws IOException {
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    Configuration conf = new Configuration();
    fileSys = FileSystem.get(conf);
    // start mr cluster
    JobConf jtConf = new JobConf();
    jtConf.setInt(JTConfig.JT_MAX_TRACKER_BLACKLISTS, 1);

    mr = new MiniMRCluster(3, fileSys.getUri().toString(),
                           1, null, hosts, jtConf);

    // setup job configuration
    JobConf mrConf = mr.createJobConf();
    JobConf job = new JobConf(mrConf);
    job.setInt(JobContext.MAX_TASK_FAILURES_PER_TRACKER, 1);
    job.setNumMapTasks(6);
    job.setNumReduceTasks(0);
    job.setMapperClass(FailOnHostMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputFormat(NullOutputFormat.class);
    job.setInputFormat(TestReduceFetchFromPartialMem.FakeIF.class);
    
    // run the job
    JobClient jc = new JobClient(mrConf);
    RunningJob running = JobClient.runJob(job);
    assertEquals("Job failed", JobStatus.SUCCEEDED, running.getJobState());
    assertEquals("Did not blacklist the host", 1, 
      jc.getClusterStatus().getBlacklistedTrackers());
    assertEquals("Fault count should be 1", 1, mr.getFaultCount(hosts[0]));

    // run the same job once again 
    // there should be no change in blacklist count
    running = JobClient.runJob(job);
    assertEquals("Job failed", JobStatus.SUCCEEDED, running.getJobState());
    assertEquals("Didn't blacklist the host", 1,
      jc.getClusterStatus().getBlacklistedTrackers());
    assertEquals("Fault count should be 1", 1, mr.getFaultCount(hosts[0]));

    if (fileSys != null) { fileSys.close(); }
    if (mr!= null) { mr.shutdown(); }
  }
}
