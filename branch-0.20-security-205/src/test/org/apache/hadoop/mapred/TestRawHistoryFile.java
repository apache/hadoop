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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.junit.Assert;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;

import junit.framework.TestCase;

public class TestRawHistoryFile extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestRawHistoryFile.class);

  public void testRetrieveHistoryFile() {

    MiniMRCluster mrCluster = null;
    JobConf conf = new JobConf();
    try {
      conf.setLong("mapred.job.tracker.retiredjobs.cache.size", 1);
      conf.setLong("mapred.jobtracker.retirejob.interval", 0);
      conf.setLong("mapred.jobtracker.retirejob.check", 0);
      conf.setLong("mapred.jobtracker.completeuserjobs.maximum", 1);
      conf.set("mapreduce.history.server.http.address", "localhost:0");

      mrCluster = new MiniMRCluster(1, conf.get("fs.default.name"), 1,
          null, null, conf);

      conf = mrCluster.createJobConf();
      createInputFile(conf, "/tmp/input");

      RunningJob job = runJob(conf);
      LOG.info("Job details: " + job);

      String historyFile = saveHistoryFile(job.getTrackingURL().
          replaceAll("jobdetails.jsp", "gethistory.jsp"));
      JobHistory.JobInfo jobInfo = new JobHistory.JobInfo(job.getJobID());
      DefaultJobHistoryParser.parseJobTasks(historyFile, jobInfo,
          FileSystem.getLocal(conf));
      LOG.info("STATUS: " + jobInfo.getValues().get(JobHistory.Keys.JOB_STATUS));
      LOG.info("JOBID: " + jobInfo.getValues().get(JobHistory.Keys.JOBID));
      Assert.assertEquals(jobInfo.getValues().get(JobHistory.Keys.JOB_STATUS),
          "SUCCESS");
      Assert.assertEquals(jobInfo.getValues().get(JobHistory.Keys.JOBID),
          job.getJobID());

    } catch (IOException e) {
      LOG.error("Failure running test", e);
      Assert.fail(e.getMessage());
    } finally {
      if (mrCluster != null) mrCluster.shutdown();
    }
  }

  public void testRunningJob() {

    MiniMRCluster mrCluster = null;
    JobConf conf = new JobConf();
    try {
      conf.setLong("mapred.job.tracker.retiredjobs.cache.size", 1);
      conf.setLong("mapred.jobtracker.retirejob.interval", 0);
      conf.setLong("mapred.jobtracker.retirejob.check", 0);
      conf.setLong("mapred.jobtracker.completeuserjobs.maximum", 0);
      conf.set("mapreduce.history.server.http.address", "localhost:0");

      mrCluster = new MiniMRCluster(1, conf.get("fs.default.name"), 1,
          null, null, conf);

      conf = mrCluster.createJobConf();
      createInputFile(conf, "/tmp/input");

      RunningJob job = submitJob(conf);
      LOG.info("Job details: " + job);

      String url = job.getTrackingURL().
          replaceAll("jobdetails.jsp", "gethistory.jsp");
      HttpClient client = new HttpClient();
      GetMethod method = new GetMethod(url);
      try {
        int status = client.executeMethod(method);
        Assert.assertEquals(status, HttpURLConnection.HTTP_BAD_REQUEST);
      } finally {
        method.releaseConnection();
        job.killJob();
      }

    } catch (IOException e) {
      LOG.error("Failure running test", e);
      Assert.fail(e.getMessage());
    } finally {
      if (mrCluster != null) mrCluster.shutdown();
    }
  }

  public void testRetrieveInvalidJob() {

    MiniMRCluster mrCluster = null;
    JobConf conf = new JobConf();
    try {
      conf.set("mapreduce.history.server.http.address", "localhost:0");
      mrCluster = new MiniMRCluster(1, conf.get("fs.default.name"), 1,
          null, null, conf);
      JobConf jobConf = mrCluster.createJobConf();

      String url = "http://" + jobConf.get("mapred.job.tracker.http.address") +
          "/gethistory.jsp?jobid=job_20100714163314505_9991";
      HttpClient client = new HttpClient();
      GetMethod method = new GetMethod(url);
      try {
        int status = client.executeMethod(method);
        Assert.assertEquals(status, HttpURLConnection.HTTP_BAD_REQUEST);
      } finally {
        method.releaseConnection();
      }

    } catch (IOException e) {
      LOG.error("Failure running test", e);
      Assert.fail(e.getMessage());
    } finally {
      if (mrCluster != null) mrCluster.shutdown();
    }
  }

  private void createInputFile(Configuration conf, String path)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream out = fs.create(new Path(path));
    try {
      out.write("hello world".getBytes());
    } finally {
      out.close();
    }
  }

  private synchronized RunningJob runJob(JobConf conf) throws IOException {
    configureJob(conf);
    return JobClient.runJob(conf);
  }

  private synchronized RunningJob submitJob(JobConf conf) throws IOException {
    configureJob(conf);

    JobClient client = new JobClient(conf);
    return client.submitJob(conf);
  }

  private void configureJob(JobConf conf) {
    conf.setJobName("History");

    conf.setInputFormat(TextInputFormat.class);

    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputFormat(NullOutputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(org.apache.hadoop.mapred.lib.IdentityMapper.class);
    conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);

    FileInputFormat.setInputPaths(conf, "/tmp/input");
  }

  private String saveHistoryFile(String url) throws IOException {
    LOG.info("History url: "  + url);
    HttpClient client = new HttpClient();
    GetMethod method = new GetMethod(url);
    try {
      int status = client.executeMethod(method);
      Assert.assertEquals(status, HttpURLConnection.HTTP_OK);
      
      File out = File.createTempFile("HIST_", "hist");
      IOUtils.copyBytes(method.getResponseBodyAsStream(),
          new FileOutputStream(out), 4096, true);
      return out.getAbsolutePath();
    } finally {
      method.releaseConnection();
    }
  }
}
