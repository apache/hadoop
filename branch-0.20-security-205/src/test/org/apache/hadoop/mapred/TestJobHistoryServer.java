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
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.junit.Assert;
import junit.framework.TestCase;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.MalformedURLException;

public class TestJobHistoryServer extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestJobHistoryServer.class);

  public void testHistoryServerEmbedded() {

    MiniMRCluster mrCluster = null;
    JobConf conf = new JobConf();
    try {
      conf.setLong("mapred.job.tracker.retiredjobs.cache.size", 1);
      conf.setLong("mapred.jobtracker.retirejob.interval", 0);
      conf.setLong("mapred.jobtracker.retirejob.check", 0);
      conf.setLong("mapred.jobtracker.completeuserjobs.maximum", 2);
      conf.set(JobHistoryServer.MAPRED_HISTORY_SERVER_HTTP_ADDRESS,
          "localhost:0");

      mrCluster = new MiniMRCluster(1, conf.get("fs.default.name"), 1,
          null, null, conf);
      String historyAddress = JobHistoryServer.getHistoryUrlPrefix(mrCluster.
          getJobTrackerRunner().getJobTracker().conf);
      LOG.info("******** History Address: " + historyAddress);

      conf = mrCluster.createJobConf();
      createInputFile(conf, "/tmp/input");

      RunningJob job = runJob(conf);
      LOG.info("Job details: " + job);

      String redirectUrl = getRedirectUrl(job.getTrackingURL());
      Assert.assertEquals(redirectUrl.contains(historyAddress), true);

    } catch (IOException e) {
      LOG.error("Failure running test", e);
      Assert.fail(e.getMessage());
    } finally {
      if (mrCluster != null) mrCluster.shutdown();
    }
  }

  public void testHistoryServerStandalone() {

    MiniMRCluster mrCluster = null;
    JobConf conf = new JobConf();
    JobHistoryServer server = null;
    try {
      conf.setLong("mapred.job.tracker.retiredjobs.cache.size", 1);
      conf.setLong("mapred.jobtracker.retirejob.interval", 0);
      conf.setLong("mapred.jobtracker.retirejob.check", 0);
      conf.setLong("mapred.jobtracker.completeuserjobs.maximum", 2);
      conf.set(JobHistoryServer.MAPRED_HISTORY_SERVER_HTTP_ADDRESS,
          "localhost:8090");
      conf.setBoolean(JobHistoryServer.MAPRED_HISTORY_SERVER_EMBEDDED, false);

      mrCluster = new MiniMRCluster(1, conf.get("fs.default.name"), 1,
          null, null, conf);
      server = new JobHistoryServer(conf);
      server.start();

      String historyAddress = JobHistoryServer.getHistoryUrlPrefix(conf);
      LOG.info("******** History Address: " + historyAddress);

      conf = mrCluster.createJobConf();
      createInputFile(conf, "/tmp/input");

      RunningJob job = runJob(conf);
      LOG.info("Job details: " + job);

      String redirectUrl = getRedirectUrl(job.getTrackingURL());
      Assert.assertEquals(redirectUrl.contains(historyAddress), true);

    } catch (IOException e) {
      LOG.error("Failure running test", e);
      Assert.fail(e.getMessage());
    } finally {
      if (mrCluster != null) mrCluster.shutdown();
      try {
        if (server != null) server.shutdown();
      } catch (Exception ignore) { }
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

    return JobClient.runJob(conf);
  }

  private String getRedirectUrl(String jobUrl) throws IOException {
    HttpClient client = new HttpClient();
    GetMethod method = new GetMethod(jobUrl);
    method.setFollowRedirects(false);
    try {
      int status = client.executeMethod(method);
      Assert.assertEquals(status, HttpURLConnection.HTTP_MOVED_TEMP);

      LOG.info("Location: " + method.getResponseHeader("Location"));
      return method.getResponseHeader("Location").getValue();
    } finally {
      method.releaseConnection();
    }
  }

}
