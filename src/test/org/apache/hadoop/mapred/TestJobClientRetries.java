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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TestMiniMRWithDFS.TestResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestJobClientRetries {
  
  private static final Log LOG = LogFactory.getLog(TestJobClientRetries.class);
  
  MiniMRCluster mr;
  
  @Test
  public void testJobSubmission() throws Exception {
    
    // Start MR cluster
    mr = new MiniMRCluster(2, "file:///", 3);
    
    final List<Exception> exceptions = new ArrayList<Exception>();

    // Get jobConf
    final JobConf jobConf = mr.createJobConf();
    
    // Stop JobTracker
    LOG.info("Stopping JobTracker");
    mr.stopJobTracker();
    
    /*
     * Submit job *after* setting job-client retries to be *on*...
     * the test *should* fail without this config being set
     */
    LOG.info("Stopping JobTracker");
    jobConf.setBoolean(
        JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_KEY, true);
    WordCountThread wc = new WordCountThread(jobConf, exceptions);
    wc.start();
    
    // Restart JobTracker after a little while
    Thread.sleep(5000);
    LOG.info("Re-starting JobTracker for job-submission to go through");
    mr.startJobTracker();
    
    // Wait for the job to complete or for an exception to occur
    LOG.info("Waiting for job success/failure ...");
    wc.join();

    Assert.assertNotNull(wc.result);
    Assert.assertEquals("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" +
        "quick\t1\nred\t1\nsilly\t1\nsox\t1\n", wc.result.output);
    Assert.assertTrue("exceptions is not empty: " + exceptions, exceptions.isEmpty());
  }

  @After
  public void tearDown() throws Exception {
    mr.shutdown();
  }
  
  public static class WordCountThread extends Thread {
    JobConf jobConf;
    List<Exception> exceptions;
    TestResult result;
    
    public WordCountThread(JobConf jobConf, List<Exception> exceptions) {
      super(WordCountThread.class.getName());
      this.jobConf = jobConf;
      this.exceptions = exceptions;
    }

    @Override
    public void run() {
      try {
        FileSystem fs = FileSystem.getLocal(jobConf);
        Path testdir = new Path(
            System.getProperty("test.build.data","/tmp")).makeQualified(fs);
        final Path inDir = new Path(testdir, "input");
        final Path outDir = new Path(testdir, "output");
        String input = "The quick brown fox\nhas many silly\nred fox sox\n";
        LOG.info("Starting word-count");
        result = 
            TestMiniMRWithDFS.launchWordCount(
                jobConf, inDir, outDir, input, 3, 1);
        LOG.info("Finished word-count");
      } catch (Exception e) {
        LOG.error("Caught exception during word-count", e);
        exceptions.add(e);
        result = null;
      }
    }
  }
}
