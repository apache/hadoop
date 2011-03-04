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
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobTracker.RetireJobInfo;

/**
 * Test if the job retire works fine.
 */
public class TestJobRetire extends TestCase {
  static final Path testDir =
    new Path(System.getProperty("test.build.data","/tmp"),
             "job-expiry-testing");

  public void testJobRetire() throws Exception {
    MiniMRCluster mr = null;
    try {
      JobConf conf = new JobConf();

      conf.setLong("mapred.job.tracker.retiredjobs.cache.size", 1);
      conf.setLong("mapred.jobtracker.retirejob.interval", 0);
      conf.setLong("mapred.jobtracker.retirejob.check", 0);
      conf.getLong("mapred.jobtracker.completeuserjobs.maximum", 0);
      mr = new MiniMRCluster(0, 0, 1, "file:///", 1, null, null, null, conf, 0);
      JobConf jobConf = mr.createJobConf();
      JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();

      Path inDir = new Path(testDir, "input1");
      Path outDir = new Path(testDir, "output1");

      JobID id1 = validateJobRetire(jobConf, inDir, outDir, jobtracker);

      outDir = new Path(testDir, "output2");
      JobID id2 = validateJobRetire(jobConf, inDir, outDir, jobtracker);

      assertNull("Job not removed from cache", jobtracker.getJobStatus(id1));

      assertEquals("Total job in cache not correct",
          1, jobtracker.getAllJobs().length);
    } finally {
      if (mr != null) { mr.shutdown();}
    }
  }

  private JobID validateJobRetire(JobConf jobConf, Path inDir, Path outDir,
      JobTracker jobtracker) throws IOException {

    RunningJob rj = UtilsForTests.runJob(jobConf, inDir, outDir, 0, 0);
    rj.waitForCompletion();
    assertTrue(rj.isSuccessful());
    JobID id = rj.getID();

    JobInProgress job = jobtracker.getJob(id);
    //wait for job to get retired
    for (int i = 0; i < 10 && job != null; i++) {
      UtilsForTests.waitFor(1000);
      job = jobtracker.getJob(id);
    }
    assertNull("Job did not retire", job);
    RetireJobInfo retired = jobtracker.retireJobs.get(id);
    assertTrue("History url not set", retired.getHistoryFile() != null &&
      retired.getHistoryFile().length() > 0);
    assertNotNull("Job is not in cache", jobtracker.getJobStatus(id));

    // get the job conf filename
    String name = jobtracker.getLocalJobFilePath(id);
    File file = new File(name);

    assertFalse("JobConf file not deleted", file.exists());
    //test redirection
    URL jobUrl = new URL(rj.getTrackingURL());
    HttpURLConnection conn = (HttpURLConnection) jobUrl.openConnection();
    conn.setInstanceFollowRedirects(false);
    conn.connect();
    assertEquals(HttpURLConnection.HTTP_MOVED_TEMP, conn.getResponseCode());
    conn.disconnect();

    URL redirectedUrl = new URL(conn.getHeaderField("Location"));
    conn = (HttpURLConnection) redirectedUrl.openConnection();
    conn.connect();
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    conn.disconnect();

    return id;
  }

}
