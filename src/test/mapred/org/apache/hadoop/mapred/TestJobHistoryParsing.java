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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.JobFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistory;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobSubmittedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskFinishedEvent;

/**
 * Unit test to test if the JobHistory writer/parser is able to handle
 * values with special characters
 * This test also tests if the job history module is able to gracefully
 * ignore events after the event writer is closed
 *
 */
public class TestJobHistoryParsing  extends TestCase {

  public void testHistoryParsing() throws IOException {
    // open a test history file
    Path historyDir = new Path(System.getProperty("test.build.data", "."),
                                "history");
    JobConf conf = new JobConf();
    conf.set("hadoop.job.history.location", historyDir.toString());
    FileSystem fs = FileSystem.getLocal(new JobConf());

    // Some weird strings
    String username = "user";
    String weirdJob = "Value has \n new line \n and " +
                    "dot followed by new line .\n in it +" +
                    "ends with escape\\";
    String weirdPath = "Value has characters: " +
                    "`1234567890-=qwertyuiop[]\\asdfghjkl;'zxcvbnm,./" +
                    "~!@#$%^&*()_+QWERTYUIOP{}|ASDFGHJKL:\"'ZXCVBNM<>?" +
                    "\t\b\n\f\"\n in it";

    conf.setUser(username);

    MiniMRCluster mr = null;
    mr = new MiniMRCluster(2, "file:///", 3, null, null, conf);

    JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
    JobHistory jh = jt.getJobHistory();

    jh.init(jt, conf, "localhost", 1234);
    JobID jobId = JobID.forName("job_200809171136_0001");
    jh.setupEventWriter(jobId, conf);
    JobSubmittedEvent jse =
      new JobSubmittedEvent(jobId, weirdJob, username, 12345, weirdPath);
    jh.logEvent(jse, jobId);

    JobFinishedEvent jfe =
      new JobFinishedEvent(jobId, 12346, 1, 1, 0, 0, new Counters(),
          new Counters(), new Counters());
    jh.logEvent(jfe, jobId);
    jh.closeWriter(jobId);

    // Try to write one more event now, should not fail
    TaskID tid = TaskID.forName("task_200809171136_0001_m_000002");
    TaskFinishedEvent tfe =
      new TaskFinishedEvent(tid, 0, TaskType.MAP, "", null);
    boolean caughtException = false;

    try {
      jh.logEvent(tfe, jobId);
    } catch (Exception e) {
      caughtException = true;
    }

    assertFalse("Writing an event after closing event writer is not handled",
        caughtException);

    String historyFileName = jobId.toString() + "_" + username;
    Path historyFilePath = new Path (historyDir.toString(),
      historyFileName);

    System.out.println("History File is " + historyFilePath.toString());

    JobHistoryParser parser =
      new JobHistoryParser(fs, historyFilePath);

    JobHistoryParser.JobInfo jobInfo = parser.parse();

    assertTrue (jobInfo.getUsername().equals(username));
    assertTrue(jobInfo.getJobname().equals(weirdJob));
    assertTrue(jobInfo.getJobConfPath().equals(weirdPath));

    if (mr != null) {
      mr.shutdown();
    }
  }
}
