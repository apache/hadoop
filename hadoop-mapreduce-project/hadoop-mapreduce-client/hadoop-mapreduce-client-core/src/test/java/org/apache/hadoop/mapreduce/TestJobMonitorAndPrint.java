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

package org.apache.hadoop.mapreduce;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.logging.LogCapturer;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

/**
 * Test to make sure that command line output for 
 * job monitoring is correct and prints 100% for map and reduce before 
 * successful completion.
 */
public class TestJobMonitorAndPrint {
  private Job job;
  private Configuration conf;
  private ClientProtocol clientProtocol;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    clientProtocol = mock(ClientProtocol.class);
    Cluster cluster = mock(Cluster.class);
    when(cluster.getConf()).thenReturn(conf);
    when(cluster.getClient()).thenReturn(clientProtocol);
    JobStatus jobStatus = new JobStatus(new JobID("job_000", 1), 0f, 0f, 0f, 0f, 
        State.RUNNING, JobPriority.HIGH, "tmp-user", "tmp-jobname", 
        "tmp-jobfile", "tmp-url");
    job = Job.getInstance(cluster, jobStatus, conf);
    job = spy(job);
  }

  @Test
  public void testJobMonitorAndPrint() throws Exception {
    LogCapturer logCapturer = LogCapturer.captureLogs(LoggerFactory.getLogger(Job.class));
    try {
      JobStatus jobStatus_1 =
          new JobStatus(new JobID("job_000", 1), 1f, 0.1f, 0.1f, 0f, State.RUNNING,
              JobPriority.HIGH, "tmp-user", "tmp-jobname", "tmp-queue", "tmp-jobfile", "tmp-url",
              true);
      JobStatus jobStatus_2 =
          new JobStatus(new JobID("job_000", 1), 1f, 1f, 1f, 1f, State.SUCCEEDED, JobPriority.HIGH,
              "tmp-user", "tmp-jobname", "tmp-queue", "tmp-jobfile", "tmp-url", true);

      doAnswer((Answer<TaskCompletionEvent[]>) invocation -> TaskCompletionEvent.EMPTY_ARRAY).when(
          job).getTaskCompletionEvents(anyInt(), anyInt());

      doReturn(new TaskReport[5]).when(job).getTaskReports(isA(TaskType.class));
      when(clientProtocol.getJobStatus(any(JobID.class))).thenReturn(jobStatus_1, jobStatus_2);

      job.monitorAndPrintJob();

      boolean foundHundred = false;
      boolean foundComplete = false;
      boolean foundUber = false;
      String uberModeMatch = "uber mode : true";
      String progressMatch = "map 100% reduce 100%";
      String completionMatch = "completed successfully";
      for (String logLine : logCapturer.getOutput().split("\n")) {
        if (logLine.contains(uberModeMatch)) {
          foundUber = true;
        }
        if (logLine.contains(progressMatch)) {
          foundHundred = true;
        }
        if (logLine.contains(completionMatch)) {
          foundComplete = true;
        }
        if (foundUber && foundHundred && foundComplete) {
          break;
        }
      }
      assertTrue(foundUber);
      assertTrue(foundHundred);
      assertTrue(foundComplete);

      System.out.println("The output of job.toString() is : \n" + job.toString());
      assertTrue(job.toString().contains("Number of maps: 5\n"));
      assertTrue(job.toString().contains("Number of reduces: 5\n"));
    } finally {
      logCapturer.stopCapturing();
    }
  }
}
