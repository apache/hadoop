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
package org.apache.hadoop.mapreduce.jobhistory;

import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Assert;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.TimeZone;

public class TestHistoryViewerPrinter {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestHistoryViewerPrinter.class);

  private final String LINE_SEPARATOR = System.lineSeparator();

  @Test
  public void testHumanPrinter() throws Exception {
    JobHistoryParser.JobInfo job = createJobInfo();
    HumanReadableHistoryViewerPrinter printer =
        new HumanReadableHistoryViewerPrinter(job, false, "http://",
            TimeZone.getTimeZone("GMT"));
    String outStr = run(printer);
    Assert.assertEquals("\n" +
        "Hadoop job: job_1317928501754_0001\n" +
        "=====================================\n" +
        "User: rkanter\n" +
        "JobName: my job\n" +
        "JobConf: /tmp/job.xml\n" +
        "Submitted At: 6-Oct-2011 19:15:01\n" +
        "Launched At: 6-Oct-2011 19:15:02 (1sec)\n" +
        "Finished At: 6-Oct-2011 19:15:16 (14sec)\n" +
        "Status: SUCCEEDED\n" +
        "Counters: \n" +
        "\n" +
        "|Group Name                    |Counter name                  |Map Value |Reduce Value|Total Value|\n" +
        "---------------------------------------------------------------------------------------" +
        LINE_SEPARATOR +
        "|group1                        |counter1                      |5         |5         |5         " +
        LINE_SEPARATOR +
        "|group1                        |counter2                      |10        |10        |10        " +
        LINE_SEPARATOR +
        "|group2                        |counter1                      |15        |15        |15        " +
        "\n\n" +
        "=====================================" +
        LINE_SEPARATOR +
        "\n" +
        "Task Summary\n" +
        "============================\n" +
        "Kind\tTotal\tSuccessful\tFailed\tKilled\tStartTime\tFinishTime\n" +
        "\n" +
        "Setup\t1\t1\t\t0\t0\t6-Oct-2011 19:15:03\t6-Oct-2011 19:15:04 (1sec)\n" +
        "Map\t6\t5\t\t1\t0\t6-Oct-2011 19:15:04\t6-Oct-2011 19:15:16 (12sec)\n" +
        "Reduce\t1\t1\t\t0\t0\t6-Oct-2011 19:15:10\t6-Oct-2011 19:15:18 (8sec)\n" +
        "Cleanup\t1\t1\t\t0\t0\t6-Oct-2011 19:15:11\t6-Oct-2011 19:15:20 (9sec)\n" +
        "============================\n" +
        LINE_SEPARATOR +
        "\n" +
        "Analysis" +
        LINE_SEPARATOR +
        "=========" +
        LINE_SEPARATOR +
        "\n" +
        "Time taken by best performing map task task_1317928501754_0001_m_000003: 3sec\n" +
        "Average time taken by map tasks: 5sec\n" +
        "Worse performing map tasks: \n" +
        "TaskId\t\tTimetaken" +
        LINE_SEPARATOR +
        "task_1317928501754_0001_m_000007 7sec" +
        LINE_SEPARATOR +
        "task_1317928501754_0001_m_000006 6sec" +
        LINE_SEPARATOR +
        "task_1317928501754_0001_m_000005 5sec" +
        LINE_SEPARATOR +
        "task_1317928501754_0001_m_000004 4sec" +
        LINE_SEPARATOR +
        "task_1317928501754_0001_m_000003 3sec" +
        LINE_SEPARATOR +
        "The last map task task_1317928501754_0001_m_000007 finished at (relative to the Job launch time): 6-Oct-2011 19:15:16 (14sec)" +
        LINE_SEPARATOR +
        "\n" +
        "Time taken by best performing shuffle task task_1317928501754_0001_r_000008: 8sec\n" +
        "Average time taken by shuffle tasks: 8sec\n" +
        "Worse performing shuffle tasks: \n" +
        "TaskId\t\tTimetaken" +
        LINE_SEPARATOR +
        "task_1317928501754_0001_r_000008 8sec" +
        LINE_SEPARATOR +
        "The last shuffle task task_1317928501754_0001_r_000008 finished at (relative to the Job launch time): 6-Oct-2011 19:15:18 (16sec)" +
        LINE_SEPARATOR +
        "\n" +
        "Time taken by best performing reduce task task_1317928501754_0001_r_000008: 0sec\n" +
        "Average time taken by reduce tasks: 0sec\n" +
        "Worse performing reduce tasks: \n" +
        "TaskId\t\tTimetaken"+
        LINE_SEPARATOR  +
        "task_1317928501754_0001_r_000008 0sec" +
        LINE_SEPARATOR +
        "The last reduce task task_1317928501754_0001_r_000008 finished at (relative to the Job launch time): 6-Oct-2011 19:15:18 (16sec)" +
        LINE_SEPARATOR +
        "=========" +
        LINE_SEPARATOR +
        "\n" +
        "FAILED MAP task list for job_1317928501754_0001\n" +
        "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
        "====================================================" +
        LINE_SEPARATOR +
        "task_1317928501754_0001_m_000002\t6-Oct-2011 19:15:04\t6-Oct-2011 19:15:06 (2sec)\t\t" +
        LINE_SEPARATOR +
        "\n" +
        "FAILED task attempts by nodes\n" +
        "Hostname\tFailedTasks\n" +
        "===============================" +
        LINE_SEPARATOR +
        "localhost\ttask_1317928501754_0001_m_000002, " +
        LINE_SEPARATOR, outStr);
  }

  @Test
  public void testHumanPrinterAll() throws Exception {
    JobHistoryParser.JobInfo job = createJobInfo();
    HumanReadableHistoryViewerPrinter printer =
        new HumanReadableHistoryViewerPrinter(job, true, "http://",
            TimeZone.getTimeZone("GMT"));
    String outStr = run(printer);
    if (System.getProperty("java.version").startsWith("1.7")) {
      Assert.assertEquals("\n" +
          "Hadoop job: job_1317928501754_0001\n" +
          "=====================================\n" +
          "User: rkanter\n" +
          "JobName: my job\n" +
          "JobConf: /tmp/job.xml\n" +
          "Submitted At: 6-Oct-2011 19:15:01\n" +
          "Launched At: 6-Oct-2011 19:15:02 (1sec)\n" +
          "Finished At: 6-Oct-2011 19:15:16 (14sec)\n" +
          "Status: SUCCEEDED\n" +
          "Counters: \n" +
          "\n" +
          "|Group Name                    |Counter name                  |Map Value |Reduce Value|Total Value|\n" +
          "---------------------------------------------------------------------------------------" +
          LINE_SEPARATOR +
          "|group1                        |counter1                      |5         |5         |5         " +
          LINE_SEPARATOR +
          "|group1                        |counter2                      |10        |10        |10        " +
          LINE_SEPARATOR +
          "|group2                        |counter1                      |15        |15        |15        \n" +
          "\n" +
          "=====================================" +
          LINE_SEPARATOR +
          "\n" +
          "Task Summary\n" +
          "============================\n" +
          "Kind\tTotal\tSuccessful\tFailed\tKilled\tStartTime\tFinishTime\n" +
          "\n" +
          "Setup\t1\t1\t\t0\t0\t6-Oct-2011 19:15:03\t6-Oct-2011 19:15:04 (1sec)\n" +
          "Map\t6\t5\t\t1\t0\t6-Oct-2011 19:15:04\t6-Oct-2011 19:15:16 (12sec)\n" +
          "Reduce\t1\t1\t\t0\t0\t6-Oct-2011 19:15:10\t6-Oct-2011 19:15:18 (8sec)\n" +
          "Cleanup\t1\t1\t\t0\t0\t6-Oct-2011 19:15:11\t6-Oct-2011 19:15:20 (9sec)\n" +
          "============================\n" +
          LINE_SEPARATOR +
          "\n" +
          "Analysis" +
          LINE_SEPARATOR +
          "=========" +
          LINE_SEPARATOR +
          "\n" +
          "Time taken by best performing map task task_1317928501754_0001_m_000003: 3sec\n" +
          "Average time taken by map tasks: 5sec\n" +
          "Worse performing map tasks: \n" +
          "TaskId\t\tTimetaken" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000007 7sec" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000006 6sec" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000005 5sec" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000004 4sec" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000003 3sec" +
          LINE_SEPARATOR +
          "The last map task task_1317928501754_0001_m_000007 finished at (relative to the Job launch time): 6-Oct-2011 19:15:16 (14sec)" +
          LINE_SEPARATOR +
          "\n" +
          "Time taken by best performing shuffle task task_1317928501754_0001_r_000008: 8sec\n" +
          "Average time taken by shuffle tasks: 8sec\n" +
          "Worse performing shuffle tasks: \n" +
          "TaskId\t\tTimetaken" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_r_000008 8sec" +
          LINE_SEPARATOR +
          "The last shuffle task task_1317928501754_0001_r_000008 finished at (relative to the Job launch time): 6-Oct-2011 19:15:18 (16sec)" +
          LINE_SEPARATOR +
          "\n" +
          "Time taken by best performing reduce task task_1317928501754_0001_r_000008: 0sec\n" +
          "Average time taken by reduce tasks: 0sec\n" +
          "Worse performing reduce tasks: \n" +
          "TaskId\t\tTimetaken" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_r_000008 0sec" +
          LINE_SEPARATOR +
          "The last reduce task task_1317928501754_0001_r_000008 finished at (relative to the Job launch time): 6-Oct-2011 19:15:18 (16sec)" +
          LINE_SEPARATOR +
          "=========" +
          LINE_SEPARATOR +
          "\n" +
          "FAILED MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000002\t6-Oct-2011 19:15:04\t6-Oct-2011 19:15:06 (2sec)\t\t" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED JOB_SETUP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_s_000001\t6-Oct-2011 19:15:03\t6-Oct-2011 19:15:04 (1sec)\t" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000006\t6-Oct-2011 19:15:08\t6-Oct-2011 19:15:14 (6sec)\t\t\n" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000005\t6-Oct-2011 19:15:07\t6-Oct-2011 19:15:12 (5sec)\t\t\n" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000004\t6-Oct-2011 19:15:06\t6-Oct-2011 19:15:10 (4sec)\t\t\n" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000003\t6-Oct-2011 19:15:05\t6-Oct-2011 19:15:08 (3sec)\t\t\n" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000007\t6-Oct-2011 19:15:09\t6-Oct-2011 19:15:16 (7sec)\t\t\n" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED REDUCE task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_r_000008\t6-Oct-2011 19:15:10\t6-Oct-2011 19:15:18 (8sec)\t" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED JOB_CLEANUP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_c_000009\t6-Oct-2011 19:15:11\t6-Oct-2011 19:15:20 (9sec)\t" +
          LINE_SEPARATOR +
          "\n" +
          "JOB_SETUP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tHostName\tError\tTaskLogs\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_s_000001_1\t6-Oct-2011 19:15:03\t6-Oct-2011 19:15:04 (1sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_s_000001_1" +
          LINE_SEPARATOR +
          "\n" +
          "MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tHostName\tError\tTaskLogs\n" +
          "====================================================\n" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_m_000002_1\t6-Oct-2011 19:15:04\t6-Oct-2011 19:15:06 (2sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000002_1\n" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_m_000006_1\t6-Oct-2011 19:15:08\t6-Oct-2011 19:15:14 (6sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000006_1\n" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_m_000005_1\t6-Oct-2011 19:15:07\t6-Oct-2011 19:15:12 (5sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000005_1\n" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_m_000004_1\t6-Oct-2011 19:15:06\t6-Oct-2011 19:15:10 (4sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000004_1\n" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_m_000003_1\t6-Oct-2011 19:15:05\t6-Oct-2011 19:15:08 (3sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000003_1\n" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_m_000007_1\t6-Oct-2011 19:15:09\t6-Oct-2011 19:15:16 (7sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000007_1\n" +
          LINE_SEPARATOR +
          "\n" +
          "REDUCE task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tShuffleFinished\tSortFinished\tFinishTime\tHostName\tError\tTaskLogs\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_r_000008_1\t6-Oct-2011 19:15:10\t6-Oct-2011 19:15:18 (8sec)\t6-Oct-2011 19:15:18 (0sec)6-Oct-2011 19:15:18 (8sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_r_000008_1" +
          LINE_SEPARATOR +
          "\n" +
          "JOB_CLEANUP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tHostName\tError\tTaskLogs\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_c_000009_1\t6-Oct-2011 19:15:11\t6-Oct-2011 19:15:20 (9sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_c_000009_1" +
          LINE_SEPARATOR +
          "\n" +
          "FAILED task attempts by nodes\n" +
          "Hostname\tFailedTasks\n" +
          "===============================" +
          LINE_SEPARATOR +
          "localhost\ttask_1317928501754_0001_m_000002, " +
          LINE_SEPARATOR, outStr);
    } else {
      Assert.assertEquals("\n" +
          "Hadoop job: job_1317928501754_0001\n" +
          "=====================================\n" +
          "User: rkanter\n" +
          "JobName: my job\n" +
          "JobConf: /tmp/job.xml\n" +
          "Submitted At: 6-Oct-2011 19:15:01\n" +
          "Launched At: 6-Oct-2011 19:15:02 (1sec)\n" +
          "Finished At: 6-Oct-2011 19:15:16 (14sec)\n" +
          "Status: SUCCEEDED\n" +
          "Counters: \n" +
          "\n" +
          "|Group Name                    |Counter name                  |Map Value |Reduce Value|Total Value|\n" +
          "---------------------------------------------------------------------------------------" +
          LINE_SEPARATOR +
          "|group1                        |counter1                      |5         |5         |5         " +
          LINE_SEPARATOR +
          "|group1                        |counter2                      |10        |10        |10        " +
          LINE_SEPARATOR +
          "|group2                        |counter1                      |15        |15        |15        \n" +
          "\n" +
          "=====================================" +
          LINE_SEPARATOR +
          "\n" +
          "Task Summary\n" +
          "============================\n" +
          "Kind\tTotal\tSuccessful\tFailed\tKilled\tStartTime\tFinishTime\n" +
          "\n" +
          "Setup\t1\t1\t\t0\t0\t6-Oct-2011 19:15:03\t6-Oct-2011 19:15:04 (1sec)\n" +
          "Map\t6\t5\t\t1\t0\t6-Oct-2011 19:15:04\t6-Oct-2011 19:15:16 (12sec)\n" +
          "Reduce\t1\t1\t\t0\t0\t6-Oct-2011 19:15:10\t6-Oct-2011 19:15:18 (8sec)\n" +
          "Cleanup\t1\t1\t\t0\t0\t6-Oct-2011 19:15:11\t6-Oct-2011 19:15:20 (9sec)\n" +
          "============================\n" +
          LINE_SEPARATOR +
          "\n" +
          "Analysis" +
          LINE_SEPARATOR +
          "=========" +
          LINE_SEPARATOR +
          "\n" +
          "Time taken by best performing map task task_1317928501754_0001_m_000003: 3sec\n" +
          "Average time taken by map tasks: 5sec\n" +
          "Worse performing map tasks: \n" +
          "TaskId\t\tTimetaken" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000007 7sec" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000006 6sec" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000005 5sec" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000004 4sec" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000003 3sec" +
          LINE_SEPARATOR +
          "The last map task task_1317928501754_0001_m_000007 finished at (relative to the Job launch time): 6-Oct-2011 19:15:16 (14sec)" +
          LINE_SEPARATOR +
          "\n" +
          "Time taken by best performing shuffle task task_1317928501754_0001_r_000008: 8sec\n" +
          "Average time taken by shuffle tasks: 8sec\n" +
          "Worse performing shuffle tasks: \n" +
          "TaskId\t\tTimetaken" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_r_000008 8sec" +
          LINE_SEPARATOR +
          "The last shuffle task task_1317928501754_0001_r_000008 finished at (relative to the Job launch time): 6-Oct-2011 19:15:18 (16sec)" +
          LINE_SEPARATOR +
          "\n" +
          "Time taken by best performing reduce task task_1317928501754_0001_r_000008: 0sec\n" +
          "Average time taken by reduce tasks: 0sec\n" +
          "Worse performing reduce tasks: \n" +
          "TaskId\t\tTimetaken" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_r_000008 0sec" +
          LINE_SEPARATOR +
          "The last reduce task task_1317928501754_0001_r_000008 finished at (relative to the Job launch time): 6-Oct-2011 19:15:18 (16sec)" +
          LINE_SEPARATOR +
          "=========" +
          LINE_SEPARATOR +
          "\n" +
          "FAILED MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000002\t6-Oct-2011 19:15:04\t6-Oct-2011 19:15:06 (2sec)\t\t" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED JOB_SETUP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_s_000001\t6-Oct-2011 19:15:03\t6-Oct-2011 19:15:04 (1sec)\t" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000007\t6-Oct-2011 19:15:09\t6-Oct-2011 19:15:16 (7sec)\t\t" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000006\t6-Oct-2011 19:15:08\t6-Oct-2011 19:15:14 (6sec)\t\t" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000005\t6-Oct-2011 19:15:07\t6-Oct-2011 19:15:12 (5sec)\t\t" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000004\t6-Oct-2011 19:15:06\t6-Oct-2011 19:15:10 (4sec)\t\t" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\tInputSplits\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_m_000003\t6-Oct-2011 19:15:05\t6-Oct-2011 19:15:08 (3sec)\t\t" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED REDUCE task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_r_000008\t6-Oct-2011 19:15:10\t6-Oct-2011 19:15:18 (8sec)\t" +
          LINE_SEPARATOR +
          "\n" +
          "SUCCEEDED JOB_CLEANUP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tError\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "task_1317928501754_0001_c_000009\t6-Oct-2011 19:15:11\t6-Oct-2011 19:15:20 (9sec)\t" +
          LINE_SEPARATOR +
          "\n" +
          "JOB_SETUP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tHostName\tError\tTaskLogs\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_s_000001_1\t6-Oct-2011 19:15:03\t6-Oct-2011 19:15:04 (1sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_s_000001_1" +
          LINE_SEPARATOR +
          "\n" +
          "MAP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tHostName\tError\tTaskLogs\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_m_000007_1\t6-Oct-2011 19:15:09\t6-Oct-2011 19:15:16 (7sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000007_1" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_m_000002_1\t6-Oct-2011 19:15:04\t6-Oct-2011 19:15:06 (2sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000002_1" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_m_000006_1\t6-Oct-2011 19:15:08\t6-Oct-2011 19:15:14 (6sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000006_1" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_m_000005_1\t6-Oct-2011 19:15:07\t6-Oct-2011 19:15:12 (5sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000005_1" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_m_000004_1\t6-Oct-2011 19:15:06\t6-Oct-2011 19:15:10 (4sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000004_1" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_m_000003_1\t6-Oct-2011 19:15:05\t6-Oct-2011 19:15:08 (3sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000003_1" +
          LINE_SEPARATOR +
          "\n" +
          "REDUCE task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tShuffleFinished\tSortFinished\tFinishTime\tHostName\tError\tTaskLogs\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_r_000008_1\t6-Oct-2011 19:15:10\t6-Oct-2011 19:15:18 (8sec)\t6-Oct-2011 19:15:18 (0sec)6-Oct-2011 19:15:18 (8sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_r_000008_1" +
          LINE_SEPARATOR +
          "\n" +
          "JOB_CLEANUP task list for job_1317928501754_0001\n" +
          "TaskId\t\tStartTime\tFinishTime\tHostName\tError\tTaskLogs\n" +
          "====================================================" +
          LINE_SEPARATOR +
          "attempt_1317928501754_0001_c_000009_1\t6-Oct-2011 19:15:11\t6-Oct-2011 19:15:20 (9sec)\tlocalhost\thttp://t:1234/tasklog?attemptid=attempt_1317928501754_0001_c_000009_1" +
          LINE_SEPARATOR +
          "\n" +
          "FAILED task attempts by nodes\n" +
          "Hostname\tFailedTasks\n" +
          "===============================" +
          LINE_SEPARATOR +
          "localhost\ttask_1317928501754_0001_m_000002, " +
          LINE_SEPARATOR, outStr);
    }
  }

  @Test
  public void testJSONPrinter() throws Exception {
    JobHistoryParser.JobInfo job = createJobInfo();
    JSONHistoryViewerPrinter printer =
        new JSONHistoryViewerPrinter(job, false, "http://");
    String outStr = run(printer);
    JSONAssert.assertEquals("{\n" +
        "    \"counters\": {\n" +
        "        \"group1\": [\n" +
        "            {\n" +
        "                \"counterName\": \"counter1\",\n" +
        "                \"mapValue\": 5,\n" +
        "                \"reduceValue\": 5,\n" +
        "                \"totalValue\": 5\n" +
        "            },\n" +
        "            {\n" +
        "                \"counterName\": \"counter2\",\n" +
        "                \"mapValue\": 10,\n" +
        "                \"reduceValue\": 10,\n" +
        "                \"totalValue\": 10\n" +
        "            }\n" +
        "        ],\n" +
        "        \"group2\": [\n" +
        "            {\n" +
        "                \"counterName\": \"counter1\",\n" +
        "                \"mapValue\": 15,\n" +
        "                \"reduceValue\": 15,\n" +
        "                \"totalValue\": 15\n" +
        "            }\n" +
        "        ]\n" +
        "    },\n" +
        "    \"finishedAt\": 1317928516754,\n" +
        "    \"hadoopJob\": \"job_1317928501754_0001\",\n" +
        "    \"jobConf\": \"/tmp/job.xml\",\n" +
        "    \"jobName\": \"my job\",\n" +
        "    \"launchedAt\": 1317928502754,\n" +
        "    \"status\": \"SUCCEEDED\",\n" +
        "    \"submittedAt\": 1317928501754,\n" +
        "    \"taskSummary\": {\n" +
        "        \"cleanup\": {\n" +
        "            \"failed\": 0,\n" +
        "            \"finishTime\": 1317928520754,\n" +
        "            \"killed\": 0,\n" +
        "            \"startTime\": 1317928511754,\n" +
        "            \"successful\": 1,\n" +
        "            \"total\": 1\n" +
        "        },\n" +
        "        \"map\": {\n" +
        "            \"failed\": 1,\n" +
        "            \"finishTime\": 1317928516754,\n" +
        "            \"killed\": 0,\n" +
        "            \"startTime\": 1317928504754,\n" +
        "            \"successful\": 5,\n" +
        "            \"total\": 6\n" +
        "        },\n" +
        "        \"reduce\": {\n" +
        "            \"failed\": 0,\n" +
        "            \"finishTime\": 1317928518754,\n" +
        "            \"killed\": 0,\n" +
        "            \"startTime\": 1317928510754,\n" +
        "            \"successful\": 1,\n" +
        "            \"total\": 1\n" +
        "        },\n" +
        "        \"setup\": {\n" +
        "            \"failed\": 0,\n" +
        "            \"finishTime\": 1317928504754,\n" +
        "            \"killed\": 0,\n" +
        "            \"startTime\": 1317928503754,\n" +
        "            \"successful\": 1,\n" +
        "            \"total\": 1\n" +
        "        }\n" +
        "    },\n" +
        "    \"tasks\": [\n" +
        "        {\n" +
        "            \"finishTime\": 1317928506754,\n" +
        "            \"inputSplits\": \"\",\n" +
        "            \"startTime\": 1317928504754,\n" +
        "            \"status\": \"FAILED\",\n" +
        "            \"taskId\": \"task_1317928501754_0001_m_000002\",\n" +
        "            \"type\": \"MAP\"\n" +
        "        }\n" +
        "    ],\n" +
        "    \"user\": \"rkanter\"\n" +
        "}\n", outStr, JSONCompareMode.NON_EXTENSIBLE);
  }

  @Test
  public void testJSONPrinterAll() throws Exception {
    JobHistoryParser.JobInfo job = createJobInfo();
    JSONHistoryViewerPrinter printer =
        new JSONHistoryViewerPrinter(job, true, "http://");
    String outStr = run(printer);
    JSONAssert.assertEquals("{\n" +
        "    \"counters\": {\n" +
        "        \"group1\": [\n" +
        "            {\n" +
        "                \"counterName\": \"counter1\",\n" +
        "                \"mapValue\": 5,\n" +
        "                \"reduceValue\": 5,\n" +
        "                \"totalValue\": 5\n" +
        "            },\n" +
        "            {\n" +
        "                \"counterName\": \"counter2\",\n" +
        "                \"mapValue\": 10,\n" +
        "                \"reduceValue\": 10,\n" +
        "                \"totalValue\": 10\n" +
        "            }\n" +
        "        ],\n" +
        "        \"group2\": [\n" +
        "            {\n" +
        "                \"counterName\": \"counter1\",\n" +
        "                \"mapValue\": 15,\n" +
        "                \"reduceValue\": 15,\n" +
        "                \"totalValue\": 15\n" +
        "            }\n" +
        "        ]\n" +
        "    },\n" +
        "    \"finishedAt\": 1317928516754,\n" +
        "    \"hadoopJob\": \"job_1317928501754_0001\",\n" +
        "    \"jobConf\": \"/tmp/job.xml\",\n" +
        "    \"jobName\": \"my job\",\n" +
        "    \"launchedAt\": 1317928502754,\n" +
        "    \"status\": \"SUCCEEDED\",\n" +
        "    \"submittedAt\": 1317928501754,\n" +
        "    \"taskSummary\": {\n" +
        "        \"cleanup\": {\n" +
        "            \"failed\": 0,\n" +
        "            \"finishTime\": 1317928520754,\n" +
        "            \"killed\": 0,\n" +
        "            \"startTime\": 1317928511754,\n" +
        "            \"successful\": 1,\n" +
        "            \"total\": 1\n" +
        "        },\n" +
        "        \"map\": {\n" +
        "            \"failed\": 1,\n" +
        "            \"finishTime\": 1317928516754,\n" +
        "            \"killed\": 0,\n" +
        "            \"startTime\": 1317928504754,\n" +
        "            \"successful\": 5,\n" +
        "            \"total\": 6\n" +
        "        },\n" +
        "        \"reduce\": {\n" +
        "            \"failed\": 0,\n" +
        "            \"finishTime\": 1317928518754,\n" +
        "            \"killed\": 0,\n" +
        "            \"startTime\": 1317928510754,\n" +
        "            \"successful\": 1,\n" +
        "            \"total\": 1\n" +
        "        },\n" +
        "        \"setup\": {\n" +
        "            \"failed\": 0,\n" +
        "            \"finishTime\": 1317928504754,\n" +
        "            \"killed\": 0,\n" +
        "            \"startTime\": 1317928503754,\n" +
        "            \"successful\": 1,\n" +
        "            \"total\": 1\n" +
        "        }\n" +
        "    },\n" +
        "    \"tasks\": [\n" +
        "        {\n" +
        "            \"attempts\": {\n" +
        "                \"attemptId\": \"attempt_1317928501754_0001_m_000002_1\",\n" +
        "                \"finishTime\": 1317928506754,\n" +
        "                \"hostName\": \"localhost\",\n" +
        "                \"startTime\": 1317928504754,\n" +
        "                \"taskLogs\": \"http://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000002_1\"\n" +
        "            },\n" +
        "            \"counters\": {\n" +
        "                \"group1\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 5\n" +
        "                    },\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter2\",\n" +
        "                        \"value\": 10\n" +
        "                    }\n" +
        "                ],\n" +
        "                \"group2\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 15\n" +
        "                    }\n" +
        "                ]\n" +
        "            },\n" +
        "            \"finishTime\": 1317928506754,\n" +
        "            \"inputSplits\": \"\",\n" +
        "            \"startTime\": 1317928504754,\n" +
        "            \"status\": \"FAILED\",\n" +
        "            \"taskId\": \"task_1317928501754_0001_m_000002\",\n" +
        "            \"type\": \"MAP\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"attempts\": {\n" +
        "                \"attemptId\": \"attempt_1317928501754_0001_s_000001_1\",\n" +
        "                \"finishTime\": 1317928504754,\n" +
        "                \"hostName\": \"localhost\",\n" +
        "                \"startTime\": 1317928503754,\n" +
        "                \"taskLogs\": \"http://t:1234/tasklog?attemptid=attempt_1317928501754_0001_s_000001_1\"\n" +
        "            },\n" +
        "            \"counters\": {\n" +
        "                \"group1\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 5\n" +
        "                    },\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter2\",\n" +
        "                        \"value\": 10\n" +
        "                    }\n" +
        "                ],\n" +
        "                \"group2\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 15\n" +
        "                    }\n" +
        "                ]\n" +
        "            },\n" +
        "            \"finishTime\": 1317928504754,\n" +
        "            \"startTime\": 1317928503754,\n" +
        "            \"status\": \"SUCCEEDED\",\n" +
        "            \"taskId\": \"task_1317928501754_0001_s_000001\",\n" +
        "            \"type\": \"JOB_SETUP\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"attempts\": {\n" +
        "                \"attemptId\": \"attempt_1317928501754_0001_m_000006_1\",\n" +
        "                \"finishTime\": 1317928514754,\n" +
        "                \"hostName\": \"localhost\",\n" +
        "                \"startTime\": 1317928508754,\n" +
        "                \"taskLogs\": \"http://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000006_1\"\n" +
        "            },\n" +
        "            \"counters\": {\n" +
        "                \"group1\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 5\n" +
        "                    },\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter2\",\n" +
        "                        \"value\": 10\n" +
        "                    }\n" +
        "                ],\n" +
        "                \"group2\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 15\n" +
        "                    }\n" +
        "                ]\n" +
        "            },\n" +
        "            \"finishTime\": 1317928514754,\n" +
        "            \"inputSplits\": \"\",\n" +
        "            \"startTime\": 1317928508754,\n" +
        "            \"status\": \"SUCCEEDED\",\n" +
        "            \"taskId\": \"task_1317928501754_0001_m_000006\",\n" +
        "            \"type\": \"MAP\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"attempts\": {\n" +
        "                \"attemptId\": \"attempt_1317928501754_0001_m_000005_1\",\n" +
        "                \"finishTime\": 1317928512754,\n" +
        "                \"hostName\": \"localhost\",\n" +
        "                \"startTime\": 1317928507754,\n" +
        "                \"taskLogs\": \"http://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000005_1\"\n" +
        "            },\n" +
        "            \"counters\": {\n" +
        "                \"group1\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 5\n" +
        "                    },\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter2\",\n" +
        "                        \"value\": 10\n" +
        "                    }\n" +
        "                ],\n" +
        "                \"group2\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 15\n" +
        "                    }\n" +
        "                ]\n" +
        "            },\n" +
        "            \"finishTime\": 1317928512754,\n" +
        "            \"inputSplits\": \"\",\n" +
        "            \"startTime\": 1317928507754,\n" +
        "            \"status\": \"SUCCEEDED\",\n" +
        "            \"taskId\": \"task_1317928501754_0001_m_000005\",\n" +
        "            \"type\": \"MAP\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"attempts\": {\n" +
        "                \"attemptId\": \"attempt_1317928501754_0001_m_000004_1\",\n" +
        "                \"finishTime\": 1317928510754,\n" +
        "                \"hostName\": \"localhost\",\n" +
        "                \"startTime\": 1317928506754,\n" +
        "                \"taskLogs\": \"http://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000004_1\"\n" +
        "            },\n" +
        "            \"counters\": {\n" +
        "                \"group1\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 5\n" +
        "                    },\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter2\",\n" +
        "                        \"value\": 10\n" +
        "                    }\n" +
        "                ],\n" +
        "                \"group2\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 15\n" +
        "                    }\n" +
        "                ]\n" +
        "            },\n" +
        "            \"finishTime\": 1317928510754,\n" +
        "            \"inputSplits\": \"\",\n" +
        "            \"startTime\": 1317928506754,\n" +
        "            \"status\": \"SUCCEEDED\",\n" +
        "            \"taskId\": \"task_1317928501754_0001_m_000004\",\n" +
        "            \"type\": \"MAP\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"attempts\": {\n" +
        "                \"attemptId\": \"attempt_1317928501754_0001_m_000003_1\",\n" +
        "                \"finishTime\": 1317928508754,\n" +
        "                \"hostName\": \"localhost\",\n" +
        "                \"startTime\": 1317928505754,\n" +
        "                \"taskLogs\": \"http://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000003_1\"\n" +
        "            },\n" +
        "            \"counters\": {\n" +
        "                \"group1\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 5\n" +
        "                    },\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter2\",\n" +
        "                        \"value\": 10\n" +
        "                    }\n" +
        "                ],\n" +
        "                \"group2\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 15\n" +
        "                    }\n" +
        "                ]\n" +
        "            },\n" +
        "            \"finishTime\": 1317928508754,\n" +
        "            \"inputSplits\": \"\",\n" +
        "            \"startTime\": 1317928505754,\n" +
        "            \"status\": \"SUCCEEDED\",\n" +
        "            \"taskId\": \"task_1317928501754_0001_m_000003\",\n" +
        "            \"type\": \"MAP\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"attempts\": {\n" +
        "                \"attemptId\": \"attempt_1317928501754_0001_c_000009_1\",\n" +
        "                \"finishTime\": 1317928520754,\n" +
        "                \"hostName\": \"localhost\",\n" +
        "                \"startTime\": 1317928511754,\n" +
        "                \"taskLogs\": \"http://t:1234/tasklog?attemptid=attempt_1317928501754_0001_c_000009_1\"\n" +
        "            },\n" +
        "            \"counters\": {\n" +
        "                \"group1\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 5\n" +
        "                    },\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter2\",\n" +
        "                        \"value\": 10\n" +
        "                    }\n" +
        "                ],\n" +
        "                \"group2\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 15\n" +
        "                    }\n" +
        "                ]\n" +
        "            },\n" +
        "            \"finishTime\": 1317928520754,\n" +
        "            \"startTime\": 1317928511754,\n" +
        "            \"status\": \"SUCCEEDED\",\n" +
        "            \"taskId\": \"task_1317928501754_0001_c_000009\",\n" +
        "            \"type\": \"JOB_CLEANUP\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"attempts\": {\n" +
        "                \"attemptId\": \"attempt_1317928501754_0001_m_000007_1\",\n" +
        "                \"finishTime\": 1317928516754,\n" +
        "                \"hostName\": \"localhost\",\n" +
        "                \"startTime\": 1317928509754,\n" +
        "                \"taskLogs\": \"http://t:1234/tasklog?attemptid=attempt_1317928501754_0001_m_000007_1\"\n" +
        "            },\n" +
        "            \"counters\": {\n" +
        "                \"group1\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 5\n" +
        "                    },\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter2\",\n" +
        "                        \"value\": 10\n" +
        "                    }\n" +
        "                ],\n" +
        "                \"group2\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 15\n" +
        "                    }\n" +
        "                ]\n" +
        "            },\n" +
        "            \"finishTime\": 1317928516754,\n" +
        "            \"inputSplits\": \"\",\n" +
        "            \"startTime\": 1317928509754,\n" +
        "            \"status\": \"SUCCEEDED\",\n" +
        "            \"taskId\": \"task_1317928501754_0001_m_000007\",\n" +
        "            \"type\": \"MAP\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"attempts\": {\n" +
        "                \"attemptId\": \"attempt_1317928501754_0001_r_000008_1\",\n" +
        "                \"finishTime\": 1317928518754,\n" +
        "                \"hostName\": \"localhost\",\n" +
        "                \"shuffleFinished\": 1317928518754,\n" +
        "                \"sortFinished\": 1317928518754,\n" +
        "                \"startTime\": 1317928510754,\n" +
        "                \"taskLogs\": \"http://t:1234/tasklog?attemptid=attempt_1317928501754_0001_r_000008_1\"\n" +
        "            },\n" +
        "            \"counters\": {\n" +
        "                \"group1\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 5\n" +
        "                    },\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter2\",\n" +
        "                        \"value\": 10\n" +
        "                    }\n" +
        "                ],\n" +
        "                \"group2\": [\n" +
        "                    {\n" +
        "                        \"counterName\": \"counter1\",\n" +
        "                        \"value\": 15\n" +
        "                    }\n" +
        "                ]\n" +
        "            },\n" +
        "            \"finishTime\": 1317928518754,\n" +
        "            \"startTime\": 1317928510754,\n" +
        "            \"status\": \"SUCCEEDED\",\n" +
        "            \"taskId\": \"task_1317928501754_0001_r_000008\",\n" +
        "            \"type\": \"REDUCE\"\n" +
        "        }\n" +
        "    ],\n" +
        "    \"user\": \"rkanter\"\n" +
        "}\n", outStr, JSONCompareMode.NON_EXTENSIBLE);
  }

  private String run(HistoryViewerPrinter printer) throws Exception {
    ByteArrayOutputStream boas = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(boas, true);
    printer.print(out);
    out.close();
    String outStr = boas.toString("UTF-8");
    LOG.info("out = " + outStr);
    return outStr;
  }

  private static JobHistoryParser.JobInfo createJobInfo() {
    JobHistoryParser.JobInfo job = new JobHistoryParser.JobInfo();
    job.submitTime = 1317928501754L;
    job.finishTime = job.submitTime + 15000;
    job.jobid = JobID.forName("job_1317928501754_0001");
    job.username = "rkanter";
    job.jobname = "my job";
    job.jobQueueName = "my queue";
    job.jobConfPath = "/tmp/job.xml";
    job.launchTime = job.submitTime + 1000;
    job.totalMaps = 5;
    job.totalReduces = 1;
    job.failedMaps = 1;
    job.failedReduces = 0;
    job.succeededMaps = 5;
    job.succeededReduces = 1;
    job.jobStatus = JobStatus.State.SUCCEEDED.name();
    job.totalCounters = createCounters();
    job.mapCounters = createCounters();
    job.reduceCounters = createCounters();
    job.tasksMap = new HashMap<>();
    addTaskInfo(job, TaskType.JOB_SETUP, 1, TaskStatus.State.SUCCEEDED);
    addTaskInfo(job, TaskType.MAP, 2, TaskStatus.State.FAILED);
    addTaskInfo(job, TaskType.MAP, 3, TaskStatus.State.SUCCEEDED);
    addTaskInfo(job, TaskType.MAP, 4, TaskStatus.State.SUCCEEDED);
    addTaskInfo(job, TaskType.MAP, 5, TaskStatus.State.SUCCEEDED);
    addTaskInfo(job, TaskType.MAP, 6, TaskStatus.State.SUCCEEDED);
    addTaskInfo(job, TaskType.MAP, 7, TaskStatus.State.SUCCEEDED);
    addTaskInfo(job, TaskType.REDUCE, 8, TaskStatus.State.SUCCEEDED);
    addTaskInfo(job, TaskType.JOB_CLEANUP, 9, TaskStatus.State.SUCCEEDED);
    return job;
  }

  private static Counters createCounters() {
    Counters counters = new Counters();
    counters.findCounter("group1", "counter1").setValue(5);
    counters.findCounter("group1", "counter2").setValue(10);
    counters.findCounter("group2", "counter1").setValue(15);
    return counters;
  }

  private static void addTaskInfo(JobHistoryParser.JobInfo job,
      TaskType type, int id, TaskStatus.State status) {
    JobHistoryParser.TaskInfo task = new JobHistoryParser.TaskInfo();
    task.taskId = new TaskID(job.getJobId(), type, id);
    task.startTime = job.getLaunchTime() + id * 1000;
    task.finishTime = task.startTime + id * 1000;
    task.taskType = type;
    task.counters = createCounters();
    task.status = status.name();
    task.attemptsMap = new HashMap<>();
    addTaskAttemptInfo(task, 1);
    job.tasksMap.put(task.getTaskId(), task);
  }

  private static void addTaskAttemptInfo(
      JobHistoryParser.TaskInfo task, int id) {
    JobHistoryParser.TaskAttemptInfo attempt =
        new JobHistoryParser.TaskAttemptInfo();
    attempt.attemptId = new TaskAttemptID(
        TaskID.downgrade(task.getTaskId()), id);
    attempt.startTime = task.getStartTime();
    attempt.finishTime = task.getFinishTime();
    attempt.shuffleFinishTime = task.getFinishTime();
    attempt.sortFinishTime = task.getFinishTime();
    attempt.mapFinishTime = task.getFinishTime();
    attempt.status = task.getTaskStatus();
    attempt.taskType = task.getTaskType();
    attempt.trackerName = "localhost";
    attempt.httpPort = 1234;
    attempt.hostname = "localhost";
    task.attemptsMap.put(attempt.getAttemptId(), attempt);
  }
}
