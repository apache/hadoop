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

package org.apache.hadoop.fs;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Job History Log Analyzer.
 *
 * @see JHLogAnalyzer
 */
public class TestJHLA {
  private static final Logger LOG =
      LoggerFactory.getLogger(JHLogAnalyzer.class);
  private String historyLog = System.getProperty("test.build.data", 
                                  "build/test/data") + "/history/test.log";

  @Before
  public void setUp() throws Exception {
    File logFile = new File(historyLog);
    if(!logFile.getParentFile().exists())
      if(!logFile.getParentFile().mkdirs())
        LOG.error("Cannot create dirs for history log file: " + historyLog);
    if(!logFile.createNewFile())
      LOG.error("Cannot create history log file: " + historyLog);
    BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(historyLog)));
    writer.write("$!!FILE=file1.log!!"); writer.newLine();
    writer.write("Meta VERSION=\"1\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0004\" JOBNAME=\"streamjob21364.jar\" USER=\"hadoop\" SUBMIT_TIME=\"1237962008012\" JOBCONF=\"hdfs:///job_200903250600_0004/job.xml\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0004\" JOB_PRIORITY=\"NORMAL\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0004\" LAUNCH_TIME=\"1237962008712\" TOTAL_MAPS=\"2\" TOTAL_REDUCES=\"0\" JOB_STATUS=\"PREP\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0004_m_000003\" TASK_TYPE=\"SETUP\" START_TIME=\"1237962008736\" SPLITS=\"\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"SETUP\" TASKID=\"task_200903250600_0004_m_000003\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000003_0\" START_TIME=\"1237962010929\" TRACKER_NAME=\"tracker_50445\" HTTP_PORT=\"50060\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"SETUP\" TASKID=\"task_200903250600_0004_m_000003\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000003_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962012459\" HOSTNAME=\"host.com\" STATE_STRING=\"setup\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0004_m_000003\" TASK_TYPE=\"SETUP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962023824\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0004\" JOB_STATUS=\"RUNNING\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0004_m_000000\" TASK_TYPE=\"MAP\" START_TIME=\"1237962024049\" SPLITS=\"host1.com,host2.com,host3.com\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0004_m_000001\" TASK_TYPE=\"MAP\" START_TIME=\"1237962024065\" SPLITS=\"host1.com,host2.com,host3.com\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0004_m_000000\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000000_0\" START_TIME=\"1237962026157\" TRACKER_NAME=\"tracker_50524\" HTTP_PORT=\"50060\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0004_m_000000\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000000_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962041307\" HOSTNAME=\"host.com\" STATE_STRING=\"Records R/W=2681/1\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(56630)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(28327)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(2681)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(28327)][(MAP_OUTPUT_RECORDS)(Map output records)(2681)]}\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0004_m_000000\" TASK_TYPE=\"MAP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962054138\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(56630)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(28327)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(2681)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(28327)][(MAP_OUTPUT_RECORDS)(Map output records)(2681)]}\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0004_m_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000001_0\" START_TIME=\"1237962026077\" TRACKER_NAME=\"tracker_50162\" HTTP_PORT=\"50060\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0004_m_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000001_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962041030\" HOSTNAME=\"host.com\" STATE_STRING=\"Records R/W=2634/1\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(28316)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(28303)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(2634)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(28303)][(MAP_OUTPUT_RECORDS)(Map output records)(2634)]}\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0004_m_000001\" TASK_TYPE=\"MAP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962054187\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(28316)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(28303)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(2634)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(28303)][(MAP_OUTPUT_RECORDS)(Map output records)(2634)]}\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0004_m_000002\" TASK_TYPE=\"CLEANUP\" START_TIME=\"1237962054187\" SPLITS=\"\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"CLEANUP\" TASKID=\"task_200903250600_0004_m_000002\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000002_0\" START_TIME=\"1237962055578\" TRACKER_NAME=\"tracker_50162\" HTTP_PORT=\"50060\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"CLEANUP\" TASKID=\"task_200903250600_0004_m_000002\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000002_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962056782\" HOSTNAME=\"host.com\" STATE_STRING=\"cleanup\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0004_m_000002\" TASK_TYPE=\"CLEANUP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962069193\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0004\" FINISH_TIME=\"1237962069193\" JOB_STATUS=\"SUCCESS\" FINISHED_MAPS=\"2\" FINISHED_REDUCES=\"0\" FAILED_MAPS=\"0\" FAILED_REDUCES=\"0\" COUNTERS=\"{(org.apache.hadoop.mapred.JobInProgress$Counter)(Job Counters )[(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(2)]}{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(84946)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(56630)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(5315)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(56630)][(MAP_OUTPUT_RECORDS)(Map output records)(5315)]}\" ."); writer.newLine();
    writer.write("$!!FILE=file2.log!!"); writer.newLine();
    writer.write("Meta VERSION=\"1\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0023\" JOBNAME=\"TestJob\" USER=\"hadoop2\" SUBMIT_TIME=\"1237964779799\" JOBCONF=\"hdfs:///job_200903250600_0023/job.xml\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0023\" JOB_PRIORITY=\"NORMAL\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0023\" LAUNCH_TIME=\"1237964780928\" TOTAL_MAPS=\"2\" TOTAL_REDUCES=\"0\" JOB_STATUS=\"PREP\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0023_r_000001\" TASK_TYPE=\"SETUP\" START_TIME=\"1237964780940\" SPLITS=\"\" ."); writer.newLine();
    writer.write("ReduceAttempt TASK_TYPE=\"SETUP\" TASKID=\"task_200903250600_0023_r_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_r_000001_0\" START_TIME=\"1237964720322\" TRACKER_NAME=\"tracker_3065\" HTTP_PORT=\"50060\" ."); writer.newLine();
    writer.write("ReduceAttempt TASK_TYPE=\"SETUP\" TASKID=\"task_200903250600_0023_r_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_r_000001_0\" TASK_STATUS=\"SUCCESS\" SHUFFLE_FINISHED=\"1237964722118\" SORT_FINISHED=\"1237964722118\" FINISH_TIME=\"1237964722118\" HOSTNAME=\"host.com\" STATE_STRING=\"setup\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(0)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(0)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(SPILLED_RECORDS)(Spilled Records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(0)]}\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0023_r_000001\" TASK_TYPE=\"SETUP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237964796054\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(0)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(0)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(SPILLED_RECORDS)(Spilled Records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(0)]}\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0023\" JOB_STATUS=\"RUNNING\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0023_m_000000\" TASK_TYPE=\"MAP\" START_TIME=\"1237964796176\" SPLITS=\"\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0023_m_000001\" TASK_TYPE=\"MAP\" START_TIME=\"1237964796176\" SPLITS=\"\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0023_m_000000\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_m_000000_0\" START_TIME=\"1237964809765\" TRACKER_NAME=\"tracker_50459\" HTTP_PORT=\"50060\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0023_m_000000\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_m_000000_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237964911772\" HOSTNAME=\"host.com\" STATE_STRING=\"\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(500000000)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(5000000)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(5000000)][(MAP_OUTPUT_RECORDS)(Map output records)(5000000)]}\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0023_m_000000\" TASK_TYPE=\"MAP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237964916534\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(500000000)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(5000000)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(5000000)][(MAP_OUTPUT_RECORDS)(Map output records)(5000000)]}\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0023_m_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_m_000001_0\" START_TIME=\"1237964798169\" TRACKER_NAME=\"tracker_1524\" HTTP_PORT=\"50060\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0023_m_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_m_000001_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237964962960\" HOSTNAME=\"host.com\" STATE_STRING=\"\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(500000000)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(5000000)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(5000000)][(MAP_OUTPUT_RECORDS)(Map output records)(5000000)]}\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0023_m_000001\" TASK_TYPE=\"MAP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237964976870\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(500000000)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(5000000)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(5000000)][(MAP_OUTPUT_RECORDS)(Map output records)(5000000)]}\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0023_r_000000\" TASK_TYPE=\"CLEANUP\" START_TIME=\"1237964976871\" SPLITS=\"\" ."); writer.newLine();
    writer.write("ReduceAttempt TASK_TYPE=\"CLEANUP\" TASKID=\"task_200903250600_0023_r_000000\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_r_000000_0\" START_TIME=\"1237964977208\" TRACKER_NAME=\"tracker_1524\" HTTP_PORT=\"50060\" ."); writer.newLine();
    writer.write("ReduceAttempt TASK_TYPE=\"CLEANUP\" TASKID=\"task_200903250600_0023_r_000000\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_r_000000_0\" TASK_STATUS=\"SUCCESS\" SHUFFLE_FINISHED=\"1237964979031\" SORT_FINISHED=\"1237964979031\" FINISH_TIME=\"1237964979032\" HOSTNAME=\"host.com\" STATE_STRING=\"cleanup\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(0)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(0)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(SPILLED_RECORDS)(Spilled Records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(0)]}\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0023_r_000000\" TASK_TYPE=\"CLEANUP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237964991879\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(0)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(0)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(SPILLED_RECORDS)(Spilled Records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(0)]}\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0023\" FINISH_TIME=\"1237964991879\" JOB_STATUS=\"SUCCESS\" FINISHED_MAPS=\"2\" FINISHED_REDUCES=\"0\" FAILED_MAPS=\"0\" FAILED_REDUCES=\"0\" COUNTERS=\"{(org.apache.hadoop.mapred.JobInProgress$Counter)(Job Counters )[(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(2)]}{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(1000000000)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(10000000)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(10000000)][(MAP_OUTPUT_RECORDS)(Map output records)(10000000)]}\" ."); writer.newLine();
    writer.write("$!!FILE=file3.log!!"); writer.newLine();
    writer.write("Meta VERSION=\"1\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0034\" JOBNAME=\"TestJob\" USER=\"hadoop3\" SUBMIT_TIME=\"1237966370007\" JOBCONF=\"hdfs:///job_200903250600_0034/job.xml\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0034\" JOB_PRIORITY=\"NORMAL\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0034\" LAUNCH_TIME=\"1237966371076\" TOTAL_MAPS=\"2\" TOTAL_REDUCES=\"0\" JOB_STATUS=\"PREP\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0034_m_000003\" TASK_TYPE=\"SETUP\" START_TIME=\"1237966371093\" SPLITS=\"\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"SETUP\" TASKID=\"task_200903250600_0034_m_000003\" TASK_ATTEMPT_ID=\"attempt_200903250600_0034_m_000003_0\" START_TIME=\"1237966371524\" TRACKER_NAME=\"tracker_50118\" HTTP_PORT=\"50060\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"SETUP\" TASKID=\"task_200903250600_0034_m_000003\" TASK_ATTEMPT_ID=\"attempt_200903250600_0034_m_000003_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237966373174\" HOSTNAME=\"host.com\" STATE_STRING=\"setup\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0034_m_000003\" TASK_TYPE=\"SETUP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237966386098\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0034\" JOB_STATUS=\"RUNNING\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0034_m_000000\" TASK_TYPE=\"MAP\" START_TIME=\"1237966386111\" SPLITS=\"\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0034_m_000001\" TASK_TYPE=\"MAP\" START_TIME=\"1237966386124\" SPLITS=\"\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0034_m_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0034_m_000001_0\" TASK_STATUS=\"FAILED\" FINISH_TIME=\"1237967174546\" HOSTNAME=\"host.com\" ERROR=\"java.io.IOException: Task process exit with nonzero status of 15."); writer.newLine();
    writer.write("  at org.apache.hadoop.mapred.TaskRunner.run(TaskRunner.java:424)"); writer.newLine();
    writer.write(",java.io.IOException: Task process exit with nonzero status of 15."); writer.newLine();
    writer.write("  at org.apache.hadoop.mapred.TaskRunner.run(TaskRunner.java:424)"); writer.newLine();
    writer.write("\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0034_m_000002\" TASK_TYPE=\"CLEANUP\" START_TIME=\"1237967170815\" SPLITS=\"\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"CLEANUP\" TASKID=\"task_200903250600_0034_m_000002\" TASK_ATTEMPT_ID=\"attempt_200903250600_0034_m_000002_0\" START_TIME=\"1237967168653\" TRACKER_NAME=\"tracker_3105\" HTTP_PORT=\"50060\" ."); writer.newLine();
    writer.write("MapAttempt TASK_TYPE=\"CLEANUP\" TASKID=\"task_200903250600_0034_m_000002\" TASK_ATTEMPT_ID=\"attempt_200903250600_0034_m_000002_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237967171301\" HOSTNAME=\"host.com\" STATE_STRING=\"cleanup\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."); writer.newLine();
    writer.write("Task TASKID=\"task_200903250600_0034_m_000002\" TASK_TYPE=\"CLEANUP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237967185818\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."); writer.newLine();
    writer.write("Job JOBID=\"job_200903250600_0034\" FINISH_TIME=\"1237967185818\" JOB_STATUS=\"KILLED\" FINISHED_MAPS=\"0\" FINISHED_REDUCES=\"0\" ."); writer.newLine();
    writer.close();
  }

  @After
  public void tearDown() throws Exception {
    File logFile = new File(historyLog);
    if(!logFile.delete())
      LOG.error("Cannot delete history log file: " + historyLog);
    if(!logFile.getParentFile().delete())
      LOG.error("Cannot delete history log dir: " + historyLog);
  }

  /**
   * Run log analyzer in test mode for file test.log.
   */
  @Test
  public void testJHLA() {
    String[] args = {"-test", historyLog, "-jobDelimiter", ".!!FILE=.*!!"};
    JHLogAnalyzer.main(args);
    args = new String[]{"-test", historyLog, "-jobDelimiter", ".!!FILE=.*!!",
                        "-usersIncluded", "hadoop,hadoop2"};
    JHLogAnalyzer.main(args);
    args = new String[]{"-test", historyLog, "-jobDelimiter", ".!!FILE=.*!!",
        "-usersExcluded", "hadoop,hadoop3"};
    JHLogAnalyzer.main(args);
  }
}
