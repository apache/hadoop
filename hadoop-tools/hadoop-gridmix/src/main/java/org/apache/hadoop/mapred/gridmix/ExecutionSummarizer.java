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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.mapred.gridmix.GenerateData.DataStatistics;
import org.apache.hadoop.mapred.gridmix.Statistics.JobStats;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;

/**
 * Summarizes a {@link Gridmix} run. Statistics that are reported are
 * <ul>
 *   <li>Total number of jobs in the input trace</li>
 *   <li>Trace signature</li>
 *   <li>Total number of jobs processed from the input trace</li>
 *   <li>Total number of jobs submitted</li>
 *   <li>Total number of successful and failed jobs</li>
 *   <li>Total number of map/reduce tasks launched</li>
 *   <li>Gridmix start & end time</li>
 *   <li>Total time for the Gridmix run (data-generation and simulation)</li>
 *   <li>Gridmix Configuration (i.e job-type, submission-type, resolver)</li>
 * </ul>
 */
class ExecutionSummarizer implements StatListener<JobStats> {
  static final Log LOG = LogFactory.getLog(ExecutionSummarizer.class);
  private static final FastDateFormat UTIL = FastDateFormat.getInstance();
  
  private int numJobsInInputTrace;
  private int totalSuccessfulJobs;
  private int totalFailedJobs;
  private int totalLostJobs;
  private int totalMapTasksLaunched;
  private int totalReduceTasksLaunched;
  private long totalSimulationTime;
  private long totalRuntime;
  private final String commandLineArgs;
  private long startTime;
  private long endTime;
  private long simulationStartTime;
  private String inputTraceLocation;
  private String inputTraceSignature;
  private String jobSubmissionPolicy;
  private String resolver;
  private DataStatistics dataStats;
  private String expectedDataSize;
  
  /**
   * Basic constructor initialized with the runtime arguments. 
   */
  ExecutionSummarizer(String[] args) {
    startTime = System.currentTimeMillis();
    // flatten the args string and store it
    commandLineArgs = 
      org.apache.commons.lang.StringUtils.join(args, ' '); 
  }
  
  /**
   * Default constructor. 
   */
  ExecutionSummarizer() {
    startTime = System.currentTimeMillis();
    commandLineArgs = Summarizer.NA; 
  }
  
  void start(Configuration conf) {
    simulationStartTime = System.currentTimeMillis();
  }
  
  private void processJobState(JobStats stats) {
    Job job = stats.getJob();
    try {
      if (job.isSuccessful()) {
        ++totalSuccessfulJobs;
      } else {
        ++totalFailedJobs;
      }
    } catch (Exception e) {
      // this behavior is consistent with job-monitor which marks the job as 
      // complete (lost) if the status polling bails out
      ++totalLostJobs; 
    }
  }
  
  private void processJobTasks(JobStats stats) {
    totalMapTasksLaunched += stats.getNoOfMaps();
    totalReduceTasksLaunched += stats.getNoOfReds();
  }
  
  private void process(JobStats stats) {
    // process the job run state
    processJobState(stats);

    // process the tasks information
    processJobTasks(stats);
  }
  
  @Override
  public void update(JobStats item) {
    // process only if the simulation has started
    if (simulationStartTime > 0) {
      process(item);
      totalSimulationTime = 
        System.currentTimeMillis() - getSimulationStartTime();
    }
  }
  
  // Generates a signature for the trace file based on
  //   - filename
  //   - modification time
  //   - file length
  //   - owner
  protected static String getTraceSignature(String input) throws IOException {
    Path inputPath = new Path(input);
    FileSystem fs = inputPath.getFileSystem(new Configuration());
    FileStatus status = fs.getFileStatus(inputPath);
    Path qPath = fs.makeQualified(status.getPath());
    String traceID = status.getModificationTime() + qPath.toString()
                     + status.getOwner() + status.getLen();
    return MD5Hash.digest(traceID).toString();
  }
  
  @SuppressWarnings("unchecked")
  void finalize(JobFactory factory, String inputPath, long dataSize, 
                UserResolver userResolver, DataStatistics stats,
                Configuration conf) 
  throws IOException {
    numJobsInInputTrace = factory.numJobsInTrace;
    endTime = System.currentTimeMillis();
     if ("-".equals(inputPath)) {
      inputTraceLocation = Summarizer.NA;
      inputTraceSignature = Summarizer.NA;
    } else {
      Path inputTracePath = new Path(inputPath);
      FileSystem fs = inputTracePath.getFileSystem(conf);
      inputTraceLocation = fs.makeQualified(inputTracePath).toString();
      inputTraceSignature = getTraceSignature(inputPath);
    }
    jobSubmissionPolicy = Gridmix.getJobSubmissionPolicy(conf).name();
    resolver = userResolver.getClass().getName();
    if (dataSize > 0) {
      expectedDataSize = StringUtils.humanReadableInt(dataSize);
    } else {
      expectedDataSize = Summarizer.NA;
    }
    dataStats = stats;
    totalRuntime = System.currentTimeMillis() - getStartTime();
  }
  
  /**
   * Summarizes the current {@link Gridmix} run.
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Execution Summary:-");
    builder.append("\nInput trace: ").append(getInputTraceLocation());
    builder.append("\nInput trace signature: ")
           .append(getInputTraceSignature());
    builder.append("\nTotal number of jobs in trace: ")
           .append(getNumJobsInTrace());
    builder.append("\nExpected input data size: ")
           .append(getExpectedDataSize());
    builder.append("\nInput data statistics: ")
           .append(getInputDataStatistics());
    builder.append("\nTotal number of jobs processed: ")
           .append(getNumSubmittedJobs());
    builder.append("\nTotal number of successful jobs: ")
           .append(getNumSuccessfulJobs());
    builder.append("\nTotal number of failed jobs: ")
           .append(getNumFailedJobs());
    builder.append("\nTotal number of lost jobs: ")
           .append(getNumLostJobs());
    builder.append("\nTotal number of map tasks launched: ")
           .append(getNumMapTasksLaunched());
    builder.append("\nTotal number of reduce task launched: ")
           .append(getNumReduceTasksLaunched());
    builder.append("\nGridmix start time: ")
           .append(UTIL.format(getStartTime()));
    builder.append("\nGridmix end time: ").append(UTIL.format(getEndTime()));
    builder.append("\nGridmix simulation start time: ")
           .append(UTIL.format(getStartTime()));
    builder.append("\nGridmix runtime: ")
           .append(StringUtils.formatTime(getRuntime()));
    builder.append("\nTime spent in initialization (data-gen etc): ")
           .append(StringUtils.formatTime(getInitTime()));
    builder.append("\nTime spent in simulation: ")
           .append(StringUtils.formatTime(getSimulationTime()));
    builder.append("\nGridmix configuration parameters: ")
           .append(getCommandLineArgsString());
    builder.append("\nGridmix job submission policy: ")
           .append(getJobSubmissionPolicy());
    builder.append("\nGridmix resolver: ").append(getUserResolver());
    builder.append("\n\n");
    return builder.toString();
  }
  
  // Gets the stringified version of DataStatistics
  static String stringifyDataStatistics(DataStatistics stats) {
    if (stats != null) {
      StringBuffer buffer = new StringBuffer();
      String compressionStatus = stats.isDataCompressed() 
                                 ? "Compressed" 
                                 : "Uncompressed";
      buffer.append(compressionStatus).append(" input data size: ");
      buffer.append(StringUtils.humanReadableInt(stats.getDataSize()));
      buffer.append(", ");
      buffer.append("Number of files: ").append(stats.getNumFiles());

      return buffer.toString();
    } else {
      return Summarizer.NA;
    }
  }
  
  // Getters
  protected String getExpectedDataSize() {
    return expectedDataSize;
  }
  
  protected String getUserResolver() {
    return resolver;
  }
  
  protected String getInputDataStatistics() {
    return stringifyDataStatistics(dataStats);
  }
  
  protected String getInputTraceSignature() {
    return inputTraceSignature;
  }
  
  protected String getInputTraceLocation() {
    return inputTraceLocation;
  }
  
  protected int getNumJobsInTrace() {
    return numJobsInInputTrace;
  }
  
  protected int getNumSuccessfulJobs() {
    return totalSuccessfulJobs;
  }
  
  protected int getNumFailedJobs() {
    return totalFailedJobs;
  }
  
  protected int getNumLostJobs() {
    return totalLostJobs;
  }
  
  protected int getNumSubmittedJobs() {
    return totalSuccessfulJobs + totalFailedJobs + totalLostJobs;
  }
  
  protected int getNumMapTasksLaunched() {
    return totalMapTasksLaunched;
  }
  
  protected int getNumReduceTasksLaunched() {
    return totalReduceTasksLaunched;
  }
  
  protected long getStartTime() {
    return startTime;
  }
  
  protected long getEndTime() {
    return endTime;
  }
  
  protected long getInitTime() {
    return simulationStartTime - startTime;
  }
  
  protected long getSimulationStartTime() {
    return simulationStartTime;
  }
  
  protected long getSimulationTime() {
    return totalSimulationTime;
  }
  
  protected long getRuntime() {
    return totalRuntime;
  }
  
  protected String getCommandLineArgsString() {
    return commandLineArgs;
  }
  
  protected String getJobSubmissionPolicy() {
    return jobSubmissionPolicy;
  }
}