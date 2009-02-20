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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.SortValidator.RecordStatsChecker.NonSplitableSequenceFileInputFormat;
import org.apache.hadoop.mapred.ThreadedMapBenchmark.RandomInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.security.UserGroupInformation;

import junit.framework.TestCase;
import java.io.*;
import java.util.Iterator;

/** 
 * TestJobTrackerRestart checks if the jobtracker can restart. JobTracker 
 * should be able to continue running the previously running jobs and also 
 * recover previosuly submitted jobs.
 */
public class TestJobTrackerRestart extends TestCase {
  final static Object waitLock = new Object();
  final Path testDir = new Path("/jt-restart-testing");
  final Path inDir = new Path(testDir, "input");
  final Path shareDir = new Path(testDir, "share");
  final Path outputDir = new Path(testDir, "output");
  private static int numJobsSubmitted = 0;
  
  /**
   * Gets job status from the jobtracker given the jobclient and the job id
   */
  static JobStatus getJobStatus(JobClient jc, JobID id) throws IOException {
    JobStatus[] statuses = jc.getAllJobs();
    for (JobStatus jobStatus : statuses) {
      if (jobStatus.getJobID().equals(id)) {
        return jobStatus;
      }
    }
    return null;
  }

  /**
   * Return the job conf configured with the priorities and mappers as passed.
   * @param conf The default conf
   * @param priorities priorities for the jobs
   * @param numMaps number of maps for the jobs
   * @param numReds number of reducers for the jobs
   * @param outputDir output dir
   * @param inDir input dir
   * @param mapSignalFile filename thats acts as a signal for maps
   * @param reduceSignalFile filename thats acts as a signal for reducers
   * @return a array of jobconfs configured as needed
   * @throws IOException
   */
  static JobConf[] getJobs(JobConf conf, JobPriority[] priorities, 
                           int[] numMaps, int[] numReds,
                           Path outputDir, Path inDir,
                           String mapSignalFile, String reduceSignalFile) 
  throws IOException {
    JobConf[] jobs = new JobConf[priorities.length];
    for (int i = 0; i < jobs.length; ++i) {
      jobs[i] = new JobConf(conf);
      Path newOutputDir = outputDir.suffix(String.valueOf(numJobsSubmitted++));
      configureWaitingJobConf(jobs[i], inDir, newOutputDir, 
                              numMaps[i], numReds[i], "jt-restart-test-job", 
                              mapSignalFile, reduceSignalFile);
      jobs[i].setJobPriority(priorities[i]);
    }
    return jobs;
  }

  /**
   * A utility that waits for specified amount of time
   */
  static void waitFor(long duration) {
    try {
      synchronized (waitLock) {
        waitLock.wait(duration);
      }
    } catch (InterruptedException ie) {}
  }
  
  /**
   * Wait for the jobtracker to be RUNNING.
   */
  static void waitForJobTracker(JobClient jobClient) {
    while (true) {
      try {
        ClusterStatus status = jobClient.getClusterStatus();
        while (status.getJobTrackerState() != JobTracker.State.RUNNING) {
          waitFor(100);
          status = jobClient.getClusterStatus();
        }
        break; // means that the jt is ready
      } catch (IOException ioe) {}
    }
  }
  
  /**
   * Signal the maps/reduces to start.
   */
  static void signalTasks(MiniDFSCluster dfs, FileSystem fileSys, 
                          boolean isMap, String mapSignalFile, 
                          String reduceSignalFile)
  throws IOException {
    //  signal the maps to complete
    TestRackAwareTaskPlacement.writeFile(dfs.getNameNode(), fileSys.getConf(),
                                         isMap 
                                         ? new Path(mapSignalFile)
                                         : new Path(reduceSignalFile), 
                                         (short)1);
  }
  
  /**
   * Waits until all the jobs at the jobtracker complete.
   */
  static void waitTillDone(JobClient jobClient) throws IOException {
    // Wait for the last job to complete
    while (true) {
      boolean shouldWait = false;
      for (JobStatus jobStatuses : jobClient.getAllJobs()) {
        if (jobStatuses.getRunState() == JobStatus.RUNNING) {
          shouldWait = true;
          break;
        }
      }
      if (shouldWait) {
        waitFor(1000);
      } else {
        break;
      }
    }
  }
  
  /**
   * Clean up the signals.
   */
  static void cleanUp(FileSystem fileSys, Path dir) throws IOException {
    // Delete the map signal file
    fileSys.delete(new Path(getMapSignalFile(dir)), false);
    // Delete the reduce signal file
    fileSys.delete(new Path(getReduceSignalFile(dir)), false);
  }
  
 /**
   * Tests multiple jobs on jobtracker with restart-recovery turned on.
   * Preparation :
   *    - Configure 3 jobs as follows [format {prio, maps, reducers}]
   *       - job1 : {normal, 50, 1}
   *       - job2 : {low, 1, 1}
   *       - job3 : {high, 1, 1}
   *    - Configure the cluster to run 1 reducer
   *    - Lower the history file block size and buffer
   *    
   * Submit these 3 jobs but make sure that job1's priority is changed and job1
   * is RUNNING before submitting other jobs
   * The order in which the jobs will be executed will be job1, job3 and job2.
   * 
   * Above ordering makes sure that job1 runs before everyone else.
   * Wait for job1 to complete 50%. Note that all the jobs are configured to 
   * use {@link HalfWaitingMapper} and {@link WaitingReducer}. So job1 will 
   * eventually wait on 50%
   * 
   * Make a note of the following things
   *    - Job start times
   *    
   * Restart the jobtracker
   * 
   * Wait for job1 to finish all the maps and note the TaskCompletion events at
   * the tracker.
   * 
   * Wait for all the jobs to finish
   * 
   * Also make sure that the order in which the jobs were sorted before restart
   * remains same. For this check the follwoing
   *   job1.start-time < job2.start-time < job3.start-time and 
   *   job1.finish-time < job3.finish-time < job2.finish-time
   * This ordering makes sure that the change of priority is logged and 
   * recovered back
   */
  public void testRecoveryWithMultipleJobs(MiniDFSCluster dfs,
                                           MiniMRCluster mr) 
  throws IOException {
    FileSystem fileSys = dfs.getFileSystem();
    JobConf jobConf = mr.createJobConf();
    JobPriority[] priorities = {JobPriority.NORMAL, JobPriority.LOW, 
                                JobPriority.HIGH};
    // Note that there is only 1 tracker
    int[] numMaps = {50, 1, 1};
    int[] numReds = {1, 1, 1};

    cleanUp(fileSys, shareDir);
    
    // Configure the jobs
    JobConf[] jobs = getJobs(jobConf, priorities, numMaps, numReds,
                             outputDir, inDir, 
                             getMapSignalFile(shareDir), 
                             getReduceSignalFile(shareDir));

    // Master job parameters
    int masterJob = 0;
    JobPriority masterJobNewPriority = JobPriority.HIGH;

    // Submit a master job   
    JobClient jobClient = new JobClient(jobs[masterJob]);
    RunningJob job = jobClient.submitJob(jobs[masterJob]);
    JobID id = job.getID();

    // Wait for the job to be inited
    mr.initializeJob(id);

    // Change the master job's priority so that priority logging is tested
    mr.setJobPriority(id, masterJobNewPriority);

    // Submit the remaining jobs and find the last job id
    for (int i = 1; i < jobs.length; ++i) {
      RunningJob rJob = (new JobClient(jobs[i])).submitJob(jobs[i]);
      mr.initializeJob(rJob.getID());
    }

    // Make sure that the master job is 50% completed
    while (getJobStatus(jobClient, id).mapProgress() < 0.5f) {
      waitFor(100);
    }

    // Note the data that needs to be tested upon restart
    long jobStartTime = getJobStatus(jobClient, id).getStartTime();

    // Kill the jobtracker
    mr.stopJobTracker();

    // Signal the maps to complete
    signalTasks(dfs, fileSys, true, getMapSignalFile(shareDir), 
                getReduceSignalFile(shareDir));

    // Signal the reducers to complete
    signalTasks(dfs, fileSys, false, getMapSignalFile(shareDir), 
                getReduceSignalFile(shareDir));
    
    // Enable recovery on restart
    mr.getJobTrackerConf().setBoolean("mapred.jobtracker.restart.recover", 
                                      true);

    //  Wait for a minute before submitting a job
    waitFor(60 * 1000);
    
    // Restart the jobtracker
    mr.startJobTracker();

    // Check if the jobs are still running

    // Wait for the JT to be ready
    waitForJobTracker(jobClient);

    // Check if the job recovered
    assertEquals("Restart failed as previously submitted job was missing", 
                 true, getJobStatus(jobClient, id) != null);

    // check if the job's priority got changed
    assertEquals("Restart failed as job's priority did not match", 
                 true, mr.getJobPriority(id).equals(masterJobNewPriority));

    

    waitTillDone(jobClient);

    // Check if the jobs are in order .. the order is 1->3->2
    JobStatus[] newStatuses = jobClient.getAllJobs();
    // Check if the jobs are in the order of submission
    //   This is important for the following checks
    boolean jobOrder = newStatuses[0].getJobID().getId() == 1
                       && newStatuses[1].getJobID().getId() == 2
                       && newStatuses[2].getJobID().getId() == 3;
    assertTrue("Job submission order changed", jobOrder);
    
    // Start times are in order and non zero
    boolean startTimeOrder = newStatuses[0].getStartTime() > 0
                             && newStatuses[0].getStartTime() 
                                < newStatuses[1].getStartTime()
                             && newStatuses[1].getStartTime() 
                                < newStatuses[2].getStartTime();
    assertTrue("Job start-times are out of order", startTimeOrder);
    
    boolean finishTimeOrder = 
      mr.getJobFinishTime(newStatuses[0].getJobID()) > 0
      && mr.getJobFinishTime(newStatuses[0].getJobID()) 
         < mr.getJobFinishTime(newStatuses[2].getJobID())
      && mr.getJobFinishTime(newStatuses[2].getJobID()) 
         < mr.getJobFinishTime(newStatuses[1].getJobID());
    assertTrue("Jobs finish-times are out of order", finishTimeOrder);
            
    
    // This should be used for testing job counters
    job.getCounters();

    // check if the job was successful
    assertTrue("Previously submitted job was not successful", 
               job.isSuccessful());

    // Check if the start time was recovered
    assertTrue("Previously submitted job's start time has changed", 
               getJobStatus(jobClient, id).getStartTime() == jobStartTime);

    // Test history files
    testJobHistoryFiles(id, jobs[masterJob]);
  }
  
  /**
   * Tests the jobtracker with restart-recovery turned off.
   * Submit a job with normal priority, maps = 2, reducers = 0}
   * 
   * Wait for the job to complete 50%
   * 
   * Restart the jobtracker with recovery turned off
   * 
   * Check if the job is missing
   */
  public void testRestartWithoutRecovery(MiniDFSCluster dfs, 
                                         MiniMRCluster mr) 
  throws IOException {
    // III. Test a job with waiting mapper and recovery turned off
    
    FileSystem fileSys = dfs.getFileSystem();
    
    cleanUp(fileSys, shareDir);
    
    JobConf newConf = getJobs(mr.createJobConf(), 
                              new JobPriority[] {JobPriority.NORMAL}, 
                              new int[] {2}, new int[] {0},
                              outputDir, inDir, 
                              getMapSignalFile(shareDir), 
                              getReduceSignalFile(shareDir))[0];
    
    JobClient jobClient = new JobClient(newConf);
    RunningJob job = jobClient.submitJob(newConf);
    JobID id = job.getID();
    
    //  make sure that the job is 50% completed
    while (getJobStatus(jobClient, id).mapProgress() < 0.5f) {
      waitFor(100);
    }
    
    mr.stopJobTracker();
    
    // Turn off the recovery
    mr.getJobTrackerConf().setBoolean("mapred.jobtracker.restart.recover", 
                                      false);
    
    // Wait for a minute before submitting a job
    waitFor(60 * 1000);
    
    mr.startJobTracker();
    
    // Signal the tasks
    signalTasks(dfs, fileSys, true, getMapSignalFile(shareDir), 
                getReduceSignalFile(shareDir));
    
    // Wait for the JT to be ready
    waitForJobTracker(jobClient);
    
    waitTillDone(jobClient);
    
    // The submitted job should not exist
    assertTrue("Submitted job was detected with recovery disabled", 
               getJobStatus(jobClient, id) == null);
  }

  /** Tests a job on jobtracker with restart-recovery turned on.
   * Preparation :
   *    - Configure a job with
   *       - num-maps : 50
   *       - num-reducers : 1
   *    - Configure the cluster to run 1 reducer
   *    - Lower the history file block size and buffer
   *    
   * Wait for the job to complete 50%. Note that all the job is configured to 
   * use {@link HalfWaitingMapper} and {@link WaitingReducer}. So job will 
   * eventually wait on 50%
   * 
   * Make a note of the following things
   *    - Task completion events
   *    - Cluster status
   *    - Task Reports
   *    - Job start time
   *    
   * Restart the jobtracker
   * 
   * Wait for job to finish all the maps and note the TaskCompletion events at
   * the tracker.
   * 
   * Wait for all the jobs to finish and note the following
   *    - New task completion events at the jobtracker
   *    - Task reports
   *    - Cluster status
   * 
   * Check for the following
   *    - Task completion events for recovered tasks should match 
   *    - Task completion events at the tasktracker and the restarted 
   *      jobtracker should be same
   *    - Cluster status should be fine.
   *    - Task Reports for recovered tasks should match
   *      Checks
   *        - start time
   *        - finish time
   *        - counters
   *        - http-location
   *        - task-id
   *    - Job start time should match
   *    - Check if the counters can be accessed
   *    - Check if the history files are (re)named properly
   */
  public void testTaskEventsAndReportsWithRecovery(MiniDFSCluster dfs, 
                                                   MiniMRCluster mr) 
  throws IOException {
    // II. Test a tasktracker with waiting mapper and recovery turned on.
    //     Ideally the tracker should SYNC with the new/restarted jobtracker
    
    FileSystem fileSys = dfs.getFileSystem();
    final int numMaps = 50;
    final int numReducers = 1;
    
    
    cleanUp(fileSys, shareDir);
    
    JobConf newConf = getJobs(mr.createJobConf(), 
                              new JobPriority[] {JobPriority.NORMAL}, 
                              new int[] {numMaps}, new int[] {numReducers},
                              outputDir, inDir, 
                              getMapSignalFile(shareDir), 
                              getReduceSignalFile(shareDir))[0];
    
    JobClient jobClient = new JobClient(newConf);
    RunningJob job = jobClient.submitJob(newConf);
    JobID id = job.getID();
    
    mr.initializeJob(id);
    
    //  make sure that atleast on reducer is spawned
    while (jobClient.getClusterStatus().getReduceTasks() == 0) {
      waitFor(100);
    }
    
    while(true) {
      // Since we are using a half waiting mapper, maps should be stuck at 50%
      TaskCompletionEvent[] trackerEvents = 
        mr.getMapTaskCompletionEventsUpdates(0, id, numMaps)
          .getMapTaskCompletionEvents();
      if (trackerEvents.length < numMaps / 2) {
        waitFor(1000);
      } else {
        break;
      }
    }
    
    TaskCompletionEvent[] prevEvents = 
      mr.getTaskCompletionEvents(id, 0, numMaps);
    TaskReport[] prevSetupReports = jobClient.getSetupTaskReports(id);
    TaskReport[] prevMapReports = jobClient.getMapTaskReports(id);
    ClusterStatus prevStatus = jobClient.getClusterStatus();
    
    mr.stopJobTracker();
    
    // Turn off the recovery
    mr.getJobTrackerConf().setBoolean("mapred.jobtracker.restart.recover", 
                                      true);
    
    //  Wait for a minute before submitting a job
    waitFor(60 * 1000);
    
    mr.startJobTracker();
    
    // Signal the map tasks
    signalTasks(dfs, fileSys, true, getMapSignalFile(shareDir), 
                getReduceSignalFile(shareDir));
    
    // Wait for the JT to be ready
    waitForJobTracker(jobClient);
    
    int numToMatch = mr.getNumEventsRecovered() / 2;
    
    //  make sure that the maps are completed
    while (getJobStatus(jobClient, id).mapProgress() < 1.0f) {
      waitFor(100);
    }
    
    // Get the new jobtrackers events
    TaskCompletionEvent[] jtEvents =  
      mr.getTaskCompletionEvents(id, 0, 2 * numMaps);
    
    // Test if all the events that were recovered match exactly
    testTaskCompletionEvents(prevEvents, jtEvents, false, numToMatch);
    
    TaskCompletionEvent[] trackerEvents;
    while(true) {
      trackerEvents = 
        mr.getMapTaskCompletionEventsUpdates(0, id, 2 * numMaps)
          .getMapTaskCompletionEvents();
      if (trackerEvents.length < jtEvents.length) {
        waitFor(1000);
      } else {
        break;
      }
    }
    
    // Check the task reports
    // The reports should match exactly if the attempts are same
    TaskReport[] afterMapReports = jobClient.getMapTaskReports(id);
    TaskReport[] afterSetupReports = jobClient.getSetupTaskReports(id);
    testTaskReports(prevMapReports, afterMapReports, numToMatch - 1);
    testTaskReports(prevSetupReports, afterSetupReports, 1);
    
    //  Signal the reduce tasks
    signalTasks(dfs, fileSys, false, getMapSignalFile(shareDir), 
                getReduceSignalFile(shareDir));
    
    waitTillDone(jobClient);
    
    testTaskCompletionEvents(jtEvents, trackerEvents, true, 2 * numMaps);
    
    // check if the cluster status is insane
    ClusterStatus status = jobClient.getClusterStatus();
    assertTrue("Cluster status is insane", 
               checkClusterStatusOnCompletion(status, prevStatus));
  }
  
  /**
   * Checks if the history files are as expected
   * @param id job id
   * @param conf job conf
   */
  private void testJobHistoryFiles(JobID id, JobConf conf) 
  throws IOException  {
    // Get the history files for users
    String logFileName = JobHistory.JobInfo.getJobHistoryFileName(conf, id);
    String tempLogFileName = 
      JobHistory.JobInfo.getSecondaryJobHistoryFile(logFileName);
    
    // I. User files
    Path logFile = 
      JobHistory.JobInfo.getJobHistoryLogLocationForUser(logFileName, conf);
    FileSystem fileSys = logFile.getFileSystem(conf);
    
    // Check if the history file exists
    assertTrue("User log file does not exist", fileSys.exists(logFile));
    
    // Check if the temporary file is deleted
    Path tempLogFile = 
      JobHistory.JobInfo.getJobHistoryLogLocationForUser(tempLogFileName, 
                                                         conf);
    assertFalse("User temporary log file exists", fileSys.exists(tempLogFile));
    
    // II. Framework files
    // Get the history file
    logFile = JobHistory.JobInfo.getJobHistoryLogLocation(logFileName);
    fileSys = logFile.getFileSystem(conf);
    
    // Check if the history file exists
    assertTrue("Log file does not exist", fileSys.exists(logFile));
    
    // Check if the temporary file is deleted
    tempLogFile = JobHistory.JobInfo.getJobHistoryLogLocation(tempLogFileName);
    assertFalse("Temporary log file exists", fileSys.exists(tempLogFile));
  }
  
  /**
   * Matches specified number of task reports.
   * @param source the reports to be matched
   * @param target reports to match with
   * @param numToMatch num reports to match
   * @param mismatchSet reports that should not match
   */
  private void testTaskReports(TaskReport[] source, TaskReport[] target, 
                               int numToMatch) {
    for (int i = 0; i < numToMatch; ++i) {
      // Check if the task reports was recovered correctly
      assertTrue("Task reports for same attempt has changed", 
                 source[i].equals(target[i]));
    }
  }
  
  /**
   * Matches the task completion events.
   * @param source the events to be matched
   * @param target events to match with
   * @param fullMatch whether to match the events completely or partially
   * @param numToMatch number of events to match in case full match is not 
   *        desired
   * @param ignoreSet a set of taskids to ignore
   */
  private void testTaskCompletionEvents(TaskCompletionEvent[] source, 
                                       TaskCompletionEvent[] target, 
                                       boolean fullMatch,
                                       int numToMatch) {
    //  Check if the event list size matches
    // The lengths should match only incase of full match
    if (fullMatch) {
      assertEquals("Map task completion events mismatch", 
                   source.length, target.length);
      numToMatch = source.length;
    }
    // Check if the events match
    for (int i = 0; i < numToMatch; ++i) {
      if (source[i].getTaskAttemptId().equals(target[i].getTaskAttemptId())){
        assertTrue("Map task completion events ordering mismatch", 
                   source[i].equals(target[i]));
      }
    }
  }
  
  private boolean checkClusterStatusOnCompletion(ClusterStatus status, 
                                                 ClusterStatus prevStatus) {
    return status.getJobTrackerState() == prevStatus.getJobTrackerState()
           && status.getMapTasks() == 0
           && status.getReduceTasks() == 0;
  }
  
  public void testJobTrackerRestart() throws IOException {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;

    try {
      Configuration conf = new Configuration();
      conf.setBoolean("dfs.replication.considerLoad", false);
      dfs = new MiniDFSCluster(conf, 1, true, null, null);
      dfs.waitActive();
      fileSys = dfs.getFileSystem();
      
      // clean up
      fileSys.delete(testDir, true);
      
      if (!fileSys.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }

      // Write the input file
      TestRackAwareTaskPlacement.writeFile(dfs.getNameNode(), conf, 
                                           new Path(inDir + "/file"), 
                                           (short)1);

      dfs.startDataNodes(conf, 1, true, null, null, null, null);
      dfs.waitActive();

      namenode = (dfs.getFileSystem()).getUri().getHost() + ":" 
                 + (dfs.getFileSystem()).getUri().getPort();

      // Make sure that jobhistory leads to a proper job restart
      // So keep the blocksize and the buffer size small
      JobConf jtConf = new JobConf();
      jtConf.set("mapred.jobtracker.job.history.block.size", "1024");
      jtConf.set("mapred.jobtracker.job.history.buffer.size", "1024");
      jtConf.setInt("mapred.tasktracker.reduce.tasks.maximum", 1);
      jtConf.setLong("mapred.tasktracker.expiry.interval", 25 * 1000);
      jtConf.setBoolean("mapred.acls.enabled", true);
      // get the user group info
      UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
      jtConf.set("mapred.queue.default.acl-submit-job", ugi.getUserName());
      
      mr = new MiniMRCluster(1, namenode, 1, null, null, jtConf);
      
      // Test multiple jobs on jobtracker with restart-recovery turned on
      testRecoveryWithMultipleJobs(dfs, mr);
      
      // Test the tasktracker SYNC
      testTaskEventsAndReportsWithRecovery(dfs, mr);
      
      // Test jobtracker with restart-recovery turned off
      testRestartWithoutRecovery(dfs, mr);
    } finally {
      if (mr != null) {
        try {
          mr.shutdown();
        } catch (Exception e) {}
      }
      if (dfs != null) {
        try {
          dfs.shutdown();
        } catch (Exception e) {}
      }
    }
  }

  static String getMapSignalFile(Path dir) {
    return dir.suffix("/jt-restart-map-signal").toString();
  }

  static String getReduceSignalFile(Path dir) {
    return dir.suffix("/jt-restart-reduce-signal").toString();
  }
  
  /** 
   * Map is a Mapper that just waits for a file to be created on the dfs. The 
   * file creation is a signal to the mappers and hence acts as a waiting job. 
   * Only the later half of the maps wait for the signal while the rest 
   * complete immediately.
   */

  static class HalfWaitingMapper 
  extends MapReduceBase 
  implements Mapper<WritableComparable, Writable, 
                    WritableComparable, Writable> {

    FileSystem fs = null;
    Path signal;
    int id = 0;
    int totalMaps = 0;

    /** The waiting function.  The map exits once it gets a signal. Here the 
     * signal is the file existence. 
     */
    public void map(WritableComparable key, Writable val, 
                    OutputCollector<WritableComparable, Writable> output,
                    Reporter reporter)
    throws IOException {
      if (id > totalMaps / 2) {
        if (fs != null) {
          while (!fs.exists(signal)) {
            try {
              reporter.progress();
              synchronized (this) {
                this.wait(1000); // wait for 1 sec
              }
            } catch (InterruptedException ie) {
              System.out.println("Interrupted while the map was waiting for "
                                 + " the signal.");
              break;
            }
          }
        } else {
          throw new IOException("Could not get the DFS!!");
        }
      }
    }

    public void configure(JobConf conf) {
      try {
        String taskId = conf.get("mapred.task.id");
        id = Integer.parseInt(taskId.split("_")[4]);
        totalMaps = Integer.parseInt(conf.get("mapred.map.tasks"));
        fs = FileSystem.get(conf);
        signal = new Path(conf.get("test.mapred.map.waiting.target"));
      } catch (IOException ioe) {
        System.out.println("Got an exception while obtaining the filesystem");
      }
    }
  }
  
  /** 
   * Reduce that just waits for a file to be created on the dfs. The 
   * file creation is a signal to the reduce.
   */

  static class WaitingReducer extends MapReduceBase 
  implements Reducer<WritableComparable, Writable, 
                     WritableComparable, Writable> {

    FileSystem fs = null;
    Path signal;
    
    /** The waiting function.  The reduce exits once it gets a signal. Here the
     * signal is the file existence. 
     */
    public void reduce(WritableComparable key, Iterator<Writable> val, 
                       OutputCollector<WritableComparable, Writable> output,
                       Reporter reporter)
    throws IOException {
      if (fs != null) {
        while (!fs.exists(signal)) {
          try {
            reporter.progress();
            synchronized (this) {
              this.wait(1000); // wait for 1 sec
            }
          } catch (InterruptedException ie) {
            System.out.println("Interrupted while the map was waiting for the"
                               + " signal.");
            break;
          }
        }
      } else {
        throw new IOException("Could not get the DFS!!");
      }
    }

    public void configure(JobConf conf) {
      try {
        fs = FileSystem.get(conf);
        signal = new Path(conf.get("test.mapred.reduce.waiting.target"));
      } catch (IOException ioe) {
        System.out.println("Got an exception while obtaining the filesystem");
      }
    }
  }
  
  static void configureWaitingJobConf(JobConf jobConf, Path inDir,
                                      Path outputPath, int numMaps, int numRed,
                                      String jobName, String mapSignalFilename,
                                      String redSignalFilename)
  throws IOException {
    jobConf.setJobName(jobName);
    jobConf.setInputFormat(NonSplitableSequenceFileInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(jobConf, inDir);
    FileOutputFormat.setOutputPath(jobConf, outputPath);
    jobConf.setMapperClass(HalfWaitingMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);
    jobConf.setOutputKeyClass(BytesWritable.class);
    jobConf.setOutputValueClass(BytesWritable.class);
    jobConf.setInputFormat(RandomInputFormat.class);
    jobConf.setNumMapTasks(numMaps);
    jobConf.setNumReduceTasks(numRed);
    jobConf.setJar("build/test/testjar/testjob.jar");
    jobConf.set("test.mapred.map.waiting.target", mapSignalFilename);
    jobConf.set("test.mapred.reduce.waiting.target", redSignalFilename);
  }

  public static void main(String[] args) throws IOException {
    new TestJobTrackerRestart().testJobTrackerRestart();
  }
}
