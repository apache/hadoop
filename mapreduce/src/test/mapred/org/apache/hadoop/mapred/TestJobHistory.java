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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistory;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobSubmittedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

/**
 *
 * testJobHistoryFile
 * Run a job that will be succeeded and validate its history file format and
 * content.
 *
 * testJobHistoryJobStatus
 * Run jobs that will be (1) succeeded (2) failed (3) killed.
 *   Validate job status read from history file in each case.
 *
 */
public class TestJobHistory extends TestCase {
   private static final Log LOG = LogFactory.getLog(TestJobHistory.class);
 
  private static String TEST_ROOT_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).toURI().toString().replace(' ', '+');

  private static final String DIGITS = "[0-9]+";
  
  // hostname like   /default-rack/host1.foo.com OR host1.foo.com
  private static final Pattern hostNamePattern = Pattern.compile(
                                       "(/(([\\w\\-\\.]+)/)+)?([\\w\\-\\.]+)");

  private static final String IP_ADDR =
                       "\\d\\d?\\d?\\.\\d\\d?\\d?\\.\\d\\d?\\d?\\.\\d\\d?\\d?";

  private static final Pattern trackerNamePattern = Pattern.compile(
      "tracker_" + hostNamePattern + ":([\\w\\-\\.]+)/" +
      IP_ADDR + ":" + DIGITS);
  

  private static final Pattern splitsPattern = Pattern.compile(
                              hostNamePattern + "(," + hostNamePattern + ")*");

  private static Map<String, List<String>> taskIDsToAttemptIDs =
                                     new HashMap<String, List<String>>();

  //Each Task End seen from history file is added here
  private static List<String> taskEnds = new ArrayList<String>();


  // Validate Format of Job Level Keys, Values read from history file
  private static void validateJobLevelKeyValuesFormat(JobInfo jobInfo,
                                                      String status) {
    long submitTime = jobInfo.getSubmitTime();
    long launchTime = jobInfo.getLaunchTime();
    long finishTime = jobInfo.getFinishTime();
    
    assertTrue("Invalid submit time", submitTime > 0);
    assertTrue("SubmitTime > LaunchTime", submitTime <= launchTime);
    assertTrue("LaunchTime > FinishTime", launchTime <= finishTime);
    
    String stat = jobInfo.getJobStatus();

    assertTrue("Unexpected JOB_STATUS \"" + stat + "\" is seen in" +
               " history file", (status.equals(stat)));
    String priority = jobInfo.getPriority();

    assertNotNull(priority);
    assertTrue("Unknown priority for the job in history file",
               (priority.equals("HIGH") ||
                priority.equals("LOW")  || priority.equals("NORMAL") ||
                priority.equals("VERY_HIGH") || priority.equals("VERY_LOW")));
  }

  // Validate Format of Task Level Keys, Values read from history file
  private static void validateTaskLevelKeyValuesFormat(JobInfo job,
                                  boolean splitsCanBeEmpty) {
    Map<TaskID, TaskInfo> tasks = job.getAllTasks();

    // validate info of each task
    for (TaskInfo task : tasks.values()) {

      TaskID tid = task.getTaskId();
      long startTime = task.getStartTime();
      assertTrue("Invalid Start time", startTime > 0);
      
      long finishTime = task.getFinishTime();
      assertTrue("Task FINISH_TIME is < START_TIME in history file",
                 startTime < finishTime);

      // Make sure that the Task type exists and it is valid
      TaskType type = task.getTaskType();
      assertTrue("Unknown Task type \"" + type + "\" is seen in " +
                 "history file for task " + tid,
                 (type.equals(TaskType.MAP) || 
                  type.equals(TaskType.REDUCE) ||
                  type.equals(TaskType.JOB_CLEANUP) || 
                  type.equals(TaskType.JOB_SETUP)));

      if (type.equals(TaskType.MAP)) {
        String splits = task.getSplitLocations();
        //order in the condition OR check is important here
        if (!splitsCanBeEmpty || splits.length() != 0) {
          Matcher m = splitsPattern.matcher(splits);
          assertTrue("Unexpected format of SPLITS \"" + splits + "\" is seen" +
                     " in history file for task " + tid, m.matches());
        }
      }

      // Validate task status
      String status = task.getTaskStatus();
      assertTrue("Unexpected TASK_STATUS \"" + status + "\" is seen in" +
                 " history file for task " + tid, (status.equals("SUCCEEDED") ||
                 status.equals("FAILED") || status.equals("KILLED")));
    }
  }

  // Validate foramt of Task Attempt Level Keys, Values read from history file
  private static void validateTaskAttemptLevelKeyValuesFormat(JobInfo job) {
    Map<TaskID, TaskInfo> tasks = job.getAllTasks();

    // For each task
    for (TaskInfo task : tasks.values()) {
      // validate info of each attempt
      for (TaskAttemptInfo attempt : task.getAllTaskAttempts().values()) {

        TaskAttemptID id = attempt.getAttemptId();
        assertNotNull(id);
        
        long startTime = attempt.getStartTime();
        assertTrue("Invalid Start time", startTime > 0);

        long finishTime = attempt.getFinishTime();
        assertTrue("Task FINISH_TIME is < START_TIME in history file",
            startTime < finishTime);

        // Make sure that the Task type exists and it is valid
        TaskType type = attempt.getTaskType();
        assertTrue("Unknown Task type \"" + type + "\" is seen in " +
                   "history file for task attempt " + id,
                   (type.equals(TaskType.MAP) || type.equals(TaskType.REDUCE) ||
                    type.equals(TaskType.JOB_CLEANUP) || 
                    type.equals(TaskType.JOB_SETUP)));

        // Validate task status
        String status = attempt.getTaskStatus();
        assertTrue("Unexpected TASK_STATUS \"" + status + "\" is seen in" +
                   " history file for task attempt " + id,
                   (status.equals(TaskStatus.State.SUCCEEDED.toString()) ||
                    status.equals(TaskStatus.State.FAILED.toString()) ||
                    status.equals(TaskStatus.State.KILLED.toString())));

        // Successful Reduce Task Attempts should have valid SHUFFLE_FINISHED
        // time and SORT_FINISHED time
        if (type.equals(TaskType.REDUCE) && 
            status.equals(TaskStatus.State.SUCCEEDED.toString())) {
          long shuffleFinishTime = attempt.getShuffleFinishTime();
          assertTrue(startTime < shuffleFinishTime);
          
          long sortFinishTime = attempt.getSortFinishTime();
          assertTrue(shuffleFinishTime < sortFinishTime);
        }
        else if (type.equals(TaskType.MAP) && 
            status.equals(TaskStatus.State.SUCCEEDED.toString())) {
          // Successful MAP Task Attempts should have valid MAP_FINISHED time
         long mapFinishTime = attempt.getMapFinishTime();
         assertTrue(startTime < mapFinishTime);
        }

        // check if hostname is valid
        String hostname = attempt.getHostname();
        Matcher m = hostNamePattern.matcher(hostname);
        assertTrue("Unexpected Host name of task attempt " + id, m.matches());

        // check if trackername is valid
        String trackerName = attempt.getTrackerName();
        m = trackerNamePattern.matcher(trackerName);
        assertTrue("Unexpected tracker name of task attempt " + id,
                   m.matches());

        if (!status.equals("KILLED")) {
          // check if http port is valid
          int httpPort = attempt.getHttpPort();
          assertTrue(httpPort > 0);
        }
        
        // check if counters are parsable
        Counters counters = attempt.getCounters();
        assertNotNull(counters);
      }
    }
  }

  /**
   * Returns the conf file name in the same
   * @param path path of the jobhistory file
   * @param running whether the job is running or completed
   */
  private static Path getPathForConf(Path path, Path dir) {
    String parts[] = path.getName().split("_");
    //TODO this is a hack :(
    // jobtracker-hostname_jobtracker-identifier_
    String id = parts[0] + "_" + parts[1] + "_" + parts[2];
    return new Path(dir, id + "_conf.xml");
  }

  /**
   *  Validates the format of contents of history file
   *  (1) history file exists and in correct location
   *  (2) Verify if the history file is parsable
   *  (3) Validate the contents of history file
   *     (a) Format of all TIMEs are checked against a regex
   *     (b) validate legality/format of job level key, values
   *     (c) validate legality/format of task level key, values
   *     (d) validate legality/format of attempt level key, values
   *     (e) check if all the TaskAttempts, Tasks started are finished.
   *         Check finish of each TaskAttemptID against its start to make sure
   *         that all TaskAttempts, Tasks started are indeed finished and the
   *         history log lines are in the proper order.
   *         We want to catch ordering of history lines like
   *            Task START
   *            Attempt START
   *            Task FINISH
   *            Attempt FINISH
   *         (speculative execution is turned off for this).
   * @param id job id
   * @param conf job conf
   */
  public static void validateJobHistoryFileFormat(JobHistory jobHistory,
      JobID id, JobConf conf,
                 String status, boolean splitsCanBeEmpty) throws IOException  {

    // Get the history file name
    Path dir = jobHistory.getCompletedJobHistoryLocation();
    String logFileName = getDoneFile(jobHistory, conf, id, dir);

    // Framework history log file location
    Path logFile = new Path(dir, logFileName);
    FileSystem fileSys = logFile.getFileSystem(conf);
 
    // Check if the history file exists
    assertTrue("History file does not exist", fileSys.exists(logFile));

    JobHistoryParser parser = new JobHistoryParser(fileSys, 
        logFile.toUri().getPath());
    JobHistoryParser.JobInfo jobInfo = parser.parse();

    // validate format of job level key, values
    validateJobLevelKeyValuesFormat(jobInfo, status);

    // validate format of task level key, values
    validateTaskLevelKeyValuesFormat(jobInfo, splitsCanBeEmpty);

    // validate format of attempt level key, values
    validateTaskAttemptLevelKeyValuesFormat(jobInfo);

    // check if all the TaskAttempts, Tasks started are finished for
    // successful jobs
    if (status.equals("SUCCEEDED")) {
      // Make sure that the lists in taskIDsToAttemptIDs are empty.
      for(Iterator<String> it = 
        taskIDsToAttemptIDs.keySet().iterator();it.hasNext();) {
        String taskid = it.next();
        assertTrue("There are some Tasks which are not finished in history " +
                   "file.", taskEnds.contains(taskid));
        List<String> attemptIDs = taskIDsToAttemptIDs.get(taskid);
        if(attemptIDs != null) {
          assertTrue("Unexpected. TaskID " + taskid + " has task attempt(s)" +
                     " that are not finished.", (attemptIDs.size() == 1));
        }
      }
    }
  }

  // Validate Job Level Keys, Values read from history file by
  // comparing them with the actual values from JT.
  private static void validateJobLevelKeyValues(MiniMRCluster mr,
          RunningJob job, JobInfo jobInfo, JobConf conf) throws IOException  {

    JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
    JobInProgress jip = jt.getJob(job.getID());

    assertTrue("SUBMIT_TIME of job obtained from history file did not " +
               "match the expected value", jip.getStartTime() ==
               jobInfo.getSubmitTime());

    assertTrue("LAUNCH_TIME of job obtained from history file did not " +
               "match the expected value", jip.getLaunchTime() ==
               jobInfo.getLaunchTime());

    assertTrue("FINISH_TIME of job obtained from history file did not " +
               "match the expected value", jip.getFinishTime() ==
               jobInfo.getFinishTime());

    assertTrue("Job Status of job obtained from history file did not " +
               "match the expected value",
               jobInfo.getJobStatus().equals("SUCCEEDED"));

    assertTrue("Job Priority of job obtained from history file did not " +
               "match the expected value", jip.getPriority().toString().equals(
               jobInfo.getPriority()));

    assertTrue("Job Name of job obtained from history file did not " +
               "match the expected value", 
               conf.getJobName().equals(
               jobInfo.getJobname()));
    String user = UserGroupInformation.getCurrentUser().getUserName();
    assertTrue("User Name of job obtained from history file did not " +
               "match the expected value", 
               user.equals(
               jobInfo.getUsername()));

    // Validate job counters
    Counters c = new Counters(jip.getCounters());
    Counters jiCounters = jobInfo.getTotalCounters();
    assertTrue("Counters of job obtained from history file did not " +
               "match the expected value",
               c.equals(jiCounters));

    // Validate number of total maps, total reduces, finished maps,
    // finished reduces, failed maps, failed recudes
    assertTrue("Unexpected number of total maps in history file",
               jobInfo.getTotalMaps() == jip.desiredMaps());

    assertTrue("Unexpected number of total reduces in history file",
               jobInfo.getTotalReduces() == jip.desiredReduces());

    assertTrue("Unexpected number of finished maps in history file",
               jobInfo.getFinishedMaps() == jip.finishedMaps());

    assertTrue("Unexpected number of finished reduces in history file",
               jobInfo.getFinishedReduces() == jip.finishedReduces());

    assertTrue("Unexpected number of failed maps in history file",
               jobInfo.getFailedMaps() == jip.failedMapTasks);

    assertTrue("Unexpected number of failed reduces in history file",
               jobInfo.getFailedReduces() == jip.failedReduceTasks);
  }

  // Validate Task Level Keys, Values read from history file by
  // comparing them with the actual values from JT.
  private static void validateTaskLevelKeyValues(MiniMRCluster mr,
      RunningJob job, JobInfo jobInfo) throws IOException  {

    JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
    JobInProgress jip = jt.getJob(job.getID());

    // Get the 1st map, 1st reduce, cleanup & setup taskIDs and
    // validate their history info
    TaskID mapTaskId = new TaskID(job.getID(), TaskType.MAP, 0);
    TaskID reduceTaskId = new TaskID(job.getID(), TaskType.REDUCE, 0);

    TaskInProgress cleanups[] = jip.cleanup;
    TaskID cleanupTaskId;
    if (cleanups[0].isComplete()) {
      cleanupTaskId = cleanups[0].getTIPId();
    }
    else {
      cleanupTaskId = cleanups[1].getTIPId();
    }

    TaskInProgress setups[] = jip.setup;
    TaskID setupTaskId;
    if (setups[0].isComplete()) {
      setupTaskId = setups[0].getTIPId();
    }
    else {
      setupTaskId = setups[1].getTIPId();
    }

    Map<TaskID, TaskInfo> tasks = jobInfo.getAllTasks();

    // validate info of the 4 tasks(cleanup, setup, 1st map, 1st reduce)    

    for (TaskInfo task : tasks.values()) {
      TaskID tid = task.getTaskId();

      if (tid.equals(mapTaskId) ||
          tid.equals(reduceTaskId) ||
          tid.equals(cleanupTaskId) ||
          tid.equals(setupTaskId)) {

        TaskInProgress tip = jip.getTaskInProgress
        (org.apache.hadoop.mapred.TaskID.downgrade(tid));
        assertTrue("START_TIME of Task " + tid + " obtained from history " +
            "file did not match the expected value", 
            tip.getExecStartTime() ==
              task.getStartTime());

        assertTrue("FINISH_TIME of Task " + tid + " obtained from history " +
            "file did not match the expected value",
            tip.getExecFinishTime() ==
              task.getFinishTime());

        if (tid == mapTaskId) {//check splits only for map task
          assertTrue("Splits of Task " + tid + " obtained from history file " +
              " did not match the expected value",
              tip.getSplitNodes().equals(task.getSplitLocations()));
        }

        TaskAttemptID attemptId = tip.getSuccessfulTaskid();
        TaskStatus ts = tip.getTaskStatus(
            org.apache.hadoop.mapred.TaskAttemptID.downgrade(attemptId));

        // Validate task counters
        Counters c = new Counters(ts.getCounters());
        assertTrue("Counters of Task " + tid + " obtained from history file " +
            " did not match the expected value",
            c.equals(task.getCounters()));
      }
    }
  }

  // Validate Task Attempt Level Keys, Values read from history file by
  // comparing them with the actual values from JT.
  private static void validateTaskAttemptLevelKeyValues(MiniMRCluster mr,
                      RunningJob job, JobInfo jobInfo) throws IOException  {

    JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
    JobInProgress jip = jt.getJob(job.getID());

    Map<TaskID, TaskInfo> tasks = jobInfo.getAllTasks();

    // For each task
    for (TaskInfo task : tasks.values()) {
      // validate info of each attempt
      for (TaskAttemptInfo attempt : task.getAllTaskAttempts().values()) {

        TaskAttemptID attemptId = attempt.getAttemptId();
        TaskID tid = attemptId.getTaskID();

        TaskInProgress tip = jip.getTaskInProgress
        (org.apache.hadoop.mapred.TaskID.downgrade(tid));
        
        TaskStatus ts = tip.getTaskStatus(
            org.apache.hadoop.mapred.TaskAttemptID.downgrade(attemptId));

        // Validate task attempt start time
        assertTrue("START_TIME of Task attempt " + attemptId +
            " obtained from " +
            "history file did not match the expected value",
            ts.getStartTime() == attempt.getStartTime());

        // Validate task attempt finish time
        assertTrue("FINISH_TIME of Task attempt " + attemptId +
                   " obtained from " +
                   "history file " + ts.getFinishTime() + 
                   " did not match the expected value, " +
                   attempt.getFinishTime(),
            ts.getFinishTime() == attempt.getFinishTime());


        TaskTrackerStatus ttStatus =
          jt.getTaskTrackerStatus(ts.getTaskTracker());

        if (ttStatus != null) {
          assertTrue("http port of task attempt " + attemptId +
                     " obtained from " +
                     "history file did not match the expected value",
                     ttStatus.getHttpPort() ==
                     attempt.getHttpPort());

          if (attempt.getTaskStatus().equals("SUCCEEDED")) {
            String ttHostname = jt.getNode(ttStatus.getHost()).toString();

            // check if hostname is valid
            assertTrue("Host name of task attempt " + attemptId +
                       " obtained from" +
                       " history file did not match the expected value",
                       ttHostname.equals(attempt.getHostname()));
          }
        }
        if (attempt.getTaskStatus().equals("SUCCEEDED")) {
          // Validate SHUFFLE_FINISHED time and SORT_FINISHED time of
          // Reduce Task Attempts
          if (attempt.getTaskType().equals("REDUCE")) {
            assertTrue("SHUFFLE_FINISHED time of task attempt " + attemptId +
                     " obtained from history file did not match the expected" +
                     " value", ts.getShuffleFinishTime() ==
                     attempt.getShuffleFinishTime());
            assertTrue("SORT_FINISHED time of task attempt " + attemptId +
                     " obtained from history file did not match the expected" +
                     " value", ts.getSortFinishTime() ==
                     attempt.getSortFinishTime());
          }

          //Validate task counters
          Counters c = new Counters(ts.getCounters());
          assertTrue("Counters of Task Attempt " + attemptId + " obtained from " +
                     "history file did not match the expected value",
               c.equals(attempt.getCounters()));
        }
        
        // check if tracker name is valid
        assertTrue("Tracker name of task attempt " + attemptId +
                   " obtained from " +
                   "history file did not match the expected value",
                   ts.getTaskTracker().equals(attempt.getTrackerName()));
      }
    }
  }

  /**
   * Checks if the history file content is as expected comparing with the
   * actual values obtained from JT.
   * Job Level, Task Level and Task Attempt Level Keys, Values are validated.
   * @param job RunningJob object of the job whose history is to be validated
   * @param conf job conf
   */
  public static void validateJobHistoryFileContent(MiniMRCluster mr,
                              RunningJob job, JobConf conf) throws IOException  {

    JobID id = job.getID();
    JobHistory jobHistory = 
      mr.getJobTrackerRunner().getJobTracker().getJobHistory();
    Path doneDir = jobHistory.getCompletedJobHistoryLocation();
    // Get the history file name
    String logFileName = getDoneFile(jobHistory, conf, id, doneDir);

    // Framework history log file location
    Path logFile = new Path(doneDir, logFileName);
    FileSystem fileSys = logFile.getFileSystem(conf);
 
    // Check if the history file exists
    assertTrue("History file does not exist", fileSys.exists(logFile));

    JobHistoryParser parser = new JobHistoryParser(fileSys,
        logFile.toUri().getPath());
    
    JobHistoryParser.JobInfo jobInfo = parser.parse();
    // Now the history file contents are available in jobInfo. Let us compare
    // them with the actual values from JT.
    validateJobLevelKeyValues(mr, job, jobInfo, conf);
    validateTaskLevelKeyValues(mr, job, jobInfo);
    validateTaskAttemptLevelKeyValues(mr, job, jobInfo);
  }

  public void testDoneFolderOnHDFS() throws IOException, InterruptedException {
    MiniMRCluster mr = null;
    try {
      JobConf conf = new JobConf();
      // keep for less time
      conf.setLong("mapred.jobtracker.retirejob.check", 1000);
      conf.setLong("mapred.jobtracker.retirejob.interval", 1000);

      //set the done folder location
      String doneFolder = "history_done";
      conf.set(JTConfig.JT_JOBHISTORY_COMPLETED_LOCATION, doneFolder);

      String logDir =
        "file:///" + new File(System.getProperty("hadoop.log.dir")).
        getAbsolutePath() + File.separator + "history";

      Path logDirPath = new Path(logDir);
      FileSystem logDirFs = logDirPath.getFileSystem(conf);
      //there may be some stale files, clean them
      if (logDirFs.exists(logDirPath)) {
        boolean deleted = logDirFs.delete(logDirPath, true);
        LOG.info(logDirPath + " deleted " + deleted);
      }

      logDirFs.mkdirs(logDirPath);
      assertEquals("No of file in logDir not correct", 0,
          logDirFs.listStatus(logDirPath).length);
      logDirFs.create(new Path(logDirPath, "f1"));
      logDirFs.create(new Path(logDirPath, "f2"));
      assertEquals("No of file in logDir not correct", 2,
          logDirFs.listStatus(logDirPath).length);
      
      MiniDFSCluster dfsCluster = new MiniDFSCluster(conf, 2, true, null);
      mr = new MiniMRCluster(2, dfsCluster.getFileSystem().getUri().toString(),
          3, null, null, conf);

      assertEquals("Files in logDir did not move to DONE folder",
          0, logDirFs.listStatus(logDirPath).length);

      JobHistory jobHistory = 
        mr.getJobTrackerRunner().getJobTracker().getJobHistory();
      Path doneDir = jobHistory.getCompletedJobHistoryLocation();

      assertEquals("Files in DONE dir not correct",
          2, doneDir.getFileSystem(conf).listStatus(doneDir).length);

      // run the TCs
      conf = mr.createJobConf();

      FileSystem fs = FileSystem.get(conf);
      // clean up
      fs.delete(new Path("succeed"), true);

      Path inDir = new Path("succeed/input");
      Path outDir = new Path("succeed/output");

      //Disable speculative execution
      conf.setSpeculativeExecution(false);

      // Make sure that the job is not removed from memory until we do finish
      // the validation of history file content
      conf.setInt("mapred.jobtracker.completeuserjobs.maximum", 10);

      // Run a job that will be succeeded and validate its history file
      RunningJob job = UtilsForTests.runJobSucceed(conf, inDir, outDir);
      
      assertEquals("History DONE folder not correct", 
          doneFolder, doneDir.getName());
      JobID id = job.getID();
      String logFileName = getDoneFile(jobHistory, conf, id, doneDir);

      // Framework history log file location
      Path logFile = new Path(doneDir, logFileName);
      FileSystem fileSys = logFile.getFileSystem(conf);

      Cluster cluster = new Cluster(conf);
      assertEquals("Client returned wrong history url", logFile.toString(), 
          cluster.getJobHistoryUrl(id));
   
      // Check if the history file exists
      assertTrue("History file does not exist", fileSys.exists(logFile));

      // check if the corresponding conf file exists
      Path confFile = getPathForConf(logFile, doneDir);
      assertTrue("Config for completed jobs doesnt exist", 
                 fileSys.exists(confFile));

      // check if the file exists in a done folder
      assertTrue("Completed job config doesnt exist in the done folder", 
                 doneDir.getName().equals(confFile.getParent().getName()));

      // check if the file exists in a done folder
      assertTrue("Completed jobs doesnt exist in the done folder", 
                 doneDir.getName().equals(logFile.getParent().getName()));
      

      // check if the job file is removed from the history location 
      Path runningJobsHistoryFolder = logFile.getParent().getParent();
      Path runningJobHistoryFilename = 
        new Path(runningJobsHistoryFolder, logFile.getName());
      Path runningJobConfFilename = 
        new Path(runningJobsHistoryFolder, confFile.getName());
      assertFalse("History file not deleted from the running folder", 
                  fileSys.exists(runningJobHistoryFilename));
      assertFalse("Config for completed jobs not deleted from running folder", 
                  fileSys.exists(runningJobConfFilename));

      validateJobHistoryFileFormat(jobHistory,
          job.getID(), conf, "SUCCEEDED", false);
      validateJobHistoryFileContent(mr, job, conf);

      // get the job conf filename
    } finally {
      if (mr != null) {
        cleanupLocalFiles(mr);
        mr.shutdown();
      }
    }
  }

  /** Run a job that will be succeeded and validate its history file format
   *  and its content.
   */
  public void testJobHistoryFile() throws IOException {
    MiniMRCluster mr = null;
    try {
      JobConf conf = new JobConf();
      // keep for less time
      conf.setLong("mapred.jobtracker.retirejob.check", 1000);
      conf.setLong("mapred.jobtracker.retirejob.interval", 1000);

      //set the done folder location
      String doneFolder = TEST_ROOT_DIR + "history_done";
      conf.set(JTConfig.JT_JOBHISTORY_COMPLETED_LOCATION, doneFolder);
      
      mr = new MiniMRCluster(2, "file:///", 3, null, null, conf);

      // run the TCs
      conf = mr.createJobConf();

      FileSystem fs = FileSystem.get(conf);
      // clean up
      fs.delete(new Path(TEST_ROOT_DIR + "/succeed"), true);

      Path inDir = new Path(TEST_ROOT_DIR + "/succeed/input");
      Path outDir = new Path(TEST_ROOT_DIR + "/succeed/output");

      //Disable speculative execution
      conf.setSpeculativeExecution(false);

      // Make sure that the job is not removed from memory until we do finish
      // the validation of history file content
      conf.setInt("mapred.jobtracker.completeuserjobs.maximum", 10);

      // Run a job that will be succeeded and validate its history file
      RunningJob job = UtilsForTests.runJobSucceed(conf, inDir, outDir);
      JobHistory jobHistory = 
        mr.getJobTrackerRunner().getJobTracker().getJobHistory();
      Path doneDir = jobHistory.getCompletedJobHistoryLocation();
      assertEquals("History DONE folder not correct", 
          doneFolder, doneDir.toString());
      JobID id = job.getID();
      String logFileName = getDoneFile(jobHistory, conf, id, doneDir);

      // Framework history log file location
      Path logFile = new Path(doneDir, logFileName);
      FileSystem fileSys = logFile.getFileSystem(conf);
   
      // Check if the history file exists
      assertTrue("History file does not exist", fileSys.exists(logFile));

      // check if the corresponding conf file exists
      Path confFile = getPathForConf(logFile, doneDir);
      assertTrue("Config for completed jobs doesnt exist", 
                 fileSys.exists(confFile));

      // check if the conf file exists in a done folder
      assertTrue("Completed job config doesnt exist in the done folder", 
                 doneDir.getName().equals(confFile.getParent().getName()));

      // check if the file exists in a done folder
      assertTrue("Completed jobs doesnt exist in the done folder", 
                 doneDir.getName().equals(logFile.getParent().getName()));

      // check if the job file is removed from the history location 
      Path runningJobsHistoryFolder = logFile.getParent().getParent();
      Path runningJobHistoryFilename = 
        new Path(runningJobsHistoryFolder, logFile.getName());
      Path runningJobConfFilename = 
        new Path(runningJobsHistoryFolder, confFile.getName());
      assertFalse("History file not deleted from the running folder", 
                  fileSys.exists(runningJobHistoryFilename));
      assertFalse("Config for completed jobs not deleted from running folder", 
                  fileSys.exists(runningJobConfFilename));

      validateJobHistoryFileFormat(jobHistory, job.getID(), conf, 
          "SUCCEEDED", false);
      validateJobHistoryFileContent(mr, job, conf);

      // get the job conf filename
      JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
      String name = jt.getLocalJobFilePath(job.getID());
      File file = new File(name);

      // check if the file get deleted
      while (file.exists()) {
        LOG.info("Waiting for " + file + " to be deleted");
        UtilsForTests.waitFor(100);
      }
    } finally {
      if (mr != null) {
        cleanupLocalFiles(mr);
        mr.shutdown();
      }
    }
  }

  //Returns the file in the done folder
  //Waits for sometime to get the file moved to done
  private static String getDoneFile(JobHistory jobHistory, 
      JobConf conf, JobID id, 
      Path doneDir) throws IOException {
    String name = null;
    String user = UserGroupInformation.getCurrentUser().getUserName();
    for (int i = 0; name == null && i < 20; i++) {
      Path path = JobHistory.getJobHistoryFile(
          jobHistory.getCompletedJobHistoryLocation(), id, user);
      if (path.getFileSystem(conf).exists(path)) {
        name = path.toString();
      }
      UtilsForTests.waitFor(1000);
    }
    assertNotNull("Job history file not created", name);
    return name;
  }

  private void cleanupLocalFiles(MiniMRCluster mr) 
  throws IOException {
    Configuration conf = mr.createJobConf();
    JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
    Path sysDir = new Path(jt.getSystemDir());
    FileSystem fs = sysDir.getFileSystem(conf);
    fs.delete(sysDir, true);
    Path jobHistoryDir = 
      mr.getJobTrackerRunner().getJobTracker().getJobHistory().
      getJobHistoryLocation();
    fs = jobHistoryDir.getFileSystem(conf);
    fs.delete(jobHistoryDir, true);
  }

  /**
   * Checks if the history file has expected job status
   * @param id job id
   * @param conf job conf
   */
  private static void validateJobHistoryJobStatus(JobHistory jobHistory,
      JobID id, JobConf conf, String status) throws IOException  {

    // Get the history file name
    Path doneDir = jobHistory.getCompletedJobHistoryLocation();
    String logFileName = getDoneFile(jobHistory, conf, id, doneDir);
    
    // Framework history log file location
    Path logFile = new Path(doneDir, logFileName);
    FileSystem fileSys = logFile.getFileSystem(conf);
 
    // Check if the history file exists
    assertTrue("History file does not exist", fileSys.exists(logFile));

    // check history file permission
    assertTrue("History file permissions does not match", 
    fileSys.getFileStatus(logFile).getPermission().equals(
       new FsPermission(JobHistory.HISTORY_FILE_PERMISSION)));
    
    JobHistoryParser parser = new JobHistoryParser(fileSys, 
        logFile.toUri().getPath());
    JobHistoryParser.JobInfo jobInfo = parser.parse();
    

    assertTrue("Job Status read from job history file is not the expected" +
         " status", status.equals(jobInfo.getJobStatus()));
  }

  // run jobs that will be (1) succeeded (2) failed (3) killed
  // and validate job status read from history file in each case
  public void testJobHistoryJobStatus() throws IOException {
    MiniMRCluster mr = null;
    try {
      mr = new MiniMRCluster(2, "file:///", 3);

      // run the TCs
      JobConf conf = mr.createJobConf();

      FileSystem fs = FileSystem.get(conf);
      // clean up
      fs.delete(new Path(TEST_ROOT_DIR + "/succeedfailkilljob"), true);

      Path inDir = new Path(TEST_ROOT_DIR + "/succeedfailkilljob/input");
      Path outDir = new Path(TEST_ROOT_DIR + "/succeedfailkilljob/output");

      // Run a job that will be succeeded and validate its job status
      // existing in history file
      RunningJob job = UtilsForTests.runJobSucceed(conf, inDir, outDir);
      
      JobHistory jobHistory = 
        mr.getJobTrackerRunner().getJobTracker().getJobHistory();
      validateJobHistoryJobStatus(jobHistory, job.getID(), conf, 
          JobStatus.getJobRunState(JobStatus.SUCCEEDED));
      
      // Run a job that will be failed and validate its job status
      // existing in history file
      job = UtilsForTests.runJobFail(conf, inDir, outDir);
      validateJobHistoryJobStatus(jobHistory, job.getID(), conf, 
          JobStatus.getJobRunState(JobStatus.FAILED));
      
      // Run a job that will be killed and validate its job status
      // existing in history file
      job = UtilsForTests.runJobKill(conf, inDir, outDir);
      validateJobHistoryJobStatus(jobHistory, job.getID(), conf,
          JobStatus.getJobRunState(JobStatus.KILLED));
      
    } finally {
      if (mr != null) {
        cleanupLocalFiles(mr);
        mr.shutdown();
      }
    }
  }

  public void testHistoryInitWithCorruptFiles() throws IOException {
    MiniMRCluster mr = null;
    try {
      JobConf conf = new JobConf();
      Path historyDir = new Path(System.getProperty("test.build.data", "."),
      "history");
      conf.set(JTConfig.JT_JOBHISTORY_LOCATION,
          historyDir.toString());
      conf.setUser("user");

      FileSystem localFs = FileSystem.getLocal(conf);
      
      //there may be some stale files, clean them
      if (localFs.exists(historyDir)) {
        boolean deleted = localFs.delete(historyDir, true);
        LOG.info(historyDir + " deleted " + deleted);
      }

      // Start the cluster, create a history file
      mr = new MiniMRCluster(0, "file:///", 3, null, null, conf);
      JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
      JobHistory jh = jt.getJobHistory();
      final JobID jobId = JobID.forName("job_200809171136_0001");
      jh.setupEventWriter(jobId, conf);
      Map<JobACL, AccessControlList> jobACLs =
          new HashMap<JobACL, AccessControlList>();
      JobSubmittedEvent jse =
        new JobSubmittedEvent(jobId, "job", "user", 12345, "path", jobACLs);
      jh.logEvent(jse, jobId);
      jh.closeWriter(jobId);

      // Corrupt the history file. User RawLocalFileSystem so that we
      // do keep the original CRC file intact.
      String historyFileName = jobId.toString() + "_" + "user";
      Path historyFilePath = new Path (historyDir.toString(), historyFileName);

      RawLocalFileSystem fs = (RawLocalFileSystem)
        FileSystem.getLocal(conf).getRaw();

      FSDataOutputStream out = fs.create(historyFilePath, true);
      byte[] corruptData = new byte[32];
      new Random().nextBytes(corruptData);
      out.write (corruptData, 0, 32);
      out.close();

      // Stop and start the tracker. The tracker should come up nicely
      mr.stopJobTracker();
      mr.startJobTracker();
      jt = mr.getJobTrackerRunner().getJobTracker();
      assertNotNull("JobTracker did not come up", jt );
      jh = jt.getJobHistory();
      assertNotNull("JobHistory did not get initialized correctly", jh);

      // Only the done folder should remain in the history directory
      assertEquals("Files in logDir did not move to DONE folder",
          1, historyDir.getFileSystem(conf).listStatus(historyDir).length);
    } finally {
      if (mr != null) {
        cleanupLocalFiles(mr);
        mr.shutdown();
      }
    }
  }
}
