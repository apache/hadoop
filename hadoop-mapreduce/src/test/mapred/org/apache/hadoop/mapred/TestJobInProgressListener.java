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
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.TestNoJobSetupCleanup;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Test whether the JobInProgressListeners are informed as expected.
 */
public class TestJobInProgressListener extends TestCase {
  private static final Log LOG = 
    LogFactory.getLog(TestJobInProgressListener.class);
  private static String TEST_ROOT_DIR = new File(System.getProperty(
          "test.build.data", "/tmp")).toURI().toString().replace(' ', '+');
  private final Path testDir = 
    new Path(TEST_ROOT_DIR, "test-jip-listener-update");
  private static MiniMRCluster mr;
  private static JobTracker jobtracker;
  private static JobConf conf;
  private static MyScheduler myScheduler;

  public static Test suite() {
    TestSetup setup = 
      new TestSetup(new TestSuite(TestJobInProgressListener.class)) {
      @Override
      protected void setUp() throws Exception {
        conf = new JobConf();   
        conf.setClass(JTConfig.JT_TASK_SCHEDULER, MyScheduler.class,
                      TaskScheduler.class);
        mr = new MiniMRCluster(1, "file:///", 1, null, null, conf);
        jobtracker = mr.getJobTrackerRunner().getJobTracker();
        myScheduler = (MyScheduler)jobtracker.getScheduler();
        conf = mr.createJobConf();
      }
      
      @Override
      protected void tearDown() throws Exception {
        conf = null;
        try {
          mr.shutdown();
        } catch (Exception e) {
          LOG.info("Error in shutting down the MR cluster", e);
        }
        jobtracker = null;
        myScheduler.terminate();
      }
    };
    return setup;
  }
  
  /**
   * This test case tests if external updates to JIP do not result into 
   * undesirable effects
   * Test is as follows
   *   - submit 2 jobs of normal priority. job1 is a waiting job which waits and
   *     blocks the cluster
   *   - change one parameter of job2 such that the job bumps up in the queue
   *   - check if the queue looks ok
   *   
   */
  public void testJobQueueChanges() throws IOException {
    LOG.info("Testing job queue changes");
    
    // stop the job initializer
    myScheduler.stopInitializer();
    
    JobQueueJobInProgressListener myListener = 
      new JobQueueJobInProgressListener();
    
    // add the listener
    jobtracker.addJobInProgressListener(myListener);
    
    Path inDir = new Path(testDir, "input");
    Path outputDir1 = new Path(testDir, "output1");
    Path outputDir2 = new Path(testDir, "output2");
        
    RunningJob rJob1 = 
      UtilsForTests.runJob(conf, inDir, outputDir1, 1, 0);
    LOG.info("Running job " + rJob1.getID().toString());
    
    RunningJob rJob2 = 
      UtilsForTests.runJob(conf, inDir, outputDir2, 1, 0);
    LOG.info("Running job " + rJob2.getID().toString());
    
    // I. Check job-priority change
    LOG.info("Testing job priority changes");
    
    // bump up job2's priority
    LOG.info("Increasing job2's priority to HIGH");
    rJob2.setJobPriority("HIGH");
    
    // check if the queue is sane
    assertTrue("Priority change garbles the queue", 
               myListener.getJobQueue().size() == 2);
    
    JobInProgress[] queue = 
      myListener.getJobQueue().toArray(new JobInProgress[0]);
    
    // check if the bump has happened
    assertTrue("Priority change failed to bump up job2 in the queue", 
               queue[0].getJobID().equals(rJob2.getID()));
    
    assertTrue("Priority change failed to bump down job1 in the queue", 
               queue[1].getJobID().equals(rJob1.getID()));
    
    assertEquals("Priority change has garbled the queue", 
                 2, queue.length);
    
    // II. Check start-time change
    LOG.info("Testing job start-time changes");
    
    // reset the priority which will make the order as
    //  - job1
    //  - job2
    // this will help in bumping job2 on start-time change
    LOG.info("Increasing job2's priority to NORMAL"); 
    rJob2.setJobPriority("NORMAL");
    
    // create the change event
    JobInProgress jip2 = jobtracker.getJob(rJob2.getID());
    JobInProgress jip1 = jobtracker.getJob(rJob1.getID());
    
    JobStatus prevStatus = (JobStatus)jip2.getStatus().clone();
    
    // change job2's start-time and the status
    jip2.startTime =  jip1.startTime - 1;
    jip2.status.setStartTime(jip2.startTime);
    
    
    JobStatus newStatus = (JobStatus)jip2.getStatus().clone();
    
    // inform the listener
    LOG.info("Updating the listener about job2's start-time change");
    JobStatusChangeEvent event = 
      new JobStatusChangeEvent(jip2, EventType.START_TIME_CHANGED, 
                              prevStatus, newStatus);
    myListener.jobUpdated(event);
    
    // check if the queue is sane
    assertTrue("Start time change garbles the queue", 
               myListener.getJobQueue().size() == 2);
    
    queue = myListener.getJobQueue().toArray(new JobInProgress[0]);
    
    // check if the bump has happened
    assertTrue("Start time change failed to bump up job2 in the queue", 
               queue[0].getJobID().equals(rJob2.getID()));
    
    assertTrue("Start time change failed to bump down job1 in the queue", 
               queue[1].getJobID().equals(rJob1.getID()));
    
    assertEquals("Start time change has garbled the queue", 
                 2, queue.length);
  }

  /**
   * Check the queue status upon
   *   - failed job
   *   - killed job
   *   - successful job
   */
  public void testJobCompletion() throws Exception {
    MyListener mainListener = new MyListener();
    jobtracker.addJobInProgressListener(mainListener);
    
    // stop the job initializer
    myScheduler.stopInitializer();
    
    // check queued jobs
    testQueuedJobKill(conf, mainListener);
    
    myScheduler.startInitializer();
    
    // check the queue state for job states
    testFailedJob(conf, mainListener);
    
    testKilledJob(conf, mainListener);
    
    testSuccessfulJob(conf, mainListener);
  }
  
  // A listener that inits the tasks one at a time and also listens to the 
  // events
  public static class MyListener extends JobInProgressListener {
    private List<JobInProgress> wjobs = new ArrayList<JobInProgress>();
    private List<JobInProgress> rjobs = new ArrayList<JobInProgress>();
    // list of job added to the wait queue
    private List<JobID> wjobsAdded = new ArrayList<JobID>();
    // list of job added to the running queue
    private List<JobID> rjobsAdded = new ArrayList<JobID>();
    
    public boolean contains (JobID id) {
      return contains(id, true) || contains(id, false);
    }
    
    public boolean contains (JobID id, boolean waiting) {
      if (!wjobsAdded.contains(id)) {
        throw new RuntimeException("Job " + id + " not seen in waiting queue");
      }
      if (!waiting) {
        if (!rjobsAdded.contains(id)) {
          throw new RuntimeException("Job " + id + " not seen in run queue");
        }
      }
      List<JobInProgress> queue = waiting ? wjobs : rjobs;
      for (JobInProgress job : queue) {
        if (job.getJobID().equals(id)) {
          return true;
        }
      }
      return false;
    }
    
    public void jobAdded(JobInProgress job) {
      LOG.info("Job " + job.getJobID().toString() + " added");
      wjobs.add(job);
      wjobsAdded.add(job.getJobID());
    }
    
    public void jobRemoved(JobInProgress job) {
      LOG.info("Job " + job.getJobID().toString() + " removed");
      wjobs.remove(job);
      rjobs.remove(job);
    }
    
    public void jobUpdated(JobChangeEvent event) {
      LOG.info("Job " + event.getJobInProgress().getJobID().toString() + " updated");
      // remove the job is the event is for a completed job
      if (event instanceof JobStatusChangeEvent) {
        JobStatusChangeEvent statusEvent = (JobStatusChangeEvent)event;
        if (statusEvent.getEventType() == EventType.RUN_STATE_CHANGED) {
          // check if the state changes from 
          // RUNNING->COMPLETE(SUCCESS/KILLED/FAILED)
          JobInProgress jip = event.getJobInProgress();
          String jobId = jip.getJobID().toString();
          if (jip.isComplete()) {
            LOG.info("Job " +  jobId + " deleted from the running queue");
            if (statusEvent.getOldStatus().getRunState() == JobStatus.PREP) {
              wjobs.remove(jip);
            } else {
              rjobs.remove(jip);
            }
          } else {
            // PREP->RUNNING
            LOG.info("Job " +  jobId + " deleted from the waiting queue");
            wjobs.remove(jip);
            rjobs.add(jip);
            rjobsAdded.add(jip.getJobID());
          }
        }
      }
    }
  }
  
  private void testFailedJob(JobConf job, MyListener myListener) 
  throws IOException {
    LOG.info("Testing job-fail");
    
    Path inDir = new Path(TEST_ROOT_DIR + "/jiplistenerfailjob/input");
    Path outDir = new Path(TEST_ROOT_DIR + "/jiplistenerfailjob/output");

    job.setNumMapTasks(1);
    job.setNumReduceTasks(0);
    job.setMaxMapAttempts(1);
    
    // submit a job that fails 
    RunningJob rJob = UtilsForTests.runJobFail(job, inDir, outDir);
    JobID id = rJob.getID();

    // check if the job failure was notified
    assertFalse("Missing event notification on failing a running job", 
                myListener.contains(id));

    // check if failed
    assertEquals("Job failed!", JobStatus.FAILED, rJob.getJobState());
  }

  private void testKilledJob(JobConf job, MyListener myListener) 
  throws IOException {
    LOG.info("Testing job-kill");
    
    Path inDir = new Path(TEST_ROOT_DIR + "/jiplistenerkilljob/input");
    Path outDir = new Path(TEST_ROOT_DIR + "/jiplistenerkilljob/output");

    job.setNumMapTasks(1);
    job.setNumReduceTasks(0);
    
    // submit and kill the job   
    RunningJob rJob = UtilsForTests.runJobKill(job, inDir, outDir);
    JobID id = rJob.getID();

    // check if the job failure was notified
    assertFalse("Missing event notification on killing a running job", 
                myListener.contains(id));

    // check if killed
    assertEquals("Job failed!", JobStatus.KILLED, rJob.getJobState());
  }

  private void testSuccessfulJob(JobConf job, MyListener myListener) 
  throws Exception {
    LOG.info("Testing job-success");
    
    Path inDir = new Path(TEST_ROOT_DIR + "/jiplistenerjob/input");
    Path outDir = new Path(TEST_ROOT_DIR + "/jiplistenerjob/output");

    job.setNumMapTasks(1);
    job.setNumReduceTasks(0);
    
    // submit the job   
    RunningJob rJob = UtilsForTests.runJobSucceed(job, inDir, outDir);
    
    // wait for the job to be successful
    rJob.waitForCompletion();
    
    // check if the job success was notified
    assertFalse("Missing event notification for a successful job", 
                myListener.contains(rJob.getID()));

    // check if successful
    assertEquals("Job failed!", JobStatus.SUCCEEDED, rJob.getJobState());
    
    // test if 0-task jobs with setup-cleanup works fine
    LOG.info("Testing job with no task job with setup and cleanup");
    
    job.setNumMapTasks(0);
    job.setNumReduceTasks(0);
    
    outDir = new Path(TEST_ROOT_DIR + "/jiplistenerjob/output-no-tasks");
    
    // submit the job   
    rJob = UtilsForTests.runJobSucceed(job, inDir, outDir);
    
    // wait for the job to be successful
    rJob.waitForCompletion();
    
    // check if the job success was notified
    assertFalse("Missing event notification for a successful job with no tasks", 
                myListener.contains(rJob.getID(), true));
    
    // check if successful
    assertEquals("Job failed!", JobStatus.SUCCEEDED, rJob.getJobState());
   
    // test if jobs with no tasks (0 maps, 0 red) update the listener properly
    LOG.info("Testing job with no-set-cleanup no task");
    
    outDir = new Path(TEST_ROOT_DIR + "/jiplistenerjob/output-no-tasks-no-set");
    
    Job j = MapReduceTestUtil.createJob(mr.createJobConf(), inDir, outDir, 0, 0);
    j.setJobSetupCleanupNeeded(false);
    j.setOutputFormatClass(TestNoJobSetupCleanup.MyOutputFormat.class);
    j.submit();
    j.waitForCompletion(true);
    
    JobID id = JobID.downgrade(j.getJobID());
    
    // check if the job is in the waiting queue
    assertFalse("Missing event notification on no-set-cleanup no task job", 
                myListener.contains(id, true));
    
    // check if the job is successful
    assertEquals("Job status doesnt reflect success", 
                 JobStatus.SUCCEEDED, rJob.getJobState());
  }
  
  /**
   * This scheduler never schedules any task as it doesnt init any task. So all
   * the jobs are queued forever.
   */
  public static class MyScheduler extends JobQueueTaskScheduler {

    @Override
    public synchronized void start() throws IOException {
      super.start();
    }

    void stopInitializer() throws IOException {
      // Remove the eager task initializer
      taskTrackerManager.removeJobInProgressListener(
          eagerTaskInitializationListener);
      // terminate it
      eagerTaskInitializationListener.terminate();
    }
    
    void startInitializer() throws IOException {
      eagerTaskInitializationListener = 
        new EagerTaskInitializationListener(getConf());
      eagerTaskInitializationListener.setTaskTrackerManager(taskTrackerManager);
      // start it
      eagerTaskInitializationListener.start();
      // add the eager task initializer
      taskTrackerManager.addJobInProgressListener(
          eagerTaskInitializationListener);
    }
  }
  
  private void testQueuedJobKill(JobConf conf, MyListener myListener) 
  throws IOException {
    LOG.info("Testing queued-job-kill");
    
    Path inDir = new Path(TEST_ROOT_DIR + "/jiplistenerqueuedjob/input");
    Path outDir = new Path(TEST_ROOT_DIR + "/jiplistener1ueuedjob/output");

    conf.setMapperClass(IdentityMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(0);
    RunningJob rJob = UtilsForTests.runJob(conf, inDir, outDir);
    JobID id = rJob.getID();
    LOG.info("Job : " + id.toString() + " submitted");
    
    // check if the job is in the waiting queue
    assertTrue("Missing event notification on submiting a job", 
                myListener.contains(id, true));
    
    // kill the job
    LOG.info("Killing job : " + id.toString());
    rJob.killJob();
    
    // check if the job is killed
    assertEquals("Job status doesnt reflect the kill-job action", 
                 JobStatus.KILLED, rJob.getJobState());

    // check if the job is correctly moved
    // from the waiting list
    assertFalse("Missing event notification on killing a waiting job", 
                myListener.contains(id, true));
  }
}
