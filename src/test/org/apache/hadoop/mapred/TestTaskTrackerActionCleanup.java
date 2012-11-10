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

import org.apache.hadoop.mapred.TaskTracker.RunningJob;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class TestTaskTrackerActionCleanup extends TestCase {
  String jtIdentifier = "201210122331";
  TestTaskTracker tt = null;

  @Before
  public void setUp() {
    tt = new TestTaskTracker();
  }

  @Test
  // check duplicate entries do not make it to the queue
  public void testDuplicateEntries() throws InterruptedException {

    KillJobAction action = new KillJobAction();
    // add the action twice
    tt.addActionToCleanup(action);
    tt.addActionToCleanup(action);

    checkItemCountInQueue(tt, 1, 1, 0);
  }

  @Test
  // test to make sure all tasks with localizing jobs are added to another queue
  public void testLocalizingJobActions() throws InterruptedException,
      IOException {

    // job and attempt ids
    JobID jobId1 = new JobID(jtIdentifier, 1);
    JobID jobId2 = new JobID(jtIdentifier, 2);

    TaskAttemptID taskAttemptId1 = new TaskAttemptID(jtIdentifier, 3, true, 1,
        1);
    TaskAttemptID taskAttemptId2 = new TaskAttemptID(jtIdentifier, 4, true, 1,
        1);

    // job actions which is localizing
    KillJobAction jAction1 = new KillJobAction(jobId1);
    RunningJob rjob1 = new RunningJob(jAction1.getJobID());
    rjob1.localizing = true;
    // add the job to the runningJobs tree
    tt.runningJobs.put(jobId1, rjob1);
    tt.addActionToCleanup(jAction1);

    KillJobAction jAction2 = new KillJobAction(jobId2);
    RunningJob rjob2 = new RunningJob(jAction2.getJobID());
    rjob2.localizing = true;
    // add the job to the runningJobs tree
    tt.runningJobs.put(jobId2, rjob2);
    tt.addActionToCleanup(jAction2);

    // task action which is localizing
    KillTaskAction tAction1 = new KillTaskAction(taskAttemptId1);
    RunningJob rjob3 = new RunningJob(tAction1.getTaskID().getJobID());
    // add the job to the runningJobs tree
    tt.runningJobs.put(rjob3.getJobID(), rjob3);
    rjob3.localizing = true;
    tt.addActionToCleanup(tAction1);

    KillTaskAction tAction2 = new KillTaskAction(taskAttemptId2);
    RunningJob rjob4 = new RunningJob(tAction2.getTaskID().getJobID());
    rjob4.localizing = true;
    // add the job to the runningJobs tree
    tt.runningJobs.put(rjob4.getJobID(), rjob4);
    tt.addActionToCleanup(tAction2);

    // before the task clean up test the queue
    checkItemCountInQueue(tt, 4, 4, 0);

    // now run the task clean up
    tt.taskCleanUp();

    // after the one round the first task gets moved to localizing list
    checkItemCountInQueue(tt, 4, 3, 1);
    // now run the task clean up
    tt.taskCleanUp();

    // after the another round the 2nd task gets moved to localizing list
    checkItemCountInQueue(tt, 4, 2, 2);

    // now run the task clean up
    tt.taskCleanUp();

    // after the another round the 3rd task gets moved to localizing list
    checkItemCountInQueue(tt, 4, 1, 3);

    // now run the task clean up
    tt.taskCleanUp();

    // after the another round the 4th task gets moved to localizing list
    checkItemCountInQueue(tt, 4, 0, 4);

    // now run the task clean up
    tt.taskCleanUp();

    // this is a no-op there is a sleep here but cant really test it
    checkItemCountInQueue(tt, 4, 0, 4);
  }

  @Test
  // test when all active items in the queue
  public void testAllActiveJobActions() throws InterruptedException,
      IOException {
    // job and attempt ids
    JobID jobId1 = new JobID(jtIdentifier, 1);
    JobID jobId2 = new JobID(jtIdentifier, 2);

    TaskAttemptID taskAttemptId1 = new TaskAttemptID(jtIdentifier, 3, true, 1,
        1);
    TaskAttemptID taskAttemptId2 = new TaskAttemptID(jtIdentifier, 4, true, 1,
        1);

    // job actions which is localizing
    KillJobAction jAction1 = new KillJobAction(jobId1);
    RunningJob rjob1 = new RunningJob(jAction1.getJobID());
    rjob1.localizing = false;
    // add the job to the runningJobs tree
    tt.runningJobs.put(jobId1, rjob1);
    tt.addActionToCleanup(jAction1);

    KillJobAction jAction2 = new KillJobAction(jobId2);
    RunningJob rjob2 = new RunningJob(jAction2.getJobID());
    rjob2.localizing = false;
    // add the job to the runningJobs tree
    tt.runningJobs.put(jobId2, rjob2);
    tt.addActionToCleanup(jAction2);

    // task action which is localizing
    KillTaskAction tAction1 = new KillTaskAction(taskAttemptId1);
    RunningJob rjob3 = new RunningJob(tAction1.getTaskID().getJobID());
    // add the job to the runningJobs tree
    tt.runningJobs.put(rjob3.getJobID(), rjob3);
    rjob3.localizing = false;
    tt.addActionToCleanup(tAction1);

    KillTaskAction tAction2 = new KillTaskAction(taskAttemptId2);
    RunningJob rjob4 = new RunningJob(tAction2.getTaskID().getJobID());
    rjob4.localizing = false;
    // add the job to the runningJobs tree
    tt.runningJobs.put(rjob4.getJobID(), rjob4);
    tt.addActionToCleanup(tAction2);

    // before the task clean up test the queue
    checkItemCountInQueue(tt, 4, 4, 0);

    // now run the task clean up
    tt.taskCleanUp();

    // after the one round the first task gets killed
    checkItemCountInQueue(tt, 3, 3, 0);
    // now run the task clean up
    tt.taskCleanUp();

    // after the another round the 2nd task gets killed
    checkItemCountInQueue(tt, 2, 2, 0);

    // now run the task clean up
    tt.taskCleanUp();

    // after the another round the 3rd task gets killed
    checkItemCountInQueue(tt, 1, 1, 0);

    // now run the task clean up
    tt.taskCleanUp();

    // after the another round the 4th task gets killed
    checkItemCountInQueue(tt, 0, 0, 0);

    // now run the task clean up
    tt.taskCleanUp();

    // this is a no-op there is a sleep here but cant really test it
    checkItemCountInQueue(tt, 0, 0, 0);
  }

  @Test
  // test when some items are localizing and some are not in the queue
  public void testMixedJobActions() throws InterruptedException, IOException {
    // job and attempt ids
    JobID jobId1 = new JobID(jtIdentifier, 1);
    JobID jobId2 = new JobID(jtIdentifier, 2);

    TaskAttemptID taskAttemptId1 = new TaskAttemptID(jtIdentifier, 3, true, 1,
        1);
    TaskAttemptID taskAttemptId2 = new TaskAttemptID(jtIdentifier, 4, true, 1,
        1);

    // job actions which is localizing
    KillJobAction jAction1 = new KillJobAction(jobId1);
    RunningJob rjob1 = new RunningJob(jAction1.getJobID());
    rjob1.localizing = true;
    // add the job to the runningJobs tree
    tt.runningJobs.put(jobId1, rjob1);
    tt.addActionToCleanup(jAction1);

    KillJobAction jAction2 = new KillJobAction(jobId2);
    RunningJob rjob2 = new RunningJob(jAction2.getJobID());
    rjob2.localizing = false;
    // add the job to the runningJobs tree
    tt.runningJobs.put(jobId2, rjob2);
    tt.addActionToCleanup(jAction2);

    // task action which is localizing
    KillTaskAction tAction1 = new KillTaskAction(taskAttemptId1);
    RunningJob rjob3 = new RunningJob(tAction1.getTaskID().getJobID());
    // add the job to the runningJobs tree
    tt.runningJobs.put(rjob3.getJobID(), rjob3);
    rjob3.localizing = true;
    tt.addActionToCleanup(tAction1);

    KillTaskAction tAction2 = new KillTaskAction(taskAttemptId2);
    RunningJob rjob4 = new RunningJob(tAction2.getTaskID().getJobID());
    rjob4.localizing = false;
    // add the job to the runningJobs tree
    tt.runningJobs.put(rjob4.getJobID(), rjob4);
    tt.addActionToCleanup(tAction2);

    // before the task clean up test the queue
    checkItemCountInQueue(tt, 4, 4, 0);

    // now run the task clean up
    tt.taskCleanUp();

    // since the first attempt is localizing it will move to the
    // localizing queue
    checkItemCountInQueue(tt, 4, 3, 1);

    // now run the task clean up
    tt.taskCleanUp();

    // after the 2nd round the tip is a job that can be cleaned up
    checkItemCountInQueue(tt, 3, 2, 1);

    // now run the task clean up
    tt.taskCleanUp();

    // after the 3rd round the 3rd task is getting localized
    checkItemCountInQueue(tt, 3, 1, 2);

    // now run the task clean up
    tt.taskCleanUp();

    // after the another round the 4th task gets killed
    checkItemCountInQueue(tt, 2, 0, 2);

    // now run the task clean up
    tt.taskCleanUp();

    // nothing should change as the tasks are being localized
    checkItemCountInQueue(tt, 2, 0, 2);

  }

  // test when some items are localizing and some are not in the queue
  // and we update the status of the actions between clean up jobs
  @Test
  public void testMixedJobActionsAndUpdateActions()
      throws InterruptedException, IOException {
    // job and attempt ids
    JobID jobId1 = new JobID(jtIdentifier, 1);
    JobID jobId2 = new JobID(jtIdentifier, 2);

    TaskAttemptID taskAttemptId1 = new TaskAttemptID(jtIdentifier, 3, true, 1,
        1);
    TaskAttemptID taskAttemptId2 = new TaskAttemptID(jtIdentifier, 4, true, 1,
        1);

    // job actions which is localizing
    KillJobAction jAction1 = new KillJobAction(jobId1);
    RunningJob rjob1 = new RunningJob(jAction1.getJobID());
    rjob1.localizing = true;
    // add the job to the runningJobs tree
    tt.runningJobs.put(jobId1, rjob1);
    tt.addActionToCleanup(jAction1);

    KillJobAction jAction2 = new KillJobAction(jobId2);
    RunningJob rjob2 = new RunningJob(jAction2.getJobID());
    rjob2.localizing = false;
    // add the job to the runningJobs tree
    tt.runningJobs.put(jobId2, rjob2);
    tt.addActionToCleanup(jAction2);

    // task action which is localizing
    KillTaskAction tAction1 = new KillTaskAction(taskAttemptId1);
    RunningJob rjob3 = new RunningJob(tAction1.getTaskID().getJobID());
    // add the job to the runningJobs tree
    tt.runningJobs.put(rjob3.getJobID(), rjob3);
    rjob3.localizing = true;
    tt.addActionToCleanup(tAction1);

    KillTaskAction tAction2 = new KillTaskAction(taskAttemptId2);
    RunningJob rjob4 = new RunningJob(tAction2.getTaskID().getJobID());
    rjob4.localizing = false;
    // add the job to the runningJobs tree
    tt.runningJobs.put(rjob4.getJobID(), rjob4);
    tt.addActionToCleanup(tAction2);

    // before the task clean up test the queue
    checkItemCountInQueue(tt, 4, 4, 0);

    // now run the task clean up
    tt.taskCleanUp();

    // since the first attempt is localizing it will move to the
    // localizing queue
    checkItemCountInQueue(tt, 4, 3, 1);

    // before running clean up again change the status of the first attempt to
    // not be localizing.
    rjob1.localizing = false;

    // now run the task clean up
    tt.taskCleanUp();

    // after the 2nd round the tip is a job that can be cleaned up so it is
    // cleaned and the localizing job is no longer being localized so it moves
    // to the active queue
    checkItemCountInQueue(tt, 3, 3, 0);

    // now run the task clean up
    tt.taskCleanUp();

    // after the 3rd round the 3rd task is getting localized
    // so gets moved to the localized queue.
    checkItemCountInQueue(tt, 3, 2, 1);

    // now run the task clean up
    tt.taskCleanUp();

    // after the another round the 4th task gets killed
    // and leaves 2 tasks one which switched from localizing to clean
    // and one that is being localized.
    checkItemCountInQueue(tt, 2, 1, 1);
  }

  /*
   * Method to test the number of items in the various datastructures holding
   * clean up actions
   */
  private void checkItemCountInQueue(TaskTracker tt, int allSize,
      int activeSize, int inactiveSize) {
    // check the size and the content of the hashset and queue
    assertEquals("Size of allCleanUpActions is not " + allSize, allSize,
        tt.allCleanupActions.size());
    assertEquals("Size of activeCleanUpActions is not " + activeSize,
        activeSize, tt.activeCleanupActions.size());
    assertEquals("Size of inactiveCleanUpActions is not " + inactiveSize,
        inactiveSize, tt.inactiveCleanupActions.size());
  }

  class TestTaskTracker extends TaskTracker {
    // override the method so its a no-op
    synchronized void purgeJob(KillJobAction action) throws IOException {
      LOG.info("Received 'KillJobAction' for job: " + action.getJobID());
    }

    // override the method so its a no-op
    void processKillTaskAction(KillTaskAction killAction) throws IOException {
      LOG.info("Received KillTaskAction for task: " + killAction.getTaskID());
    }
  }
}