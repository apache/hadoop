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

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import static org.apache.hadoop.mapred.CapacityTestUtils.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TestCapacityScheduler extends TestCase {

  static final Log LOG =
    LogFactory.getLog(org.apache.hadoop.mapred.TestCapacityScheduler.class);

  String queueConfigPath =
    System.getProperty("test.build.extraconf", "build/test/extraconf");
  File queueConfigFile =
    new File(queueConfigPath, QueueManager.QUEUE_CONF_FILE_NAME);

  private static int jobCounter;

  private ControlledInitializationPoller controlledInitializationPoller;


  protected JobConf conf;
  protected CapacityTaskScheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;
  private FakeClock clock;

  @Override
  protected void setUp() {
    setUp(2, 2, 1);
  }

  private void setUp(
    int numTaskTrackers, int numMapTasksPerTracker,
    int numReduceTasksPerTracker) {
    jobCounter = 0;
    taskTrackerManager =
      new FakeTaskTrackerManager(
        numTaskTrackers, numMapTasksPerTracker,
        numReduceTasksPerTracker);
    clock = new FakeClock();
    scheduler = new CapacityTaskScheduler(clock);
    scheduler.setTaskTrackerManager(taskTrackerManager);

    conf = new JobConf();
    // Don't let the JobInitializationPoller come in our way.
    conf.set("mapred.queue.names","default");
    controlledInitializationPoller =
        new ControlledInitializationPoller(scheduler.jobQueuesManager,
            taskTrackerManager);
    scheduler.setInitializationPoller(controlledInitializationPoller);
    scheduler.setConf(conf);
    //by default disable speculative execution.
    conf.setMapSpeculativeExecution(false);
    conf.setReduceSpeculativeExecution(false);
  }

  @Override
  protected void tearDown() throws Exception {
    if (scheduler != null) {
      scheduler.terminate();
    }
  }

  /**
   * Test max capacity
   * @throws IOException
   */
  public void testMaxCapacity() throws IOException {
    this.setUp(4, 1, 1);
    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 25.0f, false, 1));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();
    scheduler.getRoot().getChildren().get(0).getQueueSchedulingContext()
      .setMaxCapacityPercent(50.0f);

    //submit the Job
    FakeJobInProgress fjob1 = taskTrackerManager.submitJob(
      JobStatus.PREP, 4, 4, "default", "user");

    taskTrackerManager.initJob(fjob1);
    HashMap<String, String> expectedStrings = new HashMap<String, String>();

    expectedStrings.put(MAP, "attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000001_0 on tt1");
    List<Task> task1 = checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1", expectedStrings);


    expectedStrings.put(MAP, "attempt_test_0001_m_000002_0 on tt2");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000002_0 on tt2");
    List<Task> task2 = checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2", expectedStrings);

    //we have already reached the limit
    //this call would return null
    List<Task> task3 = scheduler.assignTasks(tracker("tt3"));
    assertNull(task3);

    //Now complete the task 1 i.e map task.
    for (Task task : task1) {
        taskTrackerManager.finishTask(
          task.getTaskID().toString(), fjob1);
    }
    
    expectedStrings.put(MAP, "attempt_test_0001_m_000003_0 on tt1");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000003_0 on tt1");
    task2 = checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1", expectedStrings);
  }

  // test job run-state change
  public void testJobRunStateChange() throws IOException {
    // start the scheduler
    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 1));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    // submit the job
    FakeJobInProgress fjob1 =
      taskTrackerManager.submitJob(JobStatus.PREP, 1, 0, "default", "user");

    FakeJobInProgress fjob2 =
      taskTrackerManager.submitJob(JobStatus.PREP, 1, 0, "default", "user");

    // test if changing the job priority/start-time works as expected in the 
    // waiting queue
    testJobOrderChange(fjob1, fjob2, true);

    // Init the jobs
    // simulate the case where the job with a lower priority becomes running 
    // first (may be because of the setup tasks).

    // init the lower ranked job first
    taskTrackerManager.initJob(fjob2);

    // init the higher ordered job later
    taskTrackerManager.initJob(fjob1);

    // check if the jobs are missing from the waiting queue
    // The jobs are not removed from waiting queue until they are scheduled 
    assertEquals(
      "Waiting queue is garbled on job init", 2,
      scheduler.jobQueuesManager.getJobQueue("default").getWaitingJobs()
        .size());

    // test if changing the job priority/start-time works as expected in the 
    // running queue
    testJobOrderChange(fjob1, fjob2, false);

    // schedule a task
    List<Task> tasks = scheduler.assignTasks(tracker("tt1"));

    // complete the job
    taskTrackerManager.finishTask(
      tasks.get(0).getTaskID().toString(),
      fjob1);

    // mark the job as complete
    taskTrackerManager.finalizeJob(fjob1);

    Collection<JobInProgress> rqueue =
      scheduler.jobQueuesManager.getJobQueue("default").getRunningJobs();

    // check if the job is removed from the scheduler
    assertFalse(
      "Scheduler contains completed job",
      rqueue.contains(fjob1));

    // check if the running queue size is correct
    assertEquals(
      "Job finish garbles the queue",
      1, rqueue.size());

  }

  // test if the queue reflects the changes
  private void testJobOrderChange(
    FakeJobInProgress fjob1,
    FakeJobInProgress fjob2,
    boolean waiting) {
    String queueName = waiting ? "waiting" : "running";

    // check if the jobs in the queue are the right order
    JobInProgress[] jobs = getJobsInQueue(waiting);
    assertTrue(
      queueName + " queue doesnt contain job #1 in right order",
      jobs[0].getJobID().equals(fjob1.getJobID()));
    assertTrue(
      queueName + " queue doesnt contain job #2 in right order",
      jobs[1].getJobID().equals(fjob2.getJobID()));

    // I. Check the start-time change
    // Change job2 start-time and check if job2 bumps up in the queue 
    taskTrackerManager.setStartTime(fjob2, fjob1.startTime - 1);

    jobs = getJobsInQueue(waiting);
    assertTrue(
      "Start time change didnt not work as expected for job #2 in "
        + queueName + " queue",
      jobs[0].getJobID().equals(fjob2.getJobID()));
    assertTrue(
      "Start time change didnt not work as expected for job #1 in"
        + queueName + " queue",
      jobs[1].getJobID().equals(fjob1.getJobID()));

    // check if the queue is fine
    assertEquals(
      "Start-time change garbled the " + queueName + " queue",
      2, jobs.length);

    // II. Change job priority change
    // Bump up job1's priority and make sure job1 bumps up in the queue
    taskTrackerManager.setPriority(fjob1, JobPriority.HIGH);

    // Check if the priority changes are reflected
    jobs = getJobsInQueue(waiting);
    assertTrue(
      "Priority change didnt not work as expected for job #1 in "
        + queueName + " queue",
      jobs[0].getJobID().equals(fjob1.getJobID()));
    assertTrue(
      "Priority change didnt not work as expected for job #2 in "
        + queueName + " queue",
      jobs[1].getJobID().equals(fjob2.getJobID()));

    // check if the queue is fine
    assertEquals(
      "Priority change has garbled the " + queueName + " queue",
      2, jobs.length);

    // reset the queue state back to normal
    taskTrackerManager.setStartTime(fjob1, fjob2.startTime - 1);
    taskTrackerManager.setPriority(fjob1, JobPriority.NORMAL);
  }

  private JobInProgress[] getJobsInQueue(boolean waiting) {
    Collection<JobInProgress> queue =
      waiting
        ? scheduler.jobQueuesManager.getJobQueue("default").getWaitingJobs()
        : scheduler.jobQueuesManager.getJobQueue("default").getRunningJobs();
    return queue.toArray(new JobInProgress[0]);
  }

  // tests if tasks can be assinged when there are multiple jobs from a same
  // user
  public void testJobFinished() throws Exception {
    taskTrackerManager.addQueues(new String[]{"default"});

    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    // submit 2 jobs
    FakeJobInProgress j1 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, 3, 0, "default", "u1");
    FakeJobInProgress j2 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, 3, 0, "default", "u1");

    // I. Check multiple assignments with running tasks within job
    // ask for a task from first job
    Task t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    //  ask for another task from the first job
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000002_0 on tt1");

    // complete tasks
    taskTrackerManager.finishTask("attempt_test_0001_m_000001_0", j1);
    taskTrackerManager.finishTask("attempt_test_0001_m_000002_0", j1);

    // II. Check multiple assignments with running tasks across jobs
    // ask for a task from first job
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000003_0 on tt1");

    //  ask for a task from the second job
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000001_0 on tt1");

    // complete tasks
    taskTrackerManager.finishTask("attempt_test_0002_m_000001_0", j2);
    taskTrackerManager.finishTask("attempt_test_0001_m_000003_0", j1);

    // III. Check multiple assignments with completed tasks across jobs
    // ask for a task from the second job
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000002_0 on tt1");

    // complete task
    taskTrackerManager.finishTask("attempt_test_0002_m_000002_0", j2);

    // IV. Check assignment with completed job
    // finish first job
    scheduler.jobQueuesManager.getJobQueue(j1).jobCompleted(j1);

    // ask for another task from the second job
    // if tasks can be assigned then the structures are properly updated 
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000003_0 on tt1");

    // complete task
    taskTrackerManager.finishTask("attempt_test_0002_m_000003_0", j2);
  }

  /**
   * tests the submission of jobs to container and job queues
   * @throws Exception
   */
  public void testJobSubmission() throws Exception {
    JobQueueInfo[] queues = TestQueueManagerRefresh.getSimpleQueueHierarchy();

    queues[0].getProperties().setProperty(
        CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(100));
    queues[1].getProperties().setProperty(
        CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(50));
    queues[2].getProperties().setProperty(
        CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(50));

    // write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });
    setUp(1, 4, 4);
    // use the queues from the config file.
    taskTrackerManager.setQueueManager(new QueueManager());
    scheduler.start();

    // submit a job to the container queue
    try {
      taskTrackerManager.submitJobAndInit(JobStatus.PREP, 20, 0,
          queues[0].getQueueName(), "user");
      fail("Jobs are being able to be submitted to the container queue");
    } catch (Exception e) {
      assertTrue(scheduler.getJobs(queues[0].getQueueName()).isEmpty());
    }

    FakeJobInProgress job = taskTrackerManager.submitJobAndInit(JobStatus.PREP,
        1, 0, queues[1].getQueueName(), "user");
    assertEquals(1, scheduler.getJobs(queues[1].getQueueName()).size());
    assertTrue(scheduler.getJobs(queues[1].getQueueName()).contains(job));

    // check if the job is submitted
    checkAssignment(taskTrackerManager, scheduler, "tt1", 
    "attempt_test_0002_m_000001_0 on tt1");

    // test for getJobs
    HashMap<String, ArrayList<FakeJobInProgress>> subJobsList =
      taskTrackerManager.submitJobs(1, 4, queues[2].getQueueName());

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    //Raise status change events for jobs submitted.
    raiseStatusChangeEvents(mgr, queues[2].getQueueName());
    Collection<JobInProgress> jobs =
      scheduler.getJobs(queues[2].getQueueName());

    assertTrue(
      "Number of jobs returned by scheduler is wrong"
      , jobs.size() == 4);

    assertTrue(
      "Submitted jobs and Returned jobs are not same",
      subJobsList.get("u1").containsAll(jobs));
  }

  //Basic test to test capacity allocation across the queues which have no
  //capacity configured.

  public void testCapacityAllocationToQueues() throws Exception {
    String[] qs = {"default", "qAZ1", "qAZ2", "qAZ3", "qAZ4"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 25.0f, true, 25));
    queues.add(new FakeQueueInfo("qAZ1", -1.0f, true, 25));
    queues.add(new FakeQueueInfo("qAZ2", -1.0f, true, 25));
    queues.add(new FakeQueueInfo("qAZ3", -1.0f, true, 25));
    queues.add(new FakeQueueInfo("qAZ4", -1.0f, true, 25));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();
    JobQueuesManager jqm = scheduler.jobQueuesManager;
    assertEquals(18.75f, jqm.getJobQueue("qAZ1").qsc.getCapacityPercent());
    assertEquals(18.75f, jqm.getJobQueue("qAZ2").qsc.getCapacityPercent());
    assertEquals(18.75f, jqm.getJobQueue("qAZ3").qsc.getCapacityPercent());
    assertEquals(18.75f, jqm.getJobQueue("qAZ4").qsc.getCapacityPercent());
  }

  public void testCapacityAllocFailureWithLowerMaxCapacity() throws Exception {
    String[] qs = {"default", "qAZ1"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 25.0f, true, 25));
    FakeQueueInfo qi = new FakeQueueInfo("qAZ1", -1.0f, true, 25);
    qi.maxCapacity = 40.0f;
    queues.add(qi);
    taskTrackerManager.setFakeQueues(queues);
    try {
      scheduler.start();
      fail("scheduler start should fail ");
    }catch(IOException ise) {
      Throwable e = ise.getCause();
      assertTrue(e instanceof IllegalStateException);
      assertEquals(
        e.getMessage(),
        " Capacity share (" + 75.0f + ")for unconfigured queue " + "qAZ1" +
          " is greater than its maximum-capacity percentage " + 40.0f);
    }
  }

  // Tests how capacity is computed and assignment of tasks done
  // on the basis of the capacity.
  public void testCapacityBasedAllocation() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    // set the capacity % as 10%, so that capacity will be zero initially as 
    // the cluster capacity increase slowly.
    queues.add(new FakeQueueInfo("default", 10.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 90.0f, true, 25));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    // submit a job to the default queue
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 0, "default", "u1");

    // submit a job to the second queue
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 0, "q2", "u1");

    // job from q2 runs first because it has some non-zero capacity.
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000001_0 on tt1");
    verifyCapacity(taskTrackerManager, "0", "default");
    verifyCapacity(taskTrackerManager, "3", "q2");

    // add another tt to increase tt slots
    taskTrackerManager.addTaskTracker("tt3");
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_m_000002_0 on tt2");
    verifyCapacity(taskTrackerManager, "0", "default");
    verifyCapacity(taskTrackerManager, "5", "q2");

    // add another tt to increase tt slots
    taskTrackerManager.addTaskTracker("tt4");
    checkAssignment(
      taskTrackerManager, scheduler, "tt3",
      "attempt_test_0002_m_000003_0 on tt3");
    verifyCapacity(taskTrackerManager, "0", "default");
    verifyCapacity(taskTrackerManager, "7", "q2");

    // add another tt to increase tt slots
    taskTrackerManager.addTaskTracker("tt5");
    // now job from default should run, as it is furthest away
    // in terms of runningMaps / capacity.
    checkAssignment(
      taskTrackerManager, scheduler, "tt4",
      "attempt_test_0001_m_000001_0 on tt4");
    verifyCapacity(taskTrackerManager, "1", "default");
    verifyCapacity(taskTrackerManager, "9", "q2");
  }

  // test capacity transfer
  public void testCapacityTransfer() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    // submit a job  
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
    // we should get a task
    Map<String,String> expectedStrings = new HashMap<String,String>();
    expectedStrings.put(MAP,"attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(REDUCE,"attempt_test_0001_r_000001_0 on tt1");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);

    // I should get another map task.
    //No redduces as there is 1 slot only for reduce on TT
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000002_0 on tt1");

    // Now we're at full capacity for maps. If I ask for another map task,
    // I should get a map task from the default queue's capacity.
    //same with reduce
    expectedStrings.put(MAP,"attempt_test_0001_m_000003_0 on tt2");
    expectedStrings.put(REDUCE,"attempt_test_0001_r_000002_0 on tt2");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      expectedStrings);
    
    // and another
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000004_0 on tt2");
  }

  /**
   * test the high memory blocking with max capacity.
   * @throws IOException
   */
  public void testHighMemoryBlockingWithMaxCapacity()
    throws IOException {
    taskTrackerManager = new FakeTaskTrackerManager(2, 2, 2);

    taskTrackerManager.addQueues(new String[]{"defaultXYZM"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("defaultXYZM", 25.0f, true, 50));


    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(JTConfig.JT_MAX_MAPMEMORY_MB, 2 * 1024);
    scheduler.getConf().setLong(MRConfig.MAPMEMORY_MB, 1 * 1024);
    scheduler.getConf().setLong(JTConfig.JT_MAX_REDUCEMEMORY_MB, 2 * 1024);
    scheduler.getConf().setLong(MRConfig.REDUCEMEMORY_MB, 1 * 1024);
    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();
    scheduler.getRoot().getChildren().get(0).getQueueSchedulingContext()
      .setMaxCapacityPercent(50);

    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("defaultXYZM");
    jConf.setUser("u1");
    FakeJobInProgress job1 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, jConf);

    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("defaultXYZM");
    jConf.setUser("u1");
    FakeJobInProgress job2 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, jConf);

  //high ram map from job 1 and normal reduce task from job 1
    HashMap<String,String> expectedStrings = new HashMap<String,String>();
    expectedStrings.put(MAP,"attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(REDUCE,"attempt_test_0001_r_000001_0 on tt1");

    List<Task> tasks = checkMultipleTaskAssignment(taskTrackerManager,scheduler,
      "tt1", expectedStrings);

    checkOccupiedSlots("defaultXYZM", TaskType.MAP, 1, 2, 200.0f,1,0);
    checkOccupiedSlots("defaultXYZM", TaskType.REDUCE, 1, 1, 100.0f,0,2);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 1 * 1024L);

    //we have reached the maximum limit for map, so no more map tasks.
    //we have used 1 reduce already and 1 more reduce slot is left for the
    //before we reach maxcapacity for reduces.
    // But current 1 slot + 2 slots for high ram reduce would
    //mean we are crossing the maxium capacity.hence nothing would be assigned
    //in this call
    assertNull(scheduler.assignTasks(tracker("tt2")));

    //complete the high ram job on tt1.
    for (Task task : tasks) {
      taskTrackerManager.finishTask(
        task.getTaskID().toString(),
        job1);
    }

    expectedStrings.put(MAP,"attempt_test_0001_m_000002_0 on tt2");
    expectedStrings.put(REDUCE,"attempt_test_0002_r_000001_0 on tt2");

    tasks = checkMultipleTaskAssignment(taskTrackerManager,scheduler,
      "tt2", expectedStrings);

    checkOccupiedSlots("defaultXYZM", TaskType.MAP, 1, 2, 200.0f,1,0);
    checkOccupiedSlots("defaultXYZM", TaskType.REDUCE, 1, 2, 200.0f,0,2);
    checkMemReservedForTasksOnTT("tt2", 2 * 1024L, 2 * 1024L);

    //complete the high ram job on tt1.
    for (Task task : tasks) {
      taskTrackerManager.finishTask(
        task.getTaskID().toString(),
        job2);
    }


    expectedStrings.put(MAP,"attempt_test_0002_m_000001_0 on tt2");
    expectedStrings.put(REDUCE,"attempt_test_0002_r_000002_0 on tt2");

    tasks = checkMultipleTaskAssignment(taskTrackerManager,scheduler,
      "tt2", expectedStrings);
  }

  /**
   * test if user limits automatically adjust to max map or reduce limit
   */
  public void testUserLimitsWithMaxCapacity() throws Exception {
    setUp(2, 2, 2);
    // set up some queues
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 50));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();
    scheduler.getRoot().getChildren().get(0).getQueueSchedulingContext()
      .setMaxCapacityPercent(75);

    // submit a job
    FakeJobInProgress fjob1 =
      taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, "default", "u1");
    FakeJobInProgress fjob2 =
      taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, "default", "u2");

    // for queue 'default', maxCapacity for map and reduce is 3.
    // initial user limit for 50% assuming there are 2 users/queue is.
    //  1 map and 1 reduce.
    // after max capacity it is 1.5 each.

    //first job would be given 1 job each.
    HashMap<String,String> expectedStrings = new HashMap<String,String>();
    expectedStrings.put(MAP,"attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(REDUCE,"attempt_test_0001_r_000001_0 on tt1");

    List<Task> tasks = checkMultipleTaskAssignment(taskTrackerManager,scheduler,
      "tt1", expectedStrings);


    //for user u1 we have reached the limit. that is 1 job.
    //1 more map and reduce tasks.
    expectedStrings.put(MAP,"attempt_test_0002_m_000001_0 on tt1");
    expectedStrings.put(REDUCE,"attempt_test_0002_r_000001_0 on tt1");

    tasks = checkMultipleTaskAssignment(taskTrackerManager,scheduler,
      "tt1", expectedStrings);

    expectedStrings.put(MAP,"attempt_test_0001_m_000002_0 on tt2");
    expectedStrings.put(REDUCE,"attempt_test_0001_r_000002_0 on tt2");

    tasks = checkMultipleTaskAssignment(taskTrackerManager,scheduler,
      "tt2", expectedStrings);

    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  // Utility method to construct a map of expected strings
  // with exactly one map task and one reduce task.
  private void populateExpectedStrings(Map<String, String> expectedTaskStrings,
                        String mapTask, String reduceTask) {
    expectedTaskStrings.clear();
    expectedTaskStrings.put(CapacityTestUtils.MAP, mapTask);
    expectedTaskStrings.put(CapacityTestUtils.REDUCE, reduceTask);
  }


  // test user limits
  public void testUserLimits() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    // submit a job  
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity is 2 for maps and 1 for reduce. 
    // Since we're the only user, we should get tasks
    Map<String, String> expectedTaskStrings = new HashMap<String, String>();
    populateExpectedStrings(expectedTaskStrings, 
              "attempt_test_0001_m_000001_0 on tt1", 
              "attempt_test_0001_r_000001_0 on tt1");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler, 
                                  "tt1", expectedTaskStrings);

    // Submit another job, from a different user
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u2");
    // Now if I ask for a task, it should come from the second job
    checkAssignment(taskTrackerManager, scheduler, 
        "tt1", "attempt_test_0002_m_000001_0 on tt1");

    // Now we're at full capacity. If I ask for another task,
    // I should get tasks from the default queue's capacity.
    populateExpectedStrings(expectedTaskStrings, 
        "attempt_test_0001_m_000002_0 on tt2", 
        "attempt_test_0002_r_000001_0 on tt2");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler, 
          "tt2", expectedTaskStrings);
    // and another
    checkAssignment(taskTrackerManager, scheduler, 
            "tt2", "attempt_test_0002_m_000002_0 on tt2");
  }

  // test user limits when a 2nd job is submitted much after first job 
  public void testUserLimits2() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    // submit a job  
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2 and reduce is 1. 
    // Since we're the only user, we should get tasks
    Map<String, String> expectedTaskStrings = new HashMap<String, String>();
    populateExpectedStrings(expectedTaskStrings, 
        "attempt_test_0001_m_000001_0 on tt1",
        "attempt_test_0001_r_000001_0 on tt1");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler, 
        "tt1", expectedTaskStrings);

    // since we're the only job, we get another map
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000002_0 on tt1");

    // Submit another job, from a different user
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u2");
    // Now if I ask for a task, it should come from the second job
    populateExpectedStrings(expectedTaskStrings, 
        "attempt_test_0002_m_000001_0 on tt2",
        "attempt_test_0002_r_000001_0 on tt2");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler, 
        "tt2", expectedTaskStrings);
    // and another
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_m_000002_0 on tt2");
  }

  // test user limits when a 2nd job is submitted much after first job 
  // and we need to wait for first job's task to complete
  public void testUserLimits3() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    // submit a job  
    FakeJobInProgress j1 = taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2 and reduces is 1. 
    // Since we're the only user, we should get a task
    Map<String, String> expectedTaskStrings = new HashMap<String, String>();
    populateExpectedStrings(expectedTaskStrings, 
        "attempt_test_0001_m_000001_0 on tt1", 
        "attempt_test_0001_r_000001_0 on tt1");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler, 
        "tt1", expectedTaskStrings);
    // since we're the only job, we get another map
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000002_0 on tt1");
    // we get more tasks from 'default queue'
    populateExpectedStrings(expectedTaskStrings, 
        "attempt_test_0001_m_000003_0 on tt2",
        "attempt_test_0001_r_000002_0 on tt2");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler,
        "tt2", expectedTaskStrings);
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000004_0 on tt2");

    // Submit another job, from a different user
    FakeJobInProgress j2 = taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u2");
    // one of the task finishes of each type
    taskTrackerManager.finishTask("attempt_test_0001_m_000001_0", j1);
    taskTrackerManager.finishTask("attempt_test_0001_r_000001_0", j1);
    
    // Now if I ask for a task, it should come from the second job
    populateExpectedStrings(expectedTaskStrings, 
        "attempt_test_0002_m_000001_0 on tt1",
        "attempt_test_0002_r_000001_0 on tt1");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler,
        "tt1", expectedTaskStrings);

    // another task from job1 finishes, another new task to job2
    taskTrackerManager.finishTask("attempt_test_0001_m_000002_0", j1);
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000002_0 on tt1");

    // now we have equal number of tasks from each job. Whichever job's
    // task finishes, that job gets a new task
    taskTrackerManager.finishTask("attempt_test_0001_m_000003_0", j1);
    taskTrackerManager.finishTask("attempt_test_0001_r_000002_0", j1);
    populateExpectedStrings(expectedTaskStrings,
        "attempt_test_0001_m_000005_0 on tt2",
        "attempt_test_0001_r_000003_0 on tt2");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler,
        "tt2", expectedTaskStrings);
    taskTrackerManager.finishTask("attempt_test_0002_m_000001_0", j2);
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000003_0 on tt1");
  }

  // test user limits with many users, more slots
  public void testUserLimits4() throws Exception {
    // set up one queue, with 10 map slots and 5 reduce slots
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();
    // add some more TTs 
    taskTrackerManager.addTaskTracker("tt3");
    taskTrackerManager.addTaskTracker("tt4");
    taskTrackerManager.addTaskTracker("tt5");

    // u1 submits job
    FakeJobInProgress j1 = taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    // it gets the first 5 slots
    Map<String, String> expectedTaskStrings = new HashMap<String, String>();
    for (int i=0; i<5; i++) {
      String ttName = "tt"+(i+1);
      populateExpectedStrings(expectedTaskStrings,
          "attempt_test_0001_m_00000"+(i+1)+"_0 on " + ttName, 
          "attempt_test_0001_r_00000"+(i+1)+"_0 on " + ttName);
      checkMultipleTaskAssignment(taskTrackerManager, scheduler, 
          ttName, expectedTaskStrings);
    }
      
    // u2 submits job with 4 slots
    FakeJobInProgress j2 = taskTrackerManager.submitJobAndInit(JobStatus.PREP, 4, 4, null, "u2");
    // u2 should get next 4 slots
    for (int i=0; i<4; i++) {
      String ttName = "tt"+(i+1);
      checkAssignment(taskTrackerManager, scheduler, ttName,
          "attempt_test_0002_m_00000"+(i+1)+"_0 on " + ttName);
    }
    // last slot should go to u1, since u2 has no more tasks
    checkAssignment(
      taskTrackerManager, scheduler, "tt5",
      "attempt_test_0001_m_000006_0 on tt5");
    // u1 finishes tasks
    taskTrackerManager.finishTask("attempt_test_0001_m_000006_0", j1);
    taskTrackerManager.finishTask("attempt_test_0001_r_000005_0", j1);
    // u1 submits a few more jobs 
    // All the jobs are inited when submitted
    // because of addition of Eager Job Initializer all jobs in this
    //case would e initialised.
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    // u2 also submits a job
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 10, 10, null, "u2");
    // now u3 submits a job
    taskTrackerManager.submitJobAndInit(JobStatus.PREP, 2, 2, null, "u3");
    // next map slot should go to u3, even though u2 has an earlier job, since
    // user limits have changed and u1/u2 are over limits
    // reduce slot will go to job 2, as it is still under limit.
    populateExpectedStrings(expectedTaskStrings,
        "attempt_test_0007_m_000001_0 on tt5",
        "attempt_test_0002_r_000001_0 on tt5");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler, 
        "tt5", expectedTaskStrings);
    // some other task finishes and u3 gets it
    taskTrackerManager.finishTask("attempt_test_0002_m_000004_0", j1);
    checkAssignment(
      taskTrackerManager, scheduler, "tt4",
      "attempt_test_0007_m_000002_0 on tt4");
    // now, u2 finishes a task
    taskTrackerManager.finishTask("attempt_test_0002_m_000002_0", j1);
    // next slot will go to u1, since u3 has nothing to run and u1's job is 
    // first in the queue
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000007_0 on tt2");
  }

  /**
   * Test to verify that high memory jobs hit user limits faster than any normal
   * job.
   *
   * @throws IOException
   */
  public void testUserLimitsForHighMemoryJobs()
    throws IOException {
    taskTrackerManager = new FakeTaskTrackerManager(1, 10, 10);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 50));


    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
      JTConfig.JT_MAX_MAPMEMORY_MB, 2 * 1024);
    scheduler.getConf().setLong(
      MRConfig.MAPMEMORY_MB, 1 * 1024);
    scheduler.getConf().setLong(
      JTConfig.JT_MAX_REDUCEMEMORY_MB, 2 * 1024);
    scheduler.getConf().setLong(
      MRConfig.REDUCEMEMORY_MB, 1 * 1024);
    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    // Submit one normal job to the other queue.
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setUser("u1");
    jConf.setQueueName("default");
    FakeJobInProgress job1 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, jConf);

    LOG.debug(
      "Submit one high memory(2GB maps, 2GB reduces) job of "
        + "6 map and 6 reduce tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setQueueName("default");
    jConf.setUser("u2");
    FakeJobInProgress job2 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, jConf);

    // Verify that normal job takes 5 task assignments to hit user limits
    Map<String, String> expectedStrings = new HashMap<String, String>();
    for (int i = 0; i < 5; i++) {
      expectedStrings.clear();
      expectedStrings.put(
        CapacityTestUtils.MAP,
        "attempt_test_0001_m_00000" + (i + 1) + "_0 on tt1");
      expectedStrings.put(
        CapacityTestUtils.REDUCE,
        "attempt_test_0001_r_00000" + (i + 1) + "_0 on tt1");
      checkMultipleTaskAssignment(
        taskTrackerManager, scheduler, "tt1",
        expectedStrings);
    }
    // u1 has 5 map slots and 5 reduce slots. u2 has none. So u1's user limits
    // are hit. So u2 should get slots

    for (int i = 0; i < 2; i++) {
      expectedStrings.clear();
      expectedStrings.put(
        CapacityTestUtils.MAP,
        "attempt_test_0002_m_00000" + (i + 1) + "_0 on tt1");
      expectedStrings.put(
        CapacityTestUtils.REDUCE,
        "attempt_test_0002_r_00000" + (i + 1) + "_0 on tt1");
      checkMultipleTaskAssignment(
        taskTrackerManager, scheduler, "tt1",
        expectedStrings);
    }  // u1 has 5 map slots and 5 reduce slots. u2 has 4 map slots and 4 reduce
    // slots. Because of high memory tasks, giving u2 another task would
    // overflow limits. So, no more tasks should be given to anyone.
    assertNull(scheduler.assignTasks(tracker("tt1")));
  }

  /*
   * Following is the testing strategy for testing scheduling information.
   * - start capacity scheduler with two queues.
   * - check the scheduling information with respect to the configuration
   * which was used to configure the queues.
   * - Submit 5 jobs to a queue.
   * - Check the waiting jobs count, it should be 5.
   * - Then run initializationPoller()
   * - Check once again the waiting queue, it should be 5 jobs again.
   * - Then raise status change events.
   * - Assign tasks to a task tracker.
   * - Check waiting job count, it should be 4 now and used map (%) = 100
   * and used reduce (%) = 100
   * - finish the job and then check the used percentage it should go
   * back to zero
   * - Then pick an initialized job but not scheduled job and fail it.
   * - Run the poller
   * - Check the waiting job count should now be 3.
   * - Now fail a job which has not been initialized at all.
   * - Run the poller, so that it can clean up the job queue.
   * - Check the count, the waiting job count should be 2.
   * - Now raise status change events to move the initialized jobs which
   * should be two in count to running queue.
   * - Then schedule a map and reduce of the job in running queue.
   * - Run the poller because the poller is responsible for waiting
   * jobs count. Check the count, it should be using 100% map, reduce and one
   * waiting job
   * - fail the running job.
   * - Check the count, it should be now one waiting job and zero running
   * tasks
   */

  public void testSchedulingInformation() throws Exception {
    String[] qs = {"default", "q2"};
    taskTrackerManager = new FakeTaskTrackerManager(2, 1, 1);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    scheduler.assignTasks(tracker("tt1")); // heartbeat
    scheduler.assignTasks(tracker("tt2")); // heartbeat
    int totalMaps = taskTrackerManager.getClusterStatus().getMaxMapTasks();
    int totalReduces =
      taskTrackerManager.getClusterStatus().getMaxReduceTasks();
    QueueManager queueManager = scheduler.taskTrackerManager.getQueueManager();
    String schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    String schedulingInfo2 =
      queueManager.getJobQueueInfo("q2").getSchedulingInfo();

    String[] infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 18);
    assertEquals(infoStrings[0], "Queue configuration");
    assertEquals(infoStrings[1], "Capacity Percentage: 50.0%");
    assertEquals(infoStrings[2], "User Limit: 25%");
    assertEquals(infoStrings[3], "Priority Supported: YES");
    assertEquals(infoStrings[4], "-------------");
    assertEquals(infoStrings[5], "Map tasks");
    assertEquals(
      infoStrings[6], "Capacity: " + totalMaps * 50 / 100
        + " slots");
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[9], "-------------");
    assertEquals(infoStrings[10], "Reduce tasks");
    assertEquals(
      infoStrings[11], "Capacity: " + totalReduces * 50 / 100
        + " slots");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[14], "-------------");
    assertEquals(infoStrings[15], "Job info");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 0");
    assertEquals(infoStrings[17], "Number of users who have submitted jobs: 0");

    assertEquals(schedulingInfo, schedulingInfo2);

    //Testing with actual job submission.
    ArrayList<FakeJobInProgress> userJobs =
      taskTrackerManager.submitJobs(1, 5, "default").get("u1");
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");

    //waiting job should be equal to number of jobs submitted.
    assertEquals(infoStrings.length, 18);
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 5");
    assertEquals(infoStrings[17], "Number of users who have submitted jobs: 1");

    //Initalize the jobs but don't raise events
    controlledInitializationPoller.selectJobsToInitialize();

    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 18);
    //should be previous value as nothing is scheduled because no events
    //has been raised after initialization.
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 5");

    //Raise status change event so that jobs can move to running queue.
    raiseStatusChangeEvents(scheduler.jobQueuesManager);
    raiseStatusChangeEvents(scheduler.jobQueuesManager, "q2");
    //assign one job
    Map<String, String> strs = new HashMap<String, String>();
    strs.put(CapacityTestUtils.MAP, "attempt_test_0001_m_000001_0 on tt1");
    strs.put(CapacityTestUtils.REDUCE, "attempt_test_0001_r_000001_0 on tt1");
    List<Task> t1 = checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      strs);
    //Initalize extra job.
    controlledInitializationPoller.selectJobsToInitialize();

    //Get scheduling information, now the number of waiting job should have
    //changed to 4 as one is scheduled and has become running.
    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");

    assertEquals(infoStrings.length, 22);
    assertEquals(infoStrings[7], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 1");
    assertEquals(infoStrings[9], "Active users:");
    assertEquals(infoStrings[10], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[14], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[15], "Running tasks: 1");
    assertEquals(infoStrings[20], "Number of Waiting Jobs: 4");

    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");

    assertEquals(infoStrings.length, 22);
    assertEquals(infoStrings[7], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 1");
    assertEquals(infoStrings[9], "Active users:");
    assertEquals(infoStrings[10], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[14], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[15], "Running tasks: 1");
    assertEquals(infoStrings[16], "Active users:");
    assertEquals(infoStrings[17], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[20], "Number of Waiting Jobs: 4");

    //Complete the job and check the running tasks count
    FakeJobInProgress u1j1 = userJobs.get(0);
    for (Task task : t1) {
      taskTrackerManager.finishTask(task.getTaskID().toString(), u1j1);
    }
    taskTrackerManager.finalizeJob(u1j1);

    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");

    assertEquals(infoStrings.length, 18);
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 4");

    //Fail a job which is initialized but not scheduled and check the count.
    FakeJobInProgress u1j2 = userJobs.get(1);
    assertTrue(
      "User1 job 2 not initalized ",
      u1j2.getStatus().getRunState() == JobStatus.RUNNING);
    taskTrackerManager.finalizeJob(u1j2, JobStatus.FAILED);
    //Run initializer to clean up failed jobs
    controlledInitializationPoller.selectJobsToInitialize();
    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 18);
    //should be previous value as nothing is scheduled because no events
    //has been raised after initialization.
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 3");

    //Fail a job which is not initialized but is in the waiting queue.
    FakeJobInProgress u1j5 = userJobs.get(4);
    assertFalse(
      "User1 job 5 initalized ",
      u1j5.getStatus().getRunState() == JobStatus.RUNNING);

    taskTrackerManager.finalizeJob(u1j5, JobStatus.FAILED);
    //run initializer to clean up failed job
    controlledInitializationPoller.selectJobsToInitialize();
    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 18);
    //should be previous value as nothing is scheduled because no events
    //has been raised after initialization.
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 2");

    //Raise status change events as none of the intialized jobs would be
    //in running queue as we just failed the second job which was initialized
    //and completed the first one.
    raiseStatusChangeEvents(scheduler.jobQueuesManager);
    raiseStatusChangeEvents(scheduler.jobQueuesManager, "q2");

    //Now schedule a map should be job3 of the user as job1 succeeded job2
    //failed and now job3 is running
    strs.clear();
    strs.put(CapacityTestUtils.MAP, "attempt_test_0003_m_000001_0 on tt1");
    strs.put(CapacityTestUtils.REDUCE, "attempt_test_0003_r_000001_0 on tt1");
    t1 = checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      strs);
    FakeJobInProgress u1j3 = userJobs.get(2);
    assertTrue(
      "User Job 3 not running ",
      u1j3.getStatus().getRunState() == JobStatus.RUNNING);

    //now the running count of map should be one and waiting jobs should be
    //one. run the poller as it is responsible for waiting count
    controlledInitializationPoller.selectJobsToInitialize();
    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 22);
    assertEquals(infoStrings[7], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 1");
    assertEquals(infoStrings[9], "Active users:");
    assertEquals(infoStrings[10], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[14], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[15], "Running tasks: 1");
    assertEquals(infoStrings[16], "Active users:");
    assertEquals(infoStrings[17], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[20], "Number of Waiting Jobs: 1");

    //Fail the executing job
    taskTrackerManager.finalizeJob(u1j3, JobStatus.FAILED);
    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    //Now running counts should become zero
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 18);
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 1");
  }

  /**
   * Test to verify that highMemoryJobs are scheduled like all other jobs when
   * memory-based scheduling is not enabled.
   *
   * @throws IOException
   */
  public void testDisabledMemoryBasedScheduling()
    throws IOException {

    LOG.debug("Starting the scheduler.");
    taskTrackerManager = new FakeTaskTrackerManager(1, 1, 1);

    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // memory-based scheduling disabled by default.
    scheduler.start();

    LOG.debug(
      "Submit one high memory job of 1 3GB map task "
        + "and 1 1GB reduce task.");
    JobConf jConf = new JobConf();
    jConf.setMemoryForMapTask(3 * 1024L); // 3GB
    jConf.setMemoryForReduceTask(1 * 1024L); // 1 GB
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    taskTrackerManager.submitJobAndInit(JobStatus.RUNNING, jConf);

    // assert that all tasks are launched even though they transgress the
    // scheduling limits.
    Map<String, String> expectedStrings = new HashMap<String, String>();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0001_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);
  }

  /**
   * Test reverting HADOOP-4979. If there is a high-mem job, we should now look
   * at reduce jobs (if map tasks are high-mem) or vice-versa.
   *
   * @throws IOException
   */
  public void testHighMemoryBlockingAcrossTaskTypes()
    throws IOException {

    // 2 map and 1 reduce slots
    taskTrackerManager = new FakeTaskTrackerManager(1, 2, 2);

    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));


    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(JTConfig.JT_MAX_MAPMEMORY_MB, 2 * 1024);
    scheduler.getConf().setLong(MRConfig.MAPMEMORY_MB, 1 * 1024);
    scheduler.getConf().setLong(JTConfig.JT_MAX_REDUCEMEMORY_MB, 1 * 1024);
    scheduler.getConf().setLong(MRConfig.REDUCEMEMORY_MB, 1 * 1024);
    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    // The situation : Two jobs in the queue. First job with only maps and no
    // reduces and is a high memory job. Second job is a normal job with both
    // maps and reduces.
    // First job cannot run for want of memory for maps. In this case, second
    // job's reduces should run.

    LOG.debug(
      "Submit one high memory(2GB maps, 0MB reduces) job of "
        + "2 map tasks");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(0);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, jConf);

    LOG.debug(
      "Submit another regular memory(1GB vmem maps/reduces) job of "
        + "2 map/red tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, jConf);

    // first, a map from j1 and a reduce from other job j2
    Map<String,String> strs = new HashMap<String,String>();
    strs.put(MAP,"attempt_test_0001_m_000001_0 on tt1");
    strs.put(REDUCE,"attempt_test_0002_r_000001_0 on tt1");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      strs);
    // Total 2 map slots should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 1, 2, 100.0f);
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 1, 50.0f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 1 * 1024L);

    //TT has 2 slots for reduces hence this call should get a reduce task
    //from other job
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_r_000002_0 on tt1");
    checkOccupiedSlots("default", TaskType.MAP, 1, 2, 100.0f);
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 2, 100.0f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 2 * 1024L);

    //now as all the slots are occupied hence no more tasks would be
    //assigned.
    assertNull(scheduler.assignTasks(tracker("tt1")));
  }

    /**
   * Tests that scheduler schedules normal jobs once high RAM jobs
   * have been reserved to the limit.
   *
   * The test causes the scheduler to schedule a normal job on two
   * trackers, and one task of the high RAM job on a third. Then it
   * asserts that one of the first two trackers gets a reservation
   * for the remaining task of the high RAM job. After this, it
   * asserts that a normal job submitted later is allowed to run
   * on a free slot, as all tasks of the high RAM job are either
   * scheduled or reserved.
   *
   * @throws IOException
   */
  public void testClusterBlockingForLackOfMemory()
      throws IOException {

      LOG.debug("Starting the scheduler.");
      taskTrackerManager = new FakeTaskTrackerManager(3, 2, 2);

      ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
      queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
      taskTrackerManager.addQueues(new String[]{"default"});
      scheduler.setTaskTrackerManager(taskTrackerManager);
      // enabled memory-based scheduling
      // Normal jobs 1GB maps/reduces. 2GB limit on maps/reduces
      scheduler.getConf().setLong(JTConfig.JT_MAX_MAPMEMORY_MB, 2 * 1024);
      scheduler.getConf().setLong(MRConfig.MAPMEMORY_MB, 1 * 1024);
      scheduler.getConf().setLong(JTConfig.JT_MAX_REDUCEMEMORY_MB, 2 * 1024);
      scheduler.getConf().setLong(MRConfig.REDUCEMEMORY_MB, 1 * 1024);
      taskTrackerManager.setFakeQueues(queues);
      scheduler.start();

      LOG.debug(
        "Submit one normal memory(1GB maps/reduces) job of "
          + "2 map, 2 reduce tasks.");
      JobConf jConf = new JobConf(conf);
      jConf.setMemoryForMapTask(1 * 1024);
      jConf.setMemoryForReduceTask(1 * 1024);
      jConf.setNumMapTasks(2);
      jConf.setNumReduceTasks(2);
      jConf.setQueueName("default");
      jConf.setUser("u1");
      FakeJobInProgress job1 = taskTrackerManager.submitJobAndInit(
        JobStatus.PREP, jConf);

      // Fill a tt with this job's tasks.
      Map<String, String> expectedStrings = new HashMap<String, String>();
      expectedStrings.put(
        CapacityTestUtils.MAP, "attempt_test_0001_m_000001_0 on tt1");
      expectedStrings.put(
        CapacityTestUtils.REDUCE, "attempt_test_0001_r_000001_0 on tt1");
      checkMultipleTaskAssignment(
        taskTrackerManager, scheduler, "tt1",
        expectedStrings);
      // Total 1 map slot should be accounted for.
      checkOccupiedSlots("default", TaskType.MAP, 1, 1, 16.7f);
      checkOccupiedSlots("default", TaskType.REDUCE, 1, 1, 16.7f);
      assertEquals(
        String.format(
          TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING,
          1, 1, 0, 1, 1, 0),
        (String) job1.getSchedulingInfo());
      checkMemReservedForTasksOnTT("tt1", 1 * 1024L, 1 * 1024L);

      expectedStrings.clear();
      expectedStrings.put(
        CapacityTestUtils.MAP, "attempt_test_0001_m_000002_0 on tt2");
      expectedStrings.put(
        CapacityTestUtils.REDUCE, "attempt_test_0001_r_000002_0 on tt2");

      // fill another TT with the rest of the tasks of the job
      checkMultipleTaskAssignment(
        taskTrackerManager, scheduler, "tt2",
        expectedStrings);

      LOG.debug(
        "Submit one high memory(2GB maps/reduces) job of "
          + "2 map, 2 reduce tasks.");
      jConf = new JobConf(conf);
      jConf.setMemoryForMapTask(2 * 1024);
      jConf.setMemoryForReduceTask(2 * 1024);
      jConf.setNumMapTasks(2);
      jConf.setNumReduceTasks(2);
      jConf.setQueueName("default");
      jConf.setUser("u1");
      FakeJobInProgress job2 = taskTrackerManager.submitJobAndInit(
        JobStatus.PREP, jConf);

      // Have another TT run one task of each type of the high RAM
      // job. This will fill up the TT.
      expectedStrings.clear();
      expectedStrings.put(
        CapacityTestUtils.MAP, "attempt_test_0002_m_000001_0 on tt3");
      expectedStrings.put(
        CapacityTestUtils.REDUCE, "attempt_test_0002_r_000001_0 on tt3");

      checkMultipleTaskAssignment(taskTrackerManager, scheduler,
          "tt3", expectedStrings);
      checkOccupiedSlots("default", TaskType.MAP, 1, 4, 66.7f);
      checkOccupiedSlots("default", TaskType.REDUCE, 1, 4, 66.7f);
      assertEquals(
        String.format(
          TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING,
          1, 2, 0, 1, 2, 0),
        (String) job2.getSchedulingInfo());
      checkMemReservedForTasksOnTT("tt3", 2 * 1024L, 2 * 1024L);

      LOG.debug(
        "Submit one normal memory(1GB maps/reduces) job of "
          + "1 map, 1 reduce tasks.");
      jConf = new JobConf(conf);
      jConf.setMemoryForMapTask(1 * 1024);
      jConf.setMemoryForReduceTask(1 * 1024);
      jConf.setNumMapTasks(1);
      jConf.setNumReduceTasks(1);
      jConf.setQueueName("default");
      jConf.setUser("u1");
      FakeJobInProgress job3 = taskTrackerManager.submitJobAndInit(
        JobStatus.PREP, jConf);

      // Send a TT with insufficient space for task assignment,
      // This will cause a reservation for the high RAM job.
      assertNull(scheduler.assignTasks(tracker("tt1")));

      // reserved tasktrackers contribute to occupied slots for maps and reduces
      checkOccupiedSlots("default", TaskType.MAP, 1, 6, 100.0f);
      checkOccupiedSlots("default", TaskType.REDUCE, 1, 6, 100.0f);
      checkMemReservedForTasksOnTT("tt1", 1 * 1024L, 1 * 1024L);
      LOG.info(job2.getSchedulingInfo());
      assertEquals(
        String.format(
          TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING,
          1, 2, 2, 1, 2, 2),
        (String) job2.getSchedulingInfo());
      assertEquals(
        String.format(
          TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING,
          0, 0, 0, 0, 0, 0),
        (String) job3.getSchedulingInfo());

      // Reservations are already done for job2. So job3 should go ahead.
      expectedStrings.clear();
      expectedStrings.put(
        CapacityTestUtils.MAP, "attempt_test_0003_m_000001_0 on tt2");
      expectedStrings.put(
        CapacityTestUtils.REDUCE, "attempt_test_0003_r_000001_0 on tt2");

      checkMultipleTaskAssignment(
        taskTrackerManager, scheduler, "tt2",
        expectedStrings);
    }

  /**
   * Testcase to verify fix for a NPE (HADOOP-5641), when memory based
   * scheduling is enabled and jobs are retired from memory when tasks
   * are still active on some Tasktrackers.
   *
   * @throws IOException
   */
  public void testMemoryMatchingWithRetiredJobs() throws IOException {
    // create a cluster with a single node.
    LOG.debug("Starting cluster with 1 tasktracker, 2 map and 2 reduce slots");
    taskTrackerManager = new FakeTaskTrackerManager(1, 2, 2);

    // create scheduler
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    taskTrackerManager.addQueues(new String[]{"default"});


    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    LOG.debug("Assume TT has 2GB for maps and 2GB for reduces");
    scheduler.getConf().setLong(JTConfig.JT_MAX_MAPMEMORY_MB, 2 * 1024L);
    scheduler.getConf().setLong(MRConfig.MAPMEMORY_MB, 512);
    scheduler.getConf().setLong(JTConfig.JT_MAX_REDUCEMEMORY_MB, 2 * 1024L);
    scheduler.getConf().setLong(MRConfig.REDUCEMEMORY_MB, 512);
    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    // submit a normal job
    LOG.debug("Submitting a normal job with 2 maps and 2 reduces");
    JobConf jConf = new JobConf();
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setMemoryForMapTask(512);
    jConf.setMemoryForReduceTask(512);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, jConf);

    // 1st cycle - 1 map and reduce gets assigned.
    Map<String, String> expectedStrings = new HashMap<String, String>();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0001_r_000001_0 on tt1");
    List<Task> t = checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);
    // Total 1 map slot and 1 reduce slot should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 1, 1, 50.0f);
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 1, 50.0f);
    checkMemReservedForTasksOnTT("tt1", 512L, 512L);

    // kill this job !
    taskTrackerManager.killJob(job1.getJobID(), false);
    // No more map/reduce slots should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 0, 0, 0.0f);
    checkOccupiedSlots(
      "default", TaskType.REDUCE, 0, 0,
      0.0f);

    // retire the job
    taskTrackerManager.retireJob(job1.getJobID());

    // submit another job.
    LOG.debug("Submitting another normal job with 2 maps and 2 reduces");
    jConf = new JobConf();
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setMemoryForMapTask(512);
    jConf.setMemoryForReduceTask(512);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, jConf);

    // since with HADOOP-5964, we don't rely on a job conf to get
    // the memory occupied, scheduling should be able to work correctly.
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0002_m_000001_0 on tt1");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0002_r_000001_0 on tt1");

    List<Task> t1 = checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);
    checkOccupiedSlots("default", TaskType.MAP, 1, 1, 50);
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 1, 50);
    checkMemReservedForTasksOnTT("tt1", 1024L, 1024L);

    // now, no more can be assigned because all the slots are blocked.
    assertNull(scheduler.assignTasks(tracker("tt1")));

    // finish the tasks on the tracker.
    for (Task task : t) {
      taskTrackerManager.finishTask(task.getTaskID().toString(), job1);
    }
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0002_m_000002_0 on tt1");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0002_r_000002_0 on tt1");

    // now a new task can be assigned.
    t = checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);
    checkOccupiedSlots("default", TaskType.MAP, 1, 2, 100.0f);
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 2, 100.0f);
    // memory used will change because of the finished task above.
    checkMemReservedForTasksOnTT("tt1", 1024L, 1024L);
  }

  /*
   * Test cases for Job Initialization poller.
   */

  /*
  * This test verifies that the correct number of jobs for
  * correct number of users is initialized.
  * It also verifies that as jobs of users complete, new jobs
  * from the correct users are initialized.
  */

  public void testJobInitialization() throws Exception {
    // set up the scheduler
    String[] qs = {"default"};
    taskTrackerManager = new FakeTaskTrackerManager(2, 1, 1);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller initPoller = scheduler.getInitializationPoller();

    // submit 4 jobs each for 3 users.
    HashMap<String, ArrayList<FakeJobInProgress>> userJobs =
      taskTrackerManager.submitJobs(
        3,
        4, "default");

    // get the jobs submitted.
    ArrayList<FakeJobInProgress> u1Jobs = userJobs.get("u1");
    ArrayList<FakeJobInProgress> u2Jobs = userJobs.get("u2");
    ArrayList<FakeJobInProgress> u3Jobs = userJobs.get("u3");

    // reference to the initializedJobs data structure
    // changes are reflected in the set as they are made by the poller
    Set<JobID> initializedJobs = initPoller.getInitializedJobList();

    // we should have 12 (3 x 4) jobs in the job queue
    assertEquals(mgr.getJobQueue("default").getWaitingJobs().size(), 12);

    // run one poller iteration.
    controlledInitializationPoller.selectJobsToInitialize();

    // the poller should initialize 6 jobs
    // 3 users and 2 jobs from each
    assertEquals(initializedJobs.size(), 6);

    assertTrue(
      "Initialized jobs didnt contain the user1 job 1",
      initializedJobs.contains(u1Jobs.get(0).getJobID()));
    assertTrue(
      "Initialized jobs didnt contain the user1 job 2",
      initializedJobs.contains(u1Jobs.get(1).getJobID()));
    assertTrue(
      "Initialized jobs didnt contain the user2 job 1",
      initializedJobs.contains(u2Jobs.get(0).getJobID()));
    assertTrue(
      "Initialized jobs didnt contain the user2 job 2",
      initializedJobs.contains(u2Jobs.get(1).getJobID()));
    assertTrue(
      "Initialized jobs didnt contain the user3 job 1",
      initializedJobs.contains(u3Jobs.get(0).getJobID()));
    assertTrue(
      "Initialized jobs didnt contain the user3 job 2",
      initializedJobs.contains(u3Jobs.get(1).getJobID()));

    // now submit one more job from another user.
    FakeJobInProgress u4j1 =
      taskTrackerManager.submitJob(JobStatus.PREP, 1, 1, "default", "u4");

    // run the poller again.
    controlledInitializationPoller.selectJobsToInitialize();

    // since no jobs have started running, there should be no
    // change to the initialized jobs.
    assertEquals(initializedJobs.size(), 6);
    assertFalse(
      "Initialized jobs contains user 4 jobs",
      initializedJobs.contains(u4j1.getJobID()));

    // This event simulates raising the event on completion of setup task
    // and moves the job to the running list for the scheduler to pick up.
    raiseStatusChangeEvents(mgr);

    // get some tasks assigned.
    Map<String, String> expectedStrings = new HashMap<String, String>();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0001_r_000001_0 on tt1");

    List<Task> t1 = checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);

    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0002_m_000001_0 on tt2");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0002_r_000001_0 on tt2");

    List<Task> t2 = checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      expectedStrings);

    for (Task task : t1) {
      taskTrackerManager.finishTask(
        task.getTaskID().toString(), u1Jobs.get(
          0));
    }

    for (Task task : t2) {
      taskTrackerManager.finishTask(
        task.getTaskID().toString(), u1Jobs.get(
          0));
    }
    // as some jobs have running tasks, the poller will now
    // pick up new jobs to initialize.
    controlledInitializationPoller.selectJobsToInitialize();

    // count should still be the same
    assertEquals(initializedJobs.size(), 6);

    // new jobs that have got into the list
    assertTrue(initializedJobs.contains(u1Jobs.get(2).getJobID()));
    assertTrue(initializedJobs.contains(u1Jobs.get(3).getJobID()));
    raiseStatusChangeEvents(mgr);

    // the first two jobs are done, no longer in the initialized list.
    assertFalse(
      "Initialized jobs contains the user1 job 1",
      initializedJobs.contains(u1Jobs.get(0).getJobID()));
    assertFalse(
      "Initialized jobs contains the user1 job 2",
      initializedJobs.contains(u1Jobs.get(1).getJobID()));

    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0003_m_000001_0 on tt1");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0003_r_000001_0 on tt1");

    // finish one more job
    t1 = checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);
    for (Task task : t1) {
      taskTrackerManager.finishTask(
        task.getTaskID().toString(), u1Jobs.get(
          2));
    }

    // no new jobs should be picked up, because max user limit
    // is still 3.
    controlledInitializationPoller.selectJobsToInitialize();

    assertEquals(initializedJobs.size(), 5);

    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0004_m_000001_0 on tt1");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0004_r_000001_0 on tt1");

    // run 1 more jobs.. 
    t1 = checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);
    for (Task task : t1) {
      taskTrackerManager.finishTask(
        task.getTaskID().toString(), u1Jobs.get(
          3));
    }

    // Now initialised jobs should contain user 4's job, as
    // user 1's jobs are all done and the number of users is
    // below the limit
    controlledInitializationPoller.selectJobsToInitialize();
    assertEquals(initializedJobs.size(), 5);
    assertTrue(initializedJobs.contains(u4j1.getJobID()));

    controlledInitializationPoller.stopRunning();
  }

  /*
   * testHighPriorityJobInitialization() shows behaviour when high priority job
   * is submitted into a queue and how initialisation happens for the same.
   */
  public void testHighPriorityJobInitialization() throws Exception {
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    JobInitializationPoller initPoller = scheduler.getInitializationPoller();
    Set<JobID> initializedJobsList = initPoller.getInitializedJobList();

    // submit 3 jobs for 3 users
    taskTrackerManager.submitJobs(3, 3, "default");
    controlledInitializationPoller.selectJobsToInitialize();
    assertEquals(initializedJobsList.size(), 6);

    // submit 2 job for a different user. one of them will be made high priority
    FakeJobInProgress u4j1 = taskTrackerManager.submitJob(JobStatus.PREP, 1, 1, "default", "u4");
    FakeJobInProgress u4j2 = taskTrackerManager.submitJob(JobStatus.PREP, 1, 1, "default", "u4");

    controlledInitializationPoller.selectJobsToInitialize();

    // shouldn't change
    assertEquals(initializedJobsList.size(), 6);

    assertFalse(
      "Contains U4J1 high priority job ",
      initializedJobsList.contains(u4j1.getJobID()));
    assertFalse(
      "Contains U4J2 Normal priority job ",
      initializedJobsList.contains(u4j2.getJobID()));

    // change priority of one job
    taskTrackerManager.setPriority(u4j1, JobPriority.VERY_HIGH);

    controlledInitializationPoller.selectJobsToInitialize();

    // the high priority job should get initialized, but not the
    // low priority job from u4, as we have already exceeded the
    // limit.
    assertEquals(initializedJobsList.size(), 7);
    assertTrue(
      "Does not contain U4J1 high priority job ",
      initializedJobsList.contains(u4j1.getJobID()));
    assertFalse(
      "Contains U4J2 Normal priority job ",
      initializedJobsList.contains(u4j2.getJobID()));
    controlledInitializationPoller.stopRunning();
  }

  public void testJobMovement() throws Exception {
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    JobQueuesManager mgr = scheduler.jobQueuesManager;

    // check proper running job movement and completion
    checkRunningJobMovementAndCompletion();

    // check failed running job movement
    checkFailedRunningJobMovement();

    // Check job movement of failed initalized job
    checkFailedInitializedJobMovement();

    // Check failed waiting job movement
    checkFailedWaitingJobMovement();
  }

  public void testStartWithoutDefaultQueueConfigured() throws Exception {
    //configure a single queue which is not default queue
    String[] qs = {"q1"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("q1", 100.0f, true, 100));


    taskTrackerManager.setFakeQueues(queues);
    //Start the scheduler.
    scheduler.start();
    //Submit a job and wait till it completes
    FakeJobInProgress job =
      taskTrackerManager.submitJob(JobStatus.PREP, 1, 1, "q1", "u1");
    controlledInitializationPoller.selectJobsToInitialize();
    raiseStatusChangeEvents(scheduler.jobQueuesManager, "q1");
    Map<String,String> strs = new HashMap<String,String>();
    strs.put(CapacityTestUtils.MAP,"attempt_test_0001_m_000001_0 on tt1");
    strs.put(CapacityTestUtils.REDUCE,"attempt_test_0001_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      strs);

  }

  public void testFailedJobInitalizations() throws Exception {
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    JobQueuesManager mgr = scheduler.jobQueuesManager;

    //Submit a job whose initialization would fail always.
    FakeJobInProgress job =
      new FakeFailingJobInProgress(
        new JobID("test", ++jobCounter),
        new JobConf(), taskTrackerManager, "u1", UtilsForTests.getJobTracker());
    job.getStatus().setRunState(JobStatus.PREP);
    taskTrackerManager.submitJob(job);
    //check if job is present in waiting list.
    assertEquals(
      "Waiting job list does not contain submitted job",
      1, mgr.getJobQueue("default").getWaitingJobCount());
    assertTrue(
      "Waiting job does not contain submitted job",
      mgr.getJobQueue("default").getWaitingJobs().contains(job));
    //initialization should fail now.
    controlledInitializationPoller.selectJobsToInitialize();
    //Check if the job has been properly cleaned up.
    assertEquals(
      "Waiting job list contains submitted job",
      0, mgr.getJobQueue("default").getWaitingJobCount());
    assertFalse(
      "Waiting job contains submitted job",
      mgr.getJobQueue("default").getWaitingJobs().contains(job));
    assertFalse(
      "Waiting job contains submitted job",
      mgr.getJobQueue("default").getRunningJobs().contains(job));
  }

  /**
   * Test case deals with normal jobs which have speculative maps and reduce.
   * Following is test executed
   * <ol>
   * <li>Submit one job with speculative maps and reduce.</li>
   * <li>Submit another job with no speculative execution.</li>
   * <li>Observe that all tasks from first job get scheduled, speculative
   * and normal tasks</li>
   * <li>Finish all the first jobs tasks second jobs tasks get scheduled.</li>
   * </ol>
   *
   * @throws IOException
   */
  public void testSpeculativeTaskScheduling() throws IOException {
    String[] qs = {"default"};
    taskTrackerManager = new FakeTaskTrackerManager(2, 1, 1);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));


    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobConf conf = new JobConf();
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    conf.setMapSpeculativeExecution(true);
    conf.setReduceSpeculativeExecution(true);
    //Submit a job which would have one speculative map and one speculative
    //reduce.
    FakeJobInProgress fjob1 = taskTrackerManager.submitJob(
      JobStatus.PREP, conf);

    conf = new JobConf();
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    //Submit a job which has no speculative map or reduce.
    FakeJobInProgress fjob2 = taskTrackerManager.submitJob(
      JobStatus.PREP, conf);

    //Ask the poller to initalize all the submitted job and raise status
    //change event.
    controlledInitializationPoller.selectJobsToInitialize();
    raiseStatusChangeEvents(mgr);
    Map<String, String> strs = new HashMap<String, String>();
    strs.put(CapacityTestUtils.MAP, "attempt_test_0001_m_000001_0 on tt1");
    strs.put(CapacityTestUtils.REDUCE, "attempt_test_0001_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      strs);
    assertTrue(
      "Pending maps of job1 greater than zero",
      (fjob1.pendingMaps() == 0));

    assertTrue(
      "Pending reduces of job1 greater than zero",
      (fjob1.pendingReduces() == 0));

    Map<String, String> str = new HashMap<String, String>();
    str.put(CapacityTestUtils.MAP, "attempt_test_0001_m_000001_1 on tt2");
    str.put(CapacityTestUtils.REDUCE, "attempt_test_0001_r_000001_1 on tt2");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      str);

    taskTrackerManager.finishTask("attempt_test_0001_m_000001_0", fjob1);
    taskTrackerManager.finishTask("attempt_test_0001_m_000001_1", fjob1);
    taskTrackerManager.finishTask("attempt_test_0001_r_000001_0", fjob1);
    taskTrackerManager.finishTask("attempt_test_0001_r_000001_1", fjob1);
    taskTrackerManager.finalizeJob(fjob1);

    str.clear();
    str.put(CapacityTestUtils.MAP, "attempt_test_0002_m_000001_0 on tt1");
    str.put(CapacityTestUtils.REDUCE, "attempt_test_0002_r_000001_0 on tt1");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      str);
    taskTrackerManager.finishTask("attempt_test_0002_m_000001_0", fjob2);
    taskTrackerManager.finishTask("attempt_test_0002_r_000001_0", fjob2);
    taskTrackerManager.finalizeJob(fjob2);
  }

  /**
   * Test to verify that TTs are reserved for high memory jobs, but only till a
   * TT is reserved for each of the pending task.
   *
   * @throws IOException
   */
  public void testTTReservingWithHighMemoryJobs()
    throws IOException {
    // 3 taskTrackers, 2 map and 0 reduce slots on each TT
    taskTrackerManager = new FakeTaskTrackerManager(3, 2, 0);

    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));


    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(JTConfig.JT_MAX_MAPMEMORY_MB, 2 * 1024);
    scheduler.getConf().setLong(MRConfig.MAPMEMORY_MB, 1 * 1024);
    scheduler.getConf().setLong(JTConfig.JT_MAX_REDUCEMEMORY_MB, 1 * 1024);
    scheduler.getConf().setLong(MRConfig.REDUCEMEMORY_MB, 1 * 1024);
    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    LOG.debug(
      "Submit a regular memory(1GB vmem maps/reduces) job of "
        + "3 map/red tasks");
    JobConf jConf = new JobConf(conf);
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(3);
    jConf.setNumReduceTasks(3);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = taskTrackerManager.submitJobAndInit(JobStatus.PREP, jConf);

    // assign one map task of job1 on all the TTs
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment(
      taskTrackerManager, scheduler, "tt3",
      "attempt_test_0001_m_000003_0 on tt3");
    scheduler.updateContextInfoForTests();

    LOG.info(job1.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING, 3, 3, 0, 0,
        0, 0), (String) job1.getSchedulingInfo());

    LOG.debug(
      "Submit one high memory(2GB maps, 0MB reduces) job of "
        + "2 map tasks");
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(0);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = taskTrackerManager.submitJobAndInit(JobStatus.PREP, jConf);

    LOG.debug(
      "Submit another regular memory(1GB vmem maps/reduces) job of "
        + "2 map/red tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job3 = taskTrackerManager.submitJobAndInit(JobStatus.PREP, jConf);

    // Job2, a high memory job cannot be accommodated on a any TT. But with each
    // trip to the scheduler, each of the TT should be reserved by job2.
    assertNull(scheduler.assignTasks(tracker("tt1")));
    scheduler.updateContextInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 2, 0,
        0, 0), (String) job2.getSchedulingInfo());

    assertNull(scheduler.assignTasks(tracker("tt2")));
    scheduler.updateContextInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 4, 0,
        0, 0), (String) job2.getSchedulingInfo());

    // Job2 has only 2 pending tasks. So no more reservations. Job3 should get
    // slots on tt3. tt1 and tt2 should not be assigned any slots with the
    // reservation stats intact.
    assertNull(scheduler.assignTasks(tracker("tt1")));
    scheduler.updateContextInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 4, 0,
        0, 0), (String) job2.getSchedulingInfo());

    assertNull(scheduler.assignTasks(tracker("tt2")));
    scheduler.updateContextInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 4, 0,
        0, 0), (String) job2.getSchedulingInfo());

    checkAssignment(
      taskTrackerManager, scheduler, "tt3",
      "attempt_test_0003_m_000001_0 on tt3");
    scheduler.updateContextInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 4, 0,
        0, 0), (String) job2.getSchedulingInfo());

    // No more tasks there in job3 also
    assertNull(scheduler.assignTasks(tracker("tt3")));
  }

  /**
   * Test to verify that queue ordering is based on the number of slots occupied
   * and hence to verify that presence of high memory jobs is reflected properly
   * while determining used capacities of queues and hence the queue ordering.
   *
   * @throws IOException
   */
  public void testQueueOrdering()
    throws IOException {
    taskTrackerManager = new FakeTaskTrackerManager(2, 6, 6);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    String[] qs = {"default", "q1"};
    String[] reversedQs = {qs[1], qs[0]};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 100));
    queues.add(new FakeQueueInfo("q1", 50.0f, true, 100));


    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(JTConfig.JT_MAX_MAPMEMORY_MB, 2 * 1024);
    scheduler.getConf().setLong(MRConfig.MAPMEMORY_MB, 1 * 1024);
    scheduler.getConf().setLong(JTConfig.JT_MAX_REDUCEMEMORY_MB, 1 * 1024);
    scheduler.getConf().setLong(MRConfig.REDUCEMEMORY_MB, 1 * 1024);
    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    LOG.debug(
      "Submit one high memory(2GB maps, 2GB reduces) job of "
        + "6 map and 6 reduce tasks");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, jConf);

    // Submit a normal job to the other queue.
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setUser("u1");
    jConf.setQueueName("q1");
    FakeJobInProgress job2 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, jConf);

    // Map and reduce of high memory job should be assigned
    HashMap<String, String> expectedStrings = new HashMap<String, String>();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0001_r_000001_0 on tt1");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);

    checkQueuesOrder(
      qs, scheduler
        .getOrderedQueues(TaskType.MAP));

    checkQueuesOrder(
      qs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // 1st map and reduce of normal job should be assigned
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0002_m_000001_0 on tt1");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0002_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);

    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));
    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // 2nd map and reduce of normal job should be assigned
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0002_m_000002_0 on tt1");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0002_r_000002_0 on tt1");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);
    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));
    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // Now both the queues are equally served. But the comparator doesn't change
    // the order if queues are equally served.
    // Hence, 3rd map and reduce of normal job should be assigned
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0002_m_000003_0 on tt2");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0002_r_000003_0 on tt2");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      expectedStrings);

    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));

    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // 2nd map and reduce of high memory job should be assigned
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0001_m_000002_0 on tt2");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0001_r_000002_0 on tt2");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      expectedStrings);
    checkQueuesOrder(
      qs, scheduler
        .getOrderedQueues(TaskType.MAP));

    checkQueuesOrder(
      qs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // 4th map and reduce of normal job should be assigned.
    expectedStrings.clear();
    expectedStrings.put(
      CapacityTestUtils.MAP, "attempt_test_0002_m_000004_0 on tt2");
    expectedStrings.put(
      CapacityTestUtils.REDUCE, "attempt_test_0002_r_000004_0 on tt2");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      expectedStrings);
    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));

    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));
  }

  /**
   * Tests whether 1 map and 1 reduce are assigned even if reduces span across
   * multiple jobs or multiple queues.
   *
   * creates a cluster of 6 maps and 2 reduces.
   * Submits 2 jobs:
   * job1 , with 6 map and 1 reduces
   * job2 with  2 map and 1 reduces
   *
   *
   * check that first assignment assigns a map and a reduce.
   * check that second assignment assigns a map and a reduce
   * (both from other job and other queue)
   *
   * the last 2 calls just checks to make sure that we dont get further reduces
   * 
   * @throws Exception
   */
  public void testMultiTaskAssignmentInMultipleQueues() throws Exception {
    setUp(1, 6, 2);
    // set up some queues
    String[] qs = {"default", "q1"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q1", 50.0f, true, 25));
    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    //Submit the job with 6 maps and 2 reduces
    taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, 6, 1, "default", "u1");

    FakeJobInProgress j2 = taskTrackerManager.submitJobAndInit(
      JobStatus.PREP, 2, 1, "q1", "u2");

    Map<String, String> str = new HashMap<String, String>();
    str.put(MAP, "attempt_test_0001_m_000001_0 on tt1");
    str.put(REDUCE, "attempt_test_0001_r_000001_0 on tt1");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler, "tt1", str);

    // next assignment will be for job in second queue.
    str.clear();
    str.put(MAP, "attempt_test_0002_m_000001_0 on tt1");
    str.put(REDUCE, "attempt_test_0002_r_000001_0 on tt1");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler, "tt1", str);

    //now both the reduce slots are being used , hence we sholdnot get only 1
    //map task in this assignTasks call.
    str.clear();
    str.put(MAP, "attempt_test_0002_m_000002_0 on tt1");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler, "tt1", str);

    str.clear();
    str.put(MAP, "attempt_test_0001_m_000002_0 on tt1");
    checkMultipleTaskAssignment(taskTrackerManager, scheduler, "tt1", str);
  }


  private void checkRunningJobMovementAndCompletion() throws IOException {

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller p = scheduler.getInitializationPoller();
    // submit a job
    FakeJobInProgress job =
      taskTrackerManager.submitJob(JobStatus.PREP, 1, 1, "default", "u1");
    controlledInitializationPoller.selectJobsToInitialize();

    assertEquals(p.getInitializedJobList().size(), 1);

    // make it running.
    raiseStatusChangeEvents(mgr);

    // it should be there in both the queues.
    assertTrue(
      "Job not present in Job Queue",
      mgr.getJobQueue("default").getWaitingJobs().contains(job));
    assertTrue(
      "Job not present in Running Queue",
      mgr.getJobQueue("default").getRunningJobs().contains(job));

    // assign a task
    Map<String,String> strs = new HashMap<String,String>();
    strs.put(MAP,"attempt_test_0001_m_000001_0 on tt1");
    strs.put(REDUCE,"attempt_test_0001_r_000001_0 on tt1");

    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      strs);

    controlledInitializationPoller.selectJobsToInitialize();

    // now this task should be removed from the initialized list.
    assertTrue(p.getInitializedJobList().isEmpty());

    // the job should also be removed from the job queue as tasks
    // are scheduled
    assertFalse(
      "Job present in Job Queue",
      mgr.getJobQueue("default").getWaitingJobs().contains(job));

    // complete tasks and job
    taskTrackerManager.finishTask("attempt_test_0001_m_000001_0", job);
    taskTrackerManager.finishTask("attempt_test_0001_r_000001_0", job);
    taskTrackerManager.finalizeJob(job);

    // make sure it is removed from the run queue
    assertFalse(
      "Job present in running queue",
      mgr.getJobQueue("default").getRunningJobs().contains(job));
  }

  private void checkFailedRunningJobMovement() throws IOException {

    JobQueuesManager mgr = scheduler.jobQueuesManager;

    //submit a job and initalized the same
    FakeJobInProgress job =
      taskTrackerManager.submitJobAndInit(JobStatus.RUNNING, 1, 1, "default", "u1");

    //check if the job is present in running queue.
    assertTrue(
      "Running jobs list does not contain submitted job",
      mgr.getJobQueue("default").getRunningJobs().contains(job));

    taskTrackerManager.finalizeJob(job, JobStatus.KILLED);

    //check if the job is properly removed from running queue.
    assertFalse(
      "Running jobs list does not contain submitted job",
      mgr.getJobQueue("default").getRunningJobs().contains(job));

  }

  private void checkFailedInitializedJobMovement() throws IOException {

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller p = scheduler.getInitializationPoller();

    //submit a job
    FakeJobInProgress job = taskTrackerManager.submitJob(JobStatus.PREP, 1, 1, "default", "u1");
    //Initialize the job
    p.selectJobsToInitialize();
    //Don't raise the status change event.

    //check in waiting and initialized jobs list.
    assertTrue(
      "Waiting jobs list does not contain the job",
      mgr.getJobQueue("default").getWaitingJobs().contains(job));

    assertTrue(
      "Initialized job does not contain the job",
      p.getInitializedJobList().contains(job.getJobID()));

    //fail the initalized job
    taskTrackerManager.finalizeJob(job, JobStatus.KILLED);

    //Check if the job is present in waiting queue
    assertFalse(
      "Waiting jobs list contains failed job",
      mgr.getJobQueue("default").getWaitingJobs().contains(job));

    //run the poller to do the cleanup
    p.selectJobsToInitialize();

    //check for failed job in the initialized job list
    assertFalse(
      "Initialized jobs  contains failed job",
      p.getInitializedJobList().contains(job.getJobID()));
  }

  private void checkFailedWaitingJobMovement() throws IOException {
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    // submit a job
    FakeJobInProgress job = taskTrackerManager.submitJob(
      JobStatus.PREP, 1, 1, "default",
      "u1");

    // check in waiting and initialized jobs list.
    assertTrue(
      "Waiting jobs list does not contain the job", mgr
        .getJobQueue("default").getWaitingJobs().contains(job));
    // fail the waiting job
    taskTrackerManager.finalizeJob(job, JobStatus.KILLED);

    // Check if the job is present in waiting queue
    assertFalse(
      "Waiting jobs list contains failed job", mgr
        .getJobQueue("default").getWaitingJobs().contains(job));
  }

  private void raiseStatusChangeEvents(JobQueuesManager mgr) {
    raiseStatusChangeEvents(mgr, "default");
  }

  private void raiseStatusChangeEvents(JobQueuesManager mgr, String queueName) {
    Collection<JobInProgress> jips = mgr.getJobQueue(queueName)
      .getWaitingJobs();
    for (JobInProgress jip : jips) {
      if (jip.getStatus().getRunState() == JobStatus.RUNNING) {
        JobStatusChangeEvent evt = new JobStatusChangeEvent(
          jip,
          EventType.RUN_STATE_CHANGED, jip.getStatus());
        mgr.jobUpdated(evt);
      }
    }
  }

  protected TaskTracker tracker(String taskTrackerName) {
    return taskTrackerManager.getTaskTracker(taskTrackerName);
  }

  /**
   * Get the amount of memory that is reserved for tasks on the taskTracker and
   * verify that it matches what is expected.
   *
   * @param taskTracker
   * @param expectedMemForMapsOnTT
   * @param expectedMemForReducesOnTT
   */
  private void checkMemReservedForTasksOnTT(
    String taskTracker,
    Long expectedMemForMapsOnTT, Long expectedMemForReducesOnTT) {
    Long observedMemForMapsOnTT =
      scheduler.memoryMatcher.getMemReservedForTasks(
        tracker(taskTracker).getStatus(),
        TaskType.MAP);
    Long observedMemForReducesOnTT =
      scheduler.memoryMatcher.getMemReservedForTasks(
        tracker(taskTracker).getStatus(),
        TaskType.REDUCE);
    if (expectedMemForMapsOnTT == null) {
      assertEquals(observedMemForMapsOnTT,null);
    } else {
      assertEquals(observedMemForMapsOnTT,expectedMemForMapsOnTT);
    }
    if (expectedMemForReducesOnTT == null) {
      assertEquals(observedMemForReducesOnTT,null);
    } else {
      assertEquals(observedMemForReducesOnTT,expectedMemForReducesOnTT);
    }
  }

  /**
   * Verify the number of slots of type 'type' from the queue 'queue'.
   * incrMapIndex and incrReduceIndex are set , when expected output string is
   * changed.these values can be set if the index of
   * "Used capacity: %d (%.1f%% of Capacity)"
   * is changed.
   *
   * @param queue
   * @param type
   * @param numActiveUsers               in the queue at present.
   * @param expectedOccupiedSlots
   * @param expectedOccupiedSlotsPercent
   * @param incrMapIndex
   * @param incrReduceIndex
   */
  private void checkOccupiedSlots(
    String queue, TaskType type, int numActiveUsers, int expectedOccupiedSlots,
    float expectedOccupiedSlotsPercent, int incrMapIndex, int incrReduceIndex) {
    scheduler.updateContextInfoForTests();
    QueueManager queueManager = scheduler.taskTrackerManager.getQueueManager();
    String schedulingInfo = queueManager.getJobQueueInfo(queue)
      .getSchedulingInfo();
    String[] infoStrings = schedulingInfo.split("\n");
    int index = -1;
    if (type.equals(TaskType.MAP)) {
      index = 7 + incrMapIndex;
    } else if (type.equals(TaskType.REDUCE)) {
      index =
        (numActiveUsers == 0 ? 12 : 13 + numActiveUsers) + incrReduceIndex;
    }
    LOG.info(infoStrings[index]);
    assertEquals(
      String.format(
        "Used capacity: %d (%.1f%% of Capacity)", expectedOccupiedSlots,
        expectedOccupiedSlotsPercent), infoStrings[index]);
  }

  /**
   * @param queue
   * @param type
   * @param numActiveUsers
   * @param expectedOccupiedSlots
   * @param expectedOccupiedSlotsPercent
   */
  private void checkOccupiedSlots(
    String queue,
    TaskType type, int numActiveUsers,
    int expectedOccupiedSlots, float expectedOccupiedSlotsPercent
  ) {
    checkOccupiedSlots(
      queue, type, numActiveUsers, expectedOccupiedSlots,
      expectedOccupiedSlotsPercent, 0, 0);
  }

  private void checkQueuesOrder(
    String[] expectedOrder, String[] observedOrder) {
    assertTrue(
      "Observed and expected queues are not of same length.",
      expectedOrder.length == observedOrder.length);
    int i = 0;
    for (String expectedQ : expectedOrder) {
      assertTrue(
        "Observed and expected queues are not in the same order. "
          + "Differ at index " + i + ". Got " + observedOrder[i]
          + " instead of " + expectedQ, expectedQ.equals(observedOrder[i]));
      i++;
    }
  }

  public void testDeprecatedMemoryValues() throws IOException {
    // 2 map and 1 reduce slots
    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));

    JobConf conf = (JobConf) (scheduler.getConf());
    conf.set(
      JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY, String.valueOf(
        1024 * 1024 * 3));
    scheduler.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.setFakeQueues(queues);
    scheduler.start();

    assertEquals(MemoryMatcher.getLimitMaxMemForMapSlot(), 3);
    assertEquals(MemoryMatcher.getLimitMaxMemForReduceSlot(), 3);
  }
}
