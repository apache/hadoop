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
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FairScheduler.JobInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapred.UtilsForTests.FakeClock;

public class TestFairScheduler extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/streaming/test/data")).getAbsolutePath();
  final static String ALLOC_FILE = new File(TEST_DIR, 
      "test-pools").getAbsolutePath();
  
  private static final String POOL_PROPERTY = "pool";
  
  private static int jobCounter;
  
  class FakeJobInProgress extends JobInProgress {
    
    private FakeTaskTrackerManager taskTrackerManager;
    private int mapCounter = 0;
    private int reduceCounter = 0;
    
    public FakeJobInProgress(JobConf jobConf,
        FakeTaskTrackerManager taskTrackerManager) throws IOException {
      super(new JobID("test", ++jobCounter), jobConf, null);
      this.taskTrackerManager = taskTrackerManager;
      this.startTime = System.currentTimeMillis();
      this.status = new JobStatus();
      this.status.setRunState(JobStatus.PREP);
      this.nonLocalMaps = new LinkedList<TaskInProgress>();
      this.nonLocalRunningMaps = new LinkedHashSet<TaskInProgress>();
      this.runningMapCache = new IdentityHashMap<Node, Set<TaskInProgress>>();
      this.nonRunningReduces = new LinkedList<TaskInProgress>();   
      this.runningReduces = new LinkedHashSet<TaskInProgress>();
      initTasks();
    }
    
    @Override
    public synchronized void initTasks() throws IOException {
      // initTasks is needed to create non-empty cleanup and setup TIP
      // arrays, otherwise calls such as job.getTaskInProgress will fail
      JobID jobId = getJobID();
      JobConf conf = getJobConf();
      String jobFile = "";
      // create two cleanup tips, one map and one reduce.
      cleanup = new TaskInProgress[2];
      // cleanup map tip.
      cleanup[0] = new TaskInProgress(jobId, jobFile, null, 
              jobtracker, conf, this, numMapTasks, 1);
      cleanup[0].setJobCleanupTask();
      // cleanup reduce tip.
      cleanup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
                         numReduceTasks, jobtracker, conf, this, 1);
      cleanup[1].setJobCleanupTask();
      // create two setup tips, one map and one reduce.
      setup = new TaskInProgress[2];
      // setup map tip.
      setup[0] = new TaskInProgress(jobId, jobFile, null, 
              jobtracker, conf, this, numMapTasks + 1, 1);
      setup[0].setJobSetupTask();
      // setup reduce tip.
      setup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
                         numReduceTasks + 1, jobtracker, conf, this, 1);
      setup[1].setJobSetupTask();
      // create maps
      numMapTasks = conf.getNumMapTasks();
      System.out.println("numMapTasks = " + numMapTasks);
      maps = new TaskInProgress[numMapTasks];
      for (int i = 0; i < numMapTasks; i++) {
        maps[i] = new FakeTaskInProgress(getJobID(), 
            getJobConf(), true, this);
      }
      // create reduces
      numReduceTasks = conf.getNumReduceTasks();
      System.out.println("numReduceTasks = " + numReduceTasks);
      reduces = new TaskInProgress[numReduceTasks];
      for (int i = 0; i < numReduceTasks; i++) {
        reduces[i] = new FakeTaskInProgress(getJobID(), 
            getJobConf(), false, this);
      }
    }

    @Override
    public Task obtainNewMapTask(final TaskTrackerStatus tts, int clusterSize,
        int numUniqueHosts) throws IOException {
      TaskAttemptID attemptId = getTaskAttemptID(true);
      Task task = new MapTask("", attemptId, 0, "", new BytesWritable(), 1) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      runningMapTasks++;
      FakeTaskInProgress tip = 
        (FakeTaskInProgress) maps[attemptId.getTaskID().getId()];
      tip.createTaskAttempt(task, tts.getTrackerName());
      nonLocalRunningMaps.add(tip);
      taskTrackerManager.startTask(tts.getTrackerName(), task, tip);
      return task;
    }
    
    @Override
    public Task obtainNewReduceTask(final TaskTrackerStatus tts,
        int clusterSize, int ignored) throws IOException {
      TaskAttemptID attemptId = getTaskAttemptID(false);
      Task task = new ReduceTask("", attemptId, 0, 10, 1) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      runningReduceTasks++;
      FakeTaskInProgress tip = 
        (FakeTaskInProgress) reduces[attemptId.getTaskID().getId()];
      tip.createTaskAttempt(task, tts.getTrackerName());
      runningReduces.add(tip);
      taskTrackerManager.startTask(tts.getTrackerName(), task, tip);
      return task;
    }
    
    public void mapTaskFinished(TaskInProgress tip) {
      runningMapTasks--;
      finishedMapTasks++;
      nonLocalRunningMaps.remove(tip);
    }
    
    public void reduceTaskFinished(TaskInProgress tip) {
      runningReduceTasks--;
      finishedReduceTasks++;
      runningReduces.remove(tip);
    }
    
    private TaskAttemptID getTaskAttemptID(boolean isMap) {
      JobID jobId = getJobID();
      TaskType t = TaskType.REDUCE;
      if (isMap) {
        t = TaskType.MAP;
        return new TaskAttemptID(jobId.getJtIdentifier(),
            jobId.getId(), t, mapCounter++, 0);
      } else {
        return new TaskAttemptID(jobId.getJtIdentifier(),
            jobId.getId(), t, reduceCounter++, 0);
      }
    }
  }
  
  class FakeTaskInProgress extends TaskInProgress {
    private boolean isMap;
    private FakeJobInProgress fakeJob;
    private TreeMap<TaskAttemptID, String> activeTasks;
    private TaskStatus taskStatus;
    private boolean isComplete = false;
    
    FakeTaskInProgress(JobID jId, JobConf jobConf, boolean isMap,
        FakeJobInProgress job) {
      super(jId, "", new JobClient.RawSplit(), null, jobConf, job, 0, 1);
      this.isMap = isMap;
      this.fakeJob = job;
      activeTasks = new TreeMap<TaskAttemptID, String>();
      taskStatus = TaskStatus.createTaskStatus(isMap);
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }

    private void createTaskAttempt(Task task, String taskTracker) {
      activeTasks.put(task.getTaskID(), taskTracker);
      taskStatus = TaskStatus.createTaskStatus(isMap, task.getTaskID(),
          0.5f, 1, TaskStatus.State.RUNNING, "", "", "", 
          TaskStatus.Phase.STARTING, new Counters());
      taskStatus.setStartTime(clock.getTime());
    }
    
    @Override
    TreeMap<TaskAttemptID, String> getActiveTasks() {
      return activeTasks;
    }
    
    public synchronized boolean isComplete() {
      return isComplete;
    }
    
    public boolean isRunning() {
      return activeTasks.size() > 0;
    }
    
    @Override
    public TaskStatus getTaskStatus(TaskAttemptID taskid) {
      return taskStatus;
    }
    
    void killAttempt() {
      if (isMap) {
        fakeJob.mapTaskFinished(this);
      }
      else {
        fakeJob.reduceTaskFinished(this);
      }
      activeTasks.clear();
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }
    
    void finishAttempt() {
      isComplete = true;
      if (isMap) {
        fakeJob.mapTaskFinished(this);
      }
      else {
        fakeJob.reduceTaskFinished(this);
      }
      activeTasks.clear();
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }
  }
  
  static class FakeQueueManager extends QueueManager {
    private Set<String> queues = null;
    FakeQueueManager() {
      super(new Configuration());
    }
    void setQueues(Set<String> queues) {
      this.queues = queues;
    }
    public synchronized Set<String> getQueues() {
      return queues;
    }
  }
  
  static class FakeTaskTrackerManager implements TaskTrackerManager {
    int maps = 0;
    int reduces = 0;
    int maxMapTasksPerTracker = 2;
    int maxReduceTasksPerTracker = 2;
    long ttExpiryInterval = 10 * 60 * 1000L; // default interval
    List<JobInProgressListener> listeners =
      new ArrayList<JobInProgressListener>();
    
    private Map<String, TaskTracker> trackers =
      new HashMap<String, TaskTracker>();
    private Map<String, TaskStatus> statuses = 
      new HashMap<String, TaskStatus>();
    private Map<String, FakeTaskInProgress> tips = 
      new HashMap<String, FakeTaskInProgress>();
    private Map<String, TaskTrackerStatus> trackerForTip =
      new HashMap<String, TaskTrackerStatus>();

    public FakeTaskTrackerManager(int numTrackers) {
      for (int i = 1; i <= numTrackers; i++) {
        TaskTracker tt = new TaskTracker("tt" + i);
        tt.setStatus(new TaskTrackerStatus("tt" + i,  "host" + i, i,
            new ArrayList<TaskStatus>(), 0,
            maxMapTasksPerTracker, maxReduceTasksPerTracker));
        trackers.put("tt" + i, tt);
      }
    }
    
    @Override
    public ClusterStatus getClusterStatus() {
      int numTrackers = trackers.size();

      return new ClusterStatus(numTrackers, 0,
          ttExpiryInterval, maps, reduces,
          numTrackers * maxMapTasksPerTracker,
          numTrackers * maxReduceTasksPerTracker,
          JobTracker.State.RUNNING);
    }

    @Override
    public QueueManager getQueueManager() {
      return null;
    }
    
    @Override
    public int getNumberOfUniqueHosts() {
      return 0;
    }

    @Override
    public Collection<TaskTrackerStatus> taskTrackers() {
      List<TaskTrackerStatus> statuses = new ArrayList<TaskTrackerStatus>();
      for (TaskTracker tt : trackers.values()) {
        statuses.add(tt.getStatus());
      }
      return statuses;
    }


    @Override
    public void addJobInProgressListener(JobInProgressListener listener) {
      listeners.add(listener);
    }

    @Override
    public void removeJobInProgressListener(JobInProgressListener listener) {
      listeners.remove(listener);
    }
    
    @Override
    public int getNextHeartbeatInterval() {
      return MRConstants.HEARTBEAT_INTERVAL_MIN;
    }

    @Override
    public void killJob(JobID jobid) {
      return;
    }

    @Override
    public JobInProgress getJob(JobID jobid) {
      return null;
    }

    public void initJob (JobInProgress job) {
      // do nothing
    }
    
    public void failJob (JobInProgress job) {
      // do nothing
    }
    
    // Test methods
    
    public void submitJob(JobInProgress job) throws IOException {
      for (JobInProgressListener listener : listeners) {
        listener.jobAdded(job);
      }
    }
    
    public TaskTracker getTaskTracker(String trackerID) {
      return trackers.get(trackerID);
    }
    
    public void startTask(String trackerName, Task t, FakeTaskInProgress tip) {
      final boolean isMap = t.isMapTask();
      if (isMap) {
        maps++;
      } else {
        reduces++;
      }
      String attemptId = t.getTaskID().toString();
      TaskStatus status = tip.getTaskStatus(t.getTaskID());
      TaskTrackerStatus trackerStatus = trackers.get(trackerName).getStatus();
      tips.put(attemptId, tip);
      statuses.put(attemptId, status);
      trackerForTip.put(attemptId, trackerStatus);
      status.setRunState(TaskStatus.State.RUNNING);
      trackerStatus.getTaskReports().add(status);
    }
    
    public void finishTask(String taskTrackerName, String attemptId) {
      FakeTaskInProgress tip = tips.get(attemptId);
      if (tip.isMapTask()) {
        maps--;
      } else {
        reduces--;
      }
      tip.finishAttempt();
      TaskStatus status = statuses.get(attemptId);
      trackers.get(taskTrackerName).getStatus().getTaskReports().remove(status);
    }

    @Override
    public boolean killTask(TaskAttemptID attemptId, boolean shouldFail) {
      String attemptIdStr = attemptId.toString();
      FakeTaskInProgress tip = tips.get(attemptIdStr);
      if (tip.isMapTask()) {
        maps--;
      } else {
        reduces--;
      }
      tip.killAttempt();
      TaskStatus status = statuses.get(attemptIdStr);
      trackerForTip.get(attemptIdStr).getTaskReports().remove(status);
      return true;
    }
  }
  
  protected JobConf conf;
  protected FairScheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;
  private FakeClock clock;

  @Override
  protected void setUp() throws Exception {
    jobCounter = 0;
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    // Create an empty pools file (so we can add/remove pools later)
    FileWriter fileWriter = new FileWriter(ALLOC_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    fileWriter.write("<allocations />\n");
    fileWriter.close();
    setUpCluster(2);
  }

  private void setUpCluster(int numTaskTrackers) {
    conf = new JobConf();
    conf.set("mapred.fairscheduler.allocation.file", ALLOC_FILE);
    conf.set("mapred.fairscheduler.poolnameproperty", POOL_PROPERTY);
    conf.set("mapred.fairscheduler.assignmultiple", "false");
    taskTrackerManager = new FakeTaskTrackerManager(numTaskTrackers);
    clock = new FakeClock();
    scheduler = new FairScheduler(clock, true);
    scheduler.waitForMapsBeforeLaunchingReduces = false;
    scheduler.setConf(conf);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    scheduler.start();
  }
  
  @Override
  protected void tearDown() throws Exception {
    if (scheduler != null) {
      scheduler.terminate();
    }
  }
  
  private JobInProgress submitJob(int state, int maps, int reduces)
      throws IOException {
    return submitJob(state, maps, reduces, null);
  }
  
  private JobInProgress submitJob(int state, int maps, int reduces, String pool)
      throws IOException {
    JobConf jobConf = new JobConf(conf);
    jobConf.setNumMapTasks(maps);
    jobConf.setNumReduceTasks(reduces);
    if (pool != null)
      jobConf.set(POOL_PROPERTY, pool);
    JobInProgress job = new FakeJobInProgress(jobConf, taskTrackerManager);
    job.getStatus().setRunState(state);
    taskTrackerManager.submitJob(job);
    job.startTime = clock.time;
    return job;
  }
  
  protected void submitJobs(int number, int state, int maps, int reduces)
    throws IOException {
    for (int i = 0; i < number; i++) {
      submitJob(state, maps, reduces);
    }
  }

  public void testAllocationFileParsing() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>"); 
    // Give pool A a minimum of 1 map, 2 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>1</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("</pool>");
    // Give pool B a minimum of 2 maps, 1 reduce
    out.println("<pool name=\"poolB\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>1</minReduces>");
    out.println("</pool>");
    // Give pool C min maps but no min reduces
    out.println("<pool name=\"poolC\">");
    out.println("<minMaps>2</minMaps>");
    out.println("</pool>");
    // Give pool D a limit of 3 running jobs
    out.println("<pool name=\"poolD\">");
    out.println("<maxRunningJobs>3</maxRunningJobs>");
    out.println("</pool>");
    // Give pool E a preemption timeout of one minute
    out.println("<pool name=\"poolE\">");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    // Set default limit of jobs per pool to 15
    out.println("<poolMaxJobsDefault>15</poolMaxJobsDefault>");
    // Set default limit of jobs per user to 5
    out.println("<userMaxJobsDefault>5</userMaxJobsDefault>");
    // Give user1 a limit of 10 jobs
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>10</maxRunningJobs>");
    out.println("</user>");
    // Set default min share preemption timeout to 2 minutes
    out.println("<defaultMinSharePreemptionTimeout>120" 
        + "</defaultMinSharePreemptionTimeout>"); 
    // Set fair share preemption timeout to 5 minutes
    out.println("<fairSharePreemptionTimeout>300</fairSharePreemptionTimeout>"); 
    out.println("</allocations>"); 
    out.close();
    
    PoolManager poolManager = scheduler.getPoolManager();
    poolManager.reloadAllocs();
    
    assertEquals(6, poolManager.getPools().size()); // 5 in file + default pool
    assertEquals(0, poolManager.getAllocation(Pool.DEFAULT_POOL_NAME,
        TaskType.MAP));
    assertEquals(0, poolManager.getAllocation(Pool.DEFAULT_POOL_NAME,
        TaskType.REDUCE));
    assertEquals(1, poolManager.getAllocation("poolA", TaskType.MAP));
    assertEquals(2, poolManager.getAllocation("poolA", TaskType.REDUCE));
    assertEquals(2, poolManager.getAllocation("poolB", TaskType.MAP));
    assertEquals(1, poolManager.getAllocation("poolB", TaskType.REDUCE));
    assertEquals(2, poolManager.getAllocation("poolC", TaskType.MAP));
    assertEquals(0, poolManager.getAllocation("poolC", TaskType.REDUCE));
    assertEquals(0, poolManager.getAllocation("poolD", TaskType.MAP));
    assertEquals(0, poolManager.getAllocation("poolD", TaskType.REDUCE));
    assertEquals(0, poolManager.getAllocation("poolE", TaskType.MAP));
    assertEquals(0, poolManager.getAllocation("poolE", TaskType.REDUCE));
    assertEquals(15, poolManager.getPoolMaxJobs(Pool.DEFAULT_POOL_NAME));
    assertEquals(15, poolManager.getPoolMaxJobs("poolA"));
    assertEquals(15, poolManager.getPoolMaxJobs("poolB"));
    assertEquals(15, poolManager.getPoolMaxJobs("poolC"));
    assertEquals(3, poolManager.getPoolMaxJobs("poolD"));
    assertEquals(15, poolManager.getPoolMaxJobs("poolE"));
    assertEquals(10, poolManager.getUserMaxJobs("user1"));
    assertEquals(5, poolManager.getUserMaxJobs("user2"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout(
        Pool.DEFAULT_POOL_NAME));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolA"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolB"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolC"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolD"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolA"));
    assertEquals(60000, poolManager.getMinSharePreemptionTimeout("poolE"));
    assertEquals(300000, poolManager.getFairSharePreemptionTimeout());
  }
  
  public void testTaskNotAssignedWhenNoJobsArePresent() throws IOException {
    assertNull(scheduler.assignTasks(tracker("tt1")));
  }

  public void testNonRunningJobsAreIgnored() throws IOException {
    submitJobs(1, JobStatus.PREP, 10, 10);
    submitJobs(1, JobStatus.SUCCEEDED, 10, 10);
    submitJobs(1, JobStatus.FAILED, 10, 10);
    submitJobs(1, JobStatus.KILLED, 10, 10);
    assertNull(scheduler.assignTasks(tracker("tt1")));
    advanceTime(100); // Check that we still don't assign jobs after an update
    assertNull(scheduler.assignTasks(tracker("tt1")));
  }

  /**
   * This test contains two jobs with fewer required tasks than there are slots.
   * We check that all tasks are assigned, but job 1 gets them first because it
   * was submitted earlier.
   */
  public void testSmallJobs() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 1);
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(2,    info1.neededMaps);
    assertEquals(1,    info1.neededReduces);
    assertEquals(0,    info1.mapDeficit);
    assertEquals(0,    info1.reduceDeficit);
    assertEquals(4.0,  info1.mapFairShare);
    assertEquals(4.0,  info1.reduceFairShare);
    
    // Advance time before submitting another job j2, to make j1 run before j2
    // deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 2);
    JobInfo info2 = scheduler.infos.get(job2);
    
    // Check scheduler variables; the fair shares should now have been allocated
    // equally between j1 and j2, but j1 should have (4 slots)*(100 ms) deficit
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(2,    info1.neededMaps);
    assertEquals(1,    info1.neededReduces);
    assertEquals(400,  info1.mapDeficit);
    assertEquals(400,  info1.reduceDeficit);
    assertEquals(2.0,  info1.mapFairShare);
    assertEquals(2.0,  info1.reduceFairShare);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(1,    info2.neededMaps);
    assertEquals(2,    info2.neededReduces);
    assertEquals(0,    info2.mapDeficit);
    assertEquals(0,    info2.reduceDeficit);
    assertEquals(2.0,  info2.mapFairShare);
    assertEquals(2.0,  info2.reduceFairShare);
    
    // Assign tasks and check that all slots are filled with j1, then j2
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt2")));
    
    // Check that the scheduler has started counting the tasks as running
    // as soon as it launched them.
    assertEquals(2,  info1.runningMaps);
    assertEquals(1,  info1.runningReduces);
    assertEquals(0,  info1.neededMaps);
    assertEquals(0,  info1.neededReduces);
    assertEquals(1,  info2.runningMaps);
    assertEquals(2,  info2.runningReduces);
    assertEquals(0, info2.neededMaps);
    assertEquals(0, info2.neededReduces);
  }
  
  /**
   * This test begins by submitting two jobs with 10 maps and reduces each.
   * The first job is submitted 100ms after the second, during which time no
   * tasks run. After this, we assign tasks to all slots, which should all be
   * from job 1. These run for 200ms, at which point job 2 now has a deficit
   * of 400 while job 1 is down to a deficit of 0. We then finish all tasks and
   * assign new ones, which should all be from job 2. These run for 50 ms,
   * which is not enough time for job 2 to make up its deficit (it only makes up
   * 100 ms of deficit). Finally we assign a new round of tasks, which should
   * all be from job 2 again.
   */
  public void testLargeJobs() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(0,    info1.mapDeficit);
    assertEquals(0,    info1.reduceDeficit);
    assertEquals(4.0,  info1.mapFairShare);
    assertEquals(4.0,  info1.reduceFairShare);
    
    // Advance time before submitting another job j2, to make j1 run before j2
    // deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info2 = scheduler.infos.get(job2);
    
    // Check scheduler variables; the fair shares should now have been allocated
    // equally between j1 and j2, but j1 should have (4 slots)*(100 ms) deficit
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(400,  info1.mapDeficit);
    assertEquals(400,  info1.reduceDeficit);
    assertEquals(2.0,  info1.mapFairShare);
    assertEquals(2.0,  info1.reduceFairShare);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(10,   info2.neededMaps);
    assertEquals(10,   info2.neededReduces);
    assertEquals(0,    info2.mapDeficit);
    assertEquals(0,    info2.reduceDeficit);
    assertEquals(2.0,  info2.mapFairShare);
    assertEquals(2.0,  info2.reduceFairShare);
    
    // Assign tasks and check that all slots are initially filled with job 1
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    
    // Check that no new tasks can be launched once the tasktrackers are full
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    
    // Check that the scheduler has started counting the tasks as running
    // as soon as it launched them.
    assertEquals(4,  info1.runningMaps);
    assertEquals(4,  info1.runningReduces);
    assertEquals(6,  info1.neededMaps);
    assertEquals(6,  info1.neededReduces);
    assertEquals(0,  info2.runningMaps);
    assertEquals(0,  info2.runningReduces);
    assertEquals(10, info2.neededMaps);
    assertEquals(10, info2.neededReduces);
    
    // Finish up the tasks and advance time again. Note that we must finish
    // the task since FakeJobInProgress does not properly maintain running
    // tasks, so the scheduler will always get an empty task list from
    // the JobInProgress's getMapTasks/getReduceTasks and think they finished.
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000002_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000003_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_r_000002_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_r_000003_0");
    advanceTime(200);
    assertEquals(0,   info1.runningMaps);
    assertEquals(0,   info1.runningReduces);
    assertEquals(0,   info1.mapDeficit);
    assertEquals(0,   info1.reduceDeficit);
    assertEquals(0,   info2.runningMaps);
    assertEquals(0,   info2.runningReduces);
    assertEquals(400, info2.mapDeficit);
    assertEquals(400, info2.reduceDeficit);

    // Assign tasks and check that all slots are now filled with job 2
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000003_0 on tt2");

    // Finish up the tasks and advance time again, but give job 2 only 50ms.
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000001_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_r_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_r_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000002_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000003_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000002_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000003_0");
    advanceTime(50);
    assertEquals(0,   info1.runningMaps);
    assertEquals(0,   info1.runningReduces);
    assertEquals(100, info1.mapDeficit);
    assertEquals(100, info1.reduceDeficit);
    assertEquals(0,   info2.runningMaps);
    assertEquals(0,   info2.runningReduces);
    assertEquals(300, info2.mapDeficit);
    assertEquals(300, info2.reduceDeficit);

    // Assign tasks and check that all slots are now still with job 2
    checkAssignment("tt1", "attempt_test_0002_m_000004_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000005_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000004_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000005_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000006_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000007_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000006_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000007_0 on tt2");
  }
  

  /**
   * We submit two jobs such that one has 2x the priority of the other, wait
   * for 100 ms, and check that the weights/deficits are okay and that the
   * tasks all go to the high-priority job.
   */
  public void testJobsWithPriorities() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info2 = scheduler.infos.get(job2);
    job2.setPriority(JobPriority.HIGH);
    scheduler.update();
    
    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(0,    info1.mapDeficit);
    assertEquals(0,    info1.reduceDeficit);
    assertEquals(1.33, info1.mapFairShare, 0.1);
    assertEquals(1.33, info1.reduceFairShare, 0.1);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(10,   info2.neededMaps);
    assertEquals(10,   info2.neededReduces);
    assertEquals(0,    info2.mapDeficit);
    assertEquals(0,    info2.reduceDeficit);
    assertEquals(2.66, info2.mapFairShare, 0.1);
    assertEquals(2.66, info2.reduceFairShare, 0.1);
    
    // Advance time and check deficits
    advanceTime(100);
    assertEquals(133,  info1.mapDeficit, 1.0);
    assertEquals(133,  info1.reduceDeficit, 1.0);
    assertEquals(266,  info2.mapDeficit, 1.0);
    assertEquals(266,  info2.reduceDeficit, 1.0);
    
    // Assign tasks and check that all slots are filled with j1, then j2
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000003_0 on tt2");
  }
  
  /**
   * This test starts by submitting three large jobs:
   * - job1 in the default pool, at time 0
   * - job2 in poolA, with an allocation of 1 map / 2 reduces, at time 200
   * - job3 in poolB, with an allocation of 2 maps / 1 reduce, at time 200
   * 
   * After this, we sleep 100ms, until time 300. At this point, job1 has the
   * highest map deficit, job3 the second, and job2 the third. This is because
   * job3 has more maps in its min share than job2, but job1 has been around
   * a long time at the beginning. The reduce deficits are similar, except job2
   * comes before job3 because it had a higher reduce minimum share.
   * 
   * Finally, assign tasks to all slots. The maps should be assigned in the
   * order job3, job2, job1 because 3 and 2 both have guaranteed slots and 3
   * has a higher deficit. The reduces should be assigned as job2, job3, job1.
   */
  public void testLargeJobsWithPools() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 1 map, 2 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>1</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("</pool>");
    // Give pool B a minimum of 2 maps, 1 reduce
    out.println("<pool name=\"poolB\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>1</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(0,    info1.mapDeficit);
    assertEquals(0,    info1.reduceDeficit);
    assertEquals(4.0,  info1.mapFairShare);
    assertEquals(4.0,  info1.reduceFairShare);
    
    // Advance time 200ms and submit jobs 2 and 3
    advanceTime(200);
    assertEquals(800,  info1.mapDeficit);
    assertEquals(800,  info1.reduceDeficit);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    JobInfo info2 = scheduler.infos.get(job2);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    JobInfo info3 = scheduler.infos.get(job3);
    
    // Check that minimum and fair shares have been allocated
    assertEquals(0,    info1.minMaps);
    assertEquals(0,    info1.minReduces);
    assertEquals(1.0,  info1.mapFairShare);
    assertEquals(1.0,  info1.reduceFairShare);
    assertEquals(1,    info2.minMaps);
    assertEquals(2,    info2.minReduces);
    assertEquals(1.0,  info2.mapFairShare);
    assertEquals(2.0,  info2.reduceFairShare);
    assertEquals(2,    info3.minMaps);
    assertEquals(1,    info3.minReduces);
    assertEquals(2.0,  info3.mapFairShare);
    assertEquals(1.0,  info3.reduceFairShare);
    
    // Advance time 100ms and check deficits
    advanceTime(100);
    assertEquals(900,  info1.mapDeficit);
    assertEquals(900,  info1.reduceDeficit);
    assertEquals(100,  info2.mapDeficit);
    assertEquals(200,  info2.reduceDeficit);
    assertEquals(200,  info3.mapDeficit);
    assertEquals(100,  info3.reduceDeficit);
    
    // Assign tasks and check that slots are first given to needy jobs
    checkAssignment("tt1", "attempt_test_0003_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000000_0 on tt2");
  }

  /**
   * This test starts by submitting three large jobs:
   * - job1 in the default pool, at time 0
   * - job2 in poolA, with an allocation of 2 maps / 2 reduces, at time 200
   * - job3 in poolA, with an allocation of 2 maps / 2 reduces, at time 300
   * 
   * After this, we sleep 100ms, until time 400. At this point, job1 has the
   * highest deficit, job2 the second, and job3 the third. The first two tasks
   * should be assigned to job2 and job3 since they are in a pool with an
   * allocation guarantee, but the next two slots should be assigned to job 3
   * because the pool will no longer be needy.
   */
  public void testLargeJobsWithExcessCapacity() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 2 maps, 2 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(0,    info1.mapDeficit);
    assertEquals(0,    info1.reduceDeficit);
    assertEquals(4.0,  info1.mapFairShare);
    assertEquals(4.0,  info1.reduceFairShare);
    
    // Advance time 200ms and submit job 2
    advanceTime(200);
    assertEquals(800,  info1.mapDeficit);
    assertEquals(800,  info1.reduceDeficit);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    JobInfo info2 = scheduler.infos.get(job2);
    
    // Check that minimum and fair shares have been allocated
    assertEquals(0,    info1.minMaps);
    assertEquals(0,    info1.minReduces);
    assertEquals(2.0,  info1.mapFairShare);
    assertEquals(2.0,  info1.reduceFairShare);
    assertEquals(2,    info2.minMaps);
    assertEquals(2,    info2.minReduces);
    assertEquals(2.0,  info2.mapFairShare);
    assertEquals(2.0,  info2.reduceFairShare);
    
    // Advance time 100ms and submit job 3
    advanceTime(100);
    assertEquals(1000, info1.mapDeficit);
    assertEquals(1000, info1.reduceDeficit);
    assertEquals(200,  info2.mapDeficit);
    assertEquals(200,  info2.reduceDeficit);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    JobInfo info3 = scheduler.infos.get(job3);
    
    // Check that minimum and fair shares have been allocated
    assertEquals(0,    info1.minMaps);
    assertEquals(0,    info1.minReduces);
    assertEquals(2,    info1.mapFairShare, 0.1);
    assertEquals(2,    info1.reduceFairShare, 0.1);
    assertEquals(1,    info2.minMaps);
    assertEquals(1,    info2.minReduces);
    assertEquals(1,    info2.mapFairShare, 0.1);
    assertEquals(1,    info2.reduceFairShare, 0.1);
    assertEquals(1,    info3.minMaps);
    assertEquals(1,    info3.minReduces);
    assertEquals(1,    info3.mapFairShare, 0.1);
    assertEquals(1,    info3.reduceFairShare, 0.1);
    
    // Advance time 100ms and check deficits
    advanceTime(100);
    assertEquals(1200, info1.mapDeficit, 1.0);
    assertEquals(1200, info1.reduceDeficit, 1.0);
    assertEquals(300,  info2.mapDeficit, 1.0);
    assertEquals(300,  info2.reduceDeficit, 1.0);
    assertEquals(100,  info3.mapDeficit, 1.0);
    assertEquals(100,  info3.reduceDeficit, 1.0);
    
    // Assign tasks and check that slots are first given to needy jobs, but
    // that job 1 gets two tasks after due to having a larger deficit.
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
  }
  
  /**
   * This test starts by submitting two jobs at time 0:
   * - job1 in the default pool
   * - job2, with 1 map and 1 reduce, in poolA, which has an alloc of 4
   *   maps and 4 reduces
   * 
   * When we assign the slots, job2 should only get 1 of each type of task.
   * 
   * The fair share for job 2 should be 2.0 however, because even though it is
   * running only one task, it accumulates deficit in case it will have failures
   * or need speculative tasks later. (TODO: This may not be a good policy.)
   */
  public void testSmallJobInLargePool() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 4 maps, 4 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>4</minMaps>");
    out.println("<minReduces>4</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 1, "poolA");
    JobInfo info2 = scheduler.infos.get(job2);
    
    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(0,    info1.mapDeficit);
    assertEquals(0,    info1.reduceDeficit);
    assertEquals(2.0,  info1.mapFairShare);
    assertEquals(2.0,  info1.reduceFairShare);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(1,    info2.neededMaps);
    assertEquals(1,    info2.neededReduces);
    assertEquals(0,    info2.mapDeficit);
    assertEquals(0,    info2.reduceDeficit);
    assertEquals(2.0,  info2.mapFairShare);
    assertEquals(2.0,  info2.reduceFairShare);
    
    // Assign tasks and check that slots are first given to needy jobs
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
  }
  
  /**
   * This test starts by submitting four jobs in the default pool. However, the
   * maxRunningJobs limit for this pool has been set to two. We should see only
   * the first two jobs get scheduled, each with half the total slots.
   */
  public void testPoolMaxJobs() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"default\">");
    out.println("<maxRunningJobs>2</maxRunningJobs>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info2 = scheduler.infos.get(job2);
    advanceTime(10);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info4 = scheduler.infos.get(job4);
    
    // Check scheduler variables
    assertEquals(2.0,  info1.mapFairShare);
    assertEquals(2.0,  info1.reduceFairShare);
    assertEquals(2.0,  info2.mapFairShare);
    assertEquals(2.0,  info2.reduceFairShare);
    assertEquals(0.0,  info3.mapFairShare);
    assertEquals(0.0,  info3.reduceFairShare);
    assertEquals(0.0,  info4.mapFairShare);
    assertEquals(0.0,  info4.reduceFairShare);
    
    // Assign tasks and check that slots are first to jobs 1 and 2
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
  }

  /**
   * This test starts by submitting two jobs by user "user1" to the default
   * pool, and two jobs by "user2". We set user1's job limit to 1. We should
   * see one job from user1 and two from user2. 
   */
  public void testUserMaxJobs() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>1</maxRunningJobs>");
    out.println("</user>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    job1.getJobConf().set("user.name", "user1");
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    job2.getJobConf().set("user.name", "user1");
    JobInfo info2 = scheduler.infos.get(job2);
    advanceTime(10);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);
    job3.getJobConf().set("user.name", "user2");
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10);
    job4.getJobConf().set("user.name", "user2");
    JobInfo info4 = scheduler.infos.get(job4);
    
    // Check scheduler variables
    assertEquals(1.33,  info1.mapFairShare, 0.1);
    assertEquals(1.33,  info1.reduceFairShare, 0.1);
    assertEquals(0.0,   info2.mapFairShare);
    assertEquals(0.0,   info2.reduceFairShare);
    assertEquals(1.33,  info3.mapFairShare, 0.1);
    assertEquals(1.33,  info3.reduceFairShare, 0.1);
    assertEquals(1.33,  info4.mapFairShare, 0.1);
    assertEquals(1.33,  info4.reduceFairShare, 0.1);
    
    // Assign tasks and check that slots are first to jobs 1 and 3
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0003_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000001_0 on tt2");
  }
  
  /**
   * Test a combination of pool job limits and user job limits, the latter
   * specified through both the userMaxJobsDefaults (for some users) and
   * user-specific &lt;user&gt; elements in the allocations file. 
   */
  public void testComplexJobLimits() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"poolA\">");
    out.println("<maxRunningJobs>1</maxRunningJobs>");
    out.println("</pool>");
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>1</maxRunningJobs>");
    out.println("</user>");
    out.println("<user name=\"user2\">");
    out.println("<maxRunningJobs>10</maxRunningJobs>");
    out.println("</user>");
    out.println("<userMaxJobsDefault>2</userMaxJobsDefault>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    
    // Two jobs for user1; only one should get to run
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    job1.getJobConf().set("user.name", "user1");
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    job2.getJobConf().set("user.name", "user1");
    JobInfo info2 = scheduler.infos.get(job2);
    advanceTime(10);
    
    // Three jobs for user2; all should get to run
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);
    job3.getJobConf().set("user.name", "user2");
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10);
    job4.getJobConf().set("user.name", "user2");
    JobInfo info4 = scheduler.infos.get(job4);
    advanceTime(10);
    JobInProgress job5 = submitJob(JobStatus.RUNNING, 10, 10);
    job5.getJobConf().set("user.name", "user2");
    JobInfo info5 = scheduler.infos.get(job5);
    advanceTime(10);
    
    // Three jobs for user3; only two should get to run
    JobInProgress job6 = submitJob(JobStatus.RUNNING, 10, 10);
    job6.getJobConf().set("user.name", "user3");
    JobInfo info6 = scheduler.infos.get(job6);
    advanceTime(10);
    JobInProgress job7 = submitJob(JobStatus.RUNNING, 10, 10);
    job7.getJobConf().set("user.name", "user3");
    JobInfo info7 = scheduler.infos.get(job7);
    advanceTime(10);
    JobInProgress job8 = submitJob(JobStatus.RUNNING, 10, 10);
    job8.getJobConf().set("user.name", "user3");
    JobInfo info8 = scheduler.infos.get(job8);
    advanceTime(10);
    
    // Two jobs for user4, in poolA; only one should get to run
    JobInProgress job9 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    job9.getJobConf().set("user.name", "user4");
    JobInfo info9 = scheduler.infos.get(job9);
    advanceTime(10);
    JobInProgress job10 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    job10.getJobConf().set("user.name", "user4");
    JobInfo info10 = scheduler.infos.get(job10);
    advanceTime(10);
    
    // Check scheduler variables. The jobs in poolA should get half
    // the total share, while those in the default pool should get
    // the other half. This works out to 2 slots each for the jobs
    // in poolA and 1/3 each for the jobs in the default pool because
    // there are 2 runnable jobs in poolA and 6 jobs in the default pool.
    assertEquals(0.33,   info1.mapFairShare, 0.1);
    assertEquals(0.33,   info1.reduceFairShare, 0.1);
    assertEquals(0.0,    info2.mapFairShare);
    assertEquals(0.0,    info2.reduceFairShare);
    assertEquals(0.33,   info3.mapFairShare, 0.1);
    assertEquals(0.33,   info3.reduceFairShare, 0.1);
    assertEquals(0.33,   info4.mapFairShare, 0.1);
    assertEquals(0.33,   info4.reduceFairShare, 0.1);
    assertEquals(0.33,   info5.mapFairShare, 0.1);
    assertEquals(0.33,   info5.reduceFairShare, 0.1);
    assertEquals(0.33,   info6.mapFairShare, 0.1);
    assertEquals(0.33,   info6.reduceFairShare, 0.1);
    assertEquals(0.33,   info7.mapFairShare, 0.1);
    assertEquals(0.33,   info7.reduceFairShare, 0.1);
    assertEquals(0.0,    info8.mapFairShare);
    assertEquals(0.0,    info8.reduceFairShare);
    assertEquals(2.0,    info9.mapFairShare, 0.1);
    assertEquals(2.0,    info9.reduceFairShare, 0.1);
    assertEquals(0.0,    info10.mapFairShare);
    assertEquals(0.0,    info10.reduceFairShare);
  }
  
  public void testSizeBasedWeight() throws Exception {
    scheduler.sizeBasedWeight = true;
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 20, 1);
    assertTrue(scheduler.infos.get(job2).mapFairShare >
               scheduler.infos.get(job1).mapFairShare);
    assertTrue(scheduler.infos.get(job1).reduceFairShare >
               scheduler.infos.get(job2).reduceFairShare);
  }
  
  public void testWaitForMapsBeforeLaunchingReduces() {
    // We have set waitForMapsBeforeLaunchingReduces to false by default in
    // this class, so this should return true
    assertTrue(scheduler.enoughMapsFinishedToRunReduces(0, 100));
    
    // However, if we set waitForMapsBeforeLaunchingReduces to true, we should
    // now no longer be able to assign reduces until 5 have finished
    scheduler.waitForMapsBeforeLaunchingReduces = true;
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(0, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(1, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(2, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(3, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(4, 100));
    assertTrue(scheduler.enoughMapsFinishedToRunReduces(5, 100));
    assertTrue(scheduler.enoughMapsFinishedToRunReduces(6, 100));
    
    // Also test some jobs that have very few maps, in which case we will
    // wait for at least 1 map to finish
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(0, 5));
    assertTrue(scheduler.enoughMapsFinishedToRunReduces(1, 5));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(0, 1));
    assertTrue(scheduler.enoughMapsFinishedToRunReduces(1, 1));
  }

  /**
   * This test submits jobs in three pools: poolA, which has a weight
   * of 2.0; poolB, which has a weight of 0.5; and the default pool, which
   * should have a weight of 1.0. It then checks that the map and reduce
   * fair shares are given out accordingly. We then submit a second job to
   * pool B and check that each gets half of the pool (weight of 0.25).
   */
  public void testPoolWeights() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"poolA\">");
    out.println("<weight>2.0</weight>");
    out.println("</pool>");
    out.println("<pool name=\"poolB\">");
    out.println("<weight>0.5</weight>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    JobInfo info2 = scheduler.infos.get(job2);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    
    assertEquals(1.14,  info1.mapFairShare, 0.01);
    assertEquals(1.14,  info1.reduceFairShare, 0.01);
    assertEquals(2.28,  info2.mapFairShare, 0.01);
    assertEquals(2.28,  info2.reduceFairShare, 0.01);
    assertEquals(0.57,  info3.mapFairShare, 0.01);
    assertEquals(0.57,  info3.reduceFairShare, 0.01);
    
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    JobInfo info4 = scheduler.infos.get(job4);
    advanceTime(10);
    
    assertEquals(1.14,  info1.mapFairShare, 0.01);
    assertEquals(1.14,  info1.reduceFairShare, 0.01);
    assertEquals(2.28,  info2.mapFairShare, 0.01);
    assertEquals(2.28,  info2.reduceFairShare, 0.01);
    assertEquals(0.28,  info3.mapFairShare, 0.01);
    assertEquals(0.28,  info3.reduceFairShare, 0.01);
    assertEquals(0.28,  info4.mapFairShare, 0.01);
    assertEquals(0.28,  info4.reduceFairShare, 0.01);
  }

  /**
   * This test submits jobs in two pools, poolA and poolB. None of the
   * jobs in poolA have maps, but this should not affect their reduce
   * share.
   */
  public void testPoolWeightsWhenNoMaps() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"poolA\">");
    out.println("<weight>2.0</weight>");
    out.println("</pool>");
    out.println("<pool name=\"poolB\">");
    out.println("<weight>1.0</weight>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 0, 10, "poolA");
    JobInfo info1 = scheduler.infos.get(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 0, 10, "poolA");
    JobInfo info2 = scheduler.infos.get(job2);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    
    assertEquals(0,     info1.mapWeight, 0.01);
    assertEquals(1.0,   info1.reduceWeight, 0.01);
    assertEquals(0,     info2.mapWeight, 0.01);
    assertEquals(1.0,   info2.reduceWeight, 0.01);
    assertEquals(1.0,   info3.mapWeight, 0.01);
    assertEquals(1.0,   info3.reduceWeight, 0.01);
    
    assertEquals(0,     info1.mapFairShare, 0.01);
    assertEquals(1.33,  info1.reduceFairShare, 0.01);
    assertEquals(0,     info2.mapFairShare, 0.01);
    assertEquals(1.33,  info2.reduceFairShare, 0.01);
    assertEquals(4,     info3.mapFairShare, 0.01);
    assertEquals(1.33,  info3.reduceFairShare, 0.01);
  }

  /**
   * Tests that max-running-tasks per node are set by assigning load
   * equally accross the cluster in CapBasedLoadManager.
   */
  public void testCapBasedLoadManager() {
    CapBasedLoadManager loadMgr = new CapBasedLoadManager();
    // Arguments to getCap: totalRunnableTasks, nodeCap, totalSlots
    // Desired behavior: return ceil(nodeCap * min(1, runnableTasks/totalSlots))
    assertEquals(1, loadMgr.getCap(1, 1, 100));
    assertEquals(1, loadMgr.getCap(1, 2, 100));
    assertEquals(1, loadMgr.getCap(1, 10, 100));
    assertEquals(1, loadMgr.getCap(200, 1, 100));
    assertEquals(1, loadMgr.getCap(1, 5, 100));
    assertEquals(3, loadMgr.getCap(50, 5, 100));
    assertEquals(5, loadMgr.getCap(100, 5, 100));
    assertEquals(5, loadMgr.getCap(200, 5, 100));
  }

  /**
   * This test starts by launching a job in the default pool that takes
   * all the slots in the cluster. We then submit a job in a pool with
   * min share of 2 maps and 1 reduce task. After the min share preemption
   * timeout, this job should be allowed to preempt tasks. 
   */
  public void testMinSharePreemption() throws Exception {
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>1</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    
    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    
    // Ten seconds later, check that job 2 is not able to preempt tasks.
    advanceTime(10000);
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));
    
    // Advance time by 49 more seconds, putting us at 59s after the
    // submission of job 2. It should still not be able to preempt.
    advanceTime(49000);
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));
    
    // Advance time by 2 seconds, putting us at 61s after the submission
    // of job 2. It should now be able to preempt 2 maps and 1 reduce.
    advanceTime(2000);
    assertEquals(2, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(1, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));

    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(2, scheduler.runningTasks(job1, TaskType.MAP));
    assertEquals(3, scheduler.runningTasks(job1, TaskType.REDUCE));
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  /**
   * This test starts by launching a job in the default pool that takes
   * all the slots in the cluster. We then submit a job in a pool with
   * min share of 3 maps and 3 reduce tasks, but which only actually
   * needs 1 map and 2 reduces. We check that this job does not prempt
   * more than this many tasks despite its min share being higher. 
   */
  public void testMinSharePreemptionWithSmallJob() throws Exception {
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>3</minMaps>");
    out.println("<minReduces>3</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    
    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 2, "poolA");
    
    // Advance time by 59 seconds and check that no preemption occurs.
    advanceTime(59000);
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));
    
    // Advance time by 2 seconds, putting us at 61s after the submission
    // of job 2. Job 2 should now preempt 1 map and 2 reduces.
    advanceTime(2000);
    assertEquals(1, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(2, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));

    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(3, scheduler.runningTasks(job1, TaskType.MAP));
    assertEquals(2, scheduler.runningTasks(job1, TaskType.REDUCE));
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  /**
   * This test runs on a 4-node (8-slot) cluster to allow 3 jobs with fair
   * shares greater than 2 slots to coexist (which makes the half-fair-share 
   * of each job more than 1 so that fair share preemption can kick in). 
   * 
   * The test first launches job 1, which takes 6 map slots and 6 reduce slots. 
   * We then submit job 2, which takes 2 slots of each type. Finally, we submit 
   * a third job, job 3, which gets no slots. At this point the fair share
   * of each job will be 8/3 ~= 2.7 slots. Job 1 will be above its fair share,
   * job 2 will be below it but at half fair share, and job 3 will
   * be below half fair share. Therefore job 3 should be allowed to
   * preempt a task (after a timeout) but jobs 1 and 2 shouldn't. 
   */
  public void testFairSharePreemption() throws Exception {
    // Create a bigger cluster than normal (4 tasktrackers instead of 2)
    setUpCluster(4);
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file with a fair share preemtion timeout of 1 minute
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<fairSharePreemptionTimeout>60</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit jobs 1 and 2. We advance time by 100 between each task tracker
    // assignment stage to ensure that the tasks from job1 on tt3 are the ones
    // that are deterministically preempted first (being the latest launched
    // tasks in an over-allocated job).
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 6, 6);
    advanceTime(100); // Makes job 1 deterministically launch before job 2
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    advanceTime(100);
    checkAssignment("tt3", "attempt_test_0001_m_000004_0 on tt3");
    checkAssignment("tt3", "attempt_test_0001_m_000005_0 on tt3");
    checkAssignment("tt3", "attempt_test_0001_r_000004_0 on tt3");
    checkAssignment("tt3", "attempt_test_0001_r_000005_0 on tt3");
    advanceTime(100);
    checkAssignment("tt4", "attempt_test_0002_m_000000_0 on tt4");
    checkAssignment("tt4", "attempt_test_0002_m_000001_0 on tt4");
    checkAssignment("tt4", "attempt_test_0002_r_000000_0 on tt4");
    checkAssignment("tt4", "attempt_test_0002_r_000001_0 on tt4");
    
    // Submit job 3.
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);
    
    // Check that after 59 seconds, neither job can preempt
    advanceTime(59000);
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job3, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job3, TaskType.REDUCE,
        clock.getTime()));
    
    // Wait 2 more seconds, so that job 3 has now been in the system for 61s.
    // Now job 3 should be able to preempt 1 task but job 2 shouldn't.
    advanceTime(2000);
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));
    assertEquals(2, scheduler.tasksToPreempt(job3, TaskType.MAP,
        clock.getTime()));
    assertEquals(2, scheduler.tasksToPreempt(job3, TaskType.REDUCE,
        clock.getTime()));
    
    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(4, scheduler.runningTasks(job1, TaskType.MAP));
    assertEquals(4, scheduler.runningTasks(job1, TaskType.REDUCE));
    checkAssignment("tt3", "attempt_test_0003_m_000000_0 on tt3");
    checkAssignment("tt3", "attempt_test_0003_m_000001_0 on tt3");
    checkAssignment("tt3", "attempt_test_0003_r_000000_0 on tt3");
    checkAssignment("tt3", "attempt_test_0003_r_000001_0 on tt3");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    assertNull(scheduler.assignTasks(tracker("tt3")));
    assertNull(scheduler.assignTasks(tracker("tt4")));
  }
  
  /**
   * This test submits a job that takes all 4 slots, and then a second
   * job that has both a min share of 2 slots with a 60s timeout and a
   * fair share timeout of 60s. After 60 seconds, this job will be starved
   * of both min share (2 slots of each type) and fair share (2 slots of each
   * type), and we test that it does not kill more than 2 tasks of each type
   * in total.
   */
  public void testMinAndFairSharePreemption() throws Exception {
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("<fairSharePreemptionTimeout>60</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    
    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    
    // Ten seconds later, check that job 2 is not able to preempt tasks.
    advanceTime(10000);
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));
    
    // Advance time by 49 more seconds, putting us at 59s after the
    // submission of job 2. It should still not be able to preempt.
    advanceTime(49000);
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));
    
    // Advance time by 2 seconds, putting us at 61s after the submission
    // of job 2. It should now be able to preempt 2 maps and 1 reduce.
    advanceTime(2000);
    assertEquals(2, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(2, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));

    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(2, scheduler.runningTasks(job1, TaskType.MAP));
    assertEquals(2, scheduler.runningTasks(job1, TaskType.REDUCE));
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }
  
  /**
   * This is a copy of testMinAndFairSharePreemption that turns preemption
   * off and verifies that no tasks get killed.
   */
  public void testNoPreemptionIfDisabled() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("<fairSharePreemptionTimeout>60</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    
    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    
    // Advance time by 61s, putting us past the preemption timeout,
    // and check that no tasks get preempted.
    advanceTime(61000);
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(4, scheduler.runningTasks(job1, TaskType.MAP));
    assertEquals(4, scheduler.runningTasks(job1, TaskType.REDUCE));
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  /**
   * This is a copy of testMinAndFairSharePreemption that turns preemption
   * on but also turns on mapred.fairscheduler.preemption.only.log (the
   * "dry run" parameter for testing out preemption) and verifies that no
   * tasks get killed.
   */
  public void testNoPreemptionIfOnlyLogging() throws Exception {
    // Turn on preemption, but for logging only
    scheduler.preemptionEnabled = true;
    scheduler.onlyLogPreemption = true;
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("<fairSharePreemptionTimeout>60</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    
    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    
    // Advance time by 61s, putting us past the preemption timeout,
    // and check that no tasks get preempted.
    advanceTime(61000);
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(4, scheduler.runningTasks(job1, TaskType.MAP));
    assertEquals(4, scheduler.runningTasks(job1, TaskType.REDUCE));
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }
  
  private void advanceTime(long time) {
    clock.advance(time);
    scheduler.update();
  }

  protected TaskTracker tracker(String taskTrackerName) {
    return taskTrackerManager.getTaskTracker(taskTrackerName);
  }
  
  protected void checkAssignment(String taskTrackerName,
      String expectedTaskString) throws IOException {
    List<Task> tasks = scheduler.assignTasks(tracker(taskTrackerName));
    assertNotNull(expectedTaskString, tasks);
    assertEquals(expectedTaskString, 1, tasks.size());
    assertEquals(expectedTaskString, tasks.get(0).toString());
  }
  
}
