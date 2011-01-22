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
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.FairScheduler.JobInfo;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapred.UtilsForTests.FakeClock;

public class TestFairScheduler extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/streaming/test/data")).getAbsolutePath();
  final static String ALLOC_FILE = new File(TEST_DIR, 
      "test-pools").getAbsolutePath();
  
  private static final String POOL_PROPERTY = "pool";
  
  private static int jobCounter;
  private static int taskCounter;
  
  static class FakeJobInProgress extends JobInProgress {
    
    private FakeTaskTrackerManager taskTrackerManager;
    
    public FakeJobInProgress(JobConf jobConf,
        FakeTaskTrackerManager taskTrackerManager, 
        JobTracker jt) throws IOException {
      super(new JobID("test", ++jobCounter), jobConf, jt);
      this.taskTrackerManager = taskTrackerManager;
      this.startTime = System.currentTimeMillis();
      this.status = new JobStatus();
      this.status.setRunState(JobStatus.PREP);
    }
    
    @Override
    public synchronized void initTasks() throws IOException {
      // do nothing
    }

    @Override
    public Task obtainNewMapTask(final TaskTrackerStatus tts, int clusterSize,
        int ignored) throws IOException {
      TaskAttemptID attemptId = getTaskAttemptID(true);
      Task task = new MapTask("", attemptId, 0, new JobSplit.TaskSplitIndex(),
          1) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      taskTrackerManager.startTask(tts.getTrackerName(), task);
      runningMapTasks++;
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
      taskTrackerManager.startTask(tts.getTrackerName(), task);
      runningReduceTasks++;
      return task;
    }
    
    private TaskAttemptID getTaskAttemptID(boolean isMap) {
      JobID jobId = getJobID();
      return new TaskAttemptID(jobId.getJtIdentifier(),
          jobId.getId(), isMap, ++taskCounter, 0);
    }
  }
  
  static class FakeTaskTrackerManager implements TaskTrackerManager {
    int maps = 0;
    int reduces = 0;
    int maxMapTasksPerTracker = 2;
    int maxReduceTasksPerTracker = 2;
    List<JobInProgressListener> listeners =
      new ArrayList<JobInProgressListener>();
    
    private Map<String, TaskTracker> trackers =
      new HashMap<String, TaskTracker>();
    private Map<String, TaskStatus> taskStatuses = 
      new HashMap<String, TaskStatus>();

    public FakeTaskTrackerManager() {
      TaskTracker tt1 = new TaskTracker("tt1");
      tt1.setStatus(new TaskTrackerStatus("tt1", "tt1.host", 1,
                                          new ArrayList<TaskStatus>(), 0,
                                          maxMapTasksPerTracker, 
                                          maxReduceTasksPerTracker));
      trackers.put("tt1", tt1);
      
      TaskTracker tt2 = new TaskTracker("tt2");
      tt2.setStatus(new TaskTrackerStatus("tt2", "tt2.host", 2,
                                          new ArrayList<TaskStatus>(), 0,
                                          maxMapTasksPerTracker, 
                                          maxReduceTasksPerTracker));
      trackers.put("tt2", tt2);

    }
    
    @Override
    public ClusterStatus getClusterStatus() {
      int numTrackers = trackers.size();
      return new ClusterStatus(numTrackers, maps, reduces,
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
    
    public void startTask(String taskTrackerName, final Task t) {
      if (t.isMapTask()) {
        maps++;
      } else {
        reduces++;
      }
      TaskStatus status = new TaskStatus() {
        @Override
        public boolean getIsMap() {
          return t.isMapTask();
        }
      };
      taskStatuses.put(t.getTaskID().toString(), status);
      status.setRunState(TaskStatus.State.RUNNING);
      trackers.get(taskTrackerName).getStatus().getTaskReports().add(status);
    }
    
    public void finishTask(String taskTrackerName, String tipId) {
      TaskStatus status = taskStatuses.get(tipId);
      if (status.getIsMap()) {
        maps--;
      } else {
        reduces--;
      }
      status.setRunState(TaskStatus.State.SUCCEEDED);
    }
  }
  
  protected JobConf conf;
  protected FairScheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;
  private FakeClock clock;

  @Override
  protected void setUp() throws Exception {
    jobCounter = 0;
    taskCounter = 0;
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    // Create an empty pools file (so we can add/remove pools later)
    FileWriter fileWriter = new FileWriter(ALLOC_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    fileWriter.write("<allocations />\n");
    fileWriter.close();
    conf = new JobConf();
    conf.set("mapred.fairscheduler.allocation.file", ALLOC_FILE);
    conf.set("mapred.fairscheduler.poolnameproperty", POOL_PROPERTY);
    taskTrackerManager = new FakeTaskTrackerManager();
    clock = new FakeClock();
    scheduler = new FairScheduler(clock, false);
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
    JobInProgress job = new FakeJobInProgress(jobConf, taskTrackerManager,
        UtilsForTests.getJobTracker());
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
    // Set default limit of jobs per user to 5
    out.println("<userMaxJobsDefault>5</userMaxJobsDefault>");
    // Give user1 a limit of 10 jobs
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>10</maxRunningJobs>");
    out.println("</user>");
    out.println("</allocations>"); 
    out.close();
    
    PoolManager poolManager = scheduler.getPoolManager();
    poolManager.reloadAllocs();
    
    assertEquals(5, poolManager.getPools().size()); // 4 in file + default pool
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
    assertEquals(Integer.MAX_VALUE, poolManager.getPoolMaxJobs("poolA"));
    assertEquals(3, poolManager.getPoolMaxJobs("poolD"));
    assertEquals(10, poolManager.getUserMaxJobs("user1"));
    assertEquals(5, poolManager.getUserMaxJobs("user2"));
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
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000003_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000004_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000005_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000006_0 on tt2");
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
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000003_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000004_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000005_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000006_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000007_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000008_0 on tt2");
    
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
    // the JobInProgress's getTasks(TaskType.MAP)/getTasks(TaskType.REDUCE) and 
    // think they finished.
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000002_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000003_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000004_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000005_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000006_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_r_000007_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_r_000008_0");
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
    checkAssignment("tt1", "attempt_test_0002_m_000009_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000010_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000011_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000012_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000013_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000014_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000015_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000016_0 on tt2");

    // Finish up the tasks and advance time again, but give job 2 only 50ms.
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000009_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000010_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_r_000011_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_r_000012_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000013_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000014_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000015_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000016_0");
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
    checkAssignment("tt1", "attempt_test_0002_m_000017_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000018_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000019_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000020_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000021_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000022_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000023_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000024_0 on tt2");
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
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000003_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000004_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000005_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000006_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000007_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000008_0 on tt2");
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
    checkAssignment("tt1", "attempt_test_0003_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000003_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000004_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000005_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000006_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000007_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000008_0 on tt2");
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
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000003_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_r_000004_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000005_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000006_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000007_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000008_0 on tt2");
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
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000003_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000004_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000005_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000006_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000007_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000008_0 on tt2");
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
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000003_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000004_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0002_m_000005_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000006_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000007_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000008_0 on tt2");
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
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000003_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000004_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0003_m_000005_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_m_000006_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000007_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000008_0 on tt2");
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
