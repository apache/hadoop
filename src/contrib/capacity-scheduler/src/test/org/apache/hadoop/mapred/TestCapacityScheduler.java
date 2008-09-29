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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.mapred.CapacityTaskScheduler;
import org.apache.hadoop.conf.Configuration;

public class TestCapacityScheduler extends TestCase {
  
  private static int jobCounter;
  
  static class FakeJobInProgress extends JobInProgress {
    
    private FakeTaskTrackerManager taskTrackerManager;
    private int mapTaskCtr;
    private int redTaskCtr;
    private Set<TaskInProgress> mapTips = 
      new HashSet<TaskInProgress>();
    private Set<TaskInProgress> reduceTips = 
      new HashSet<TaskInProgress>();
    
    public FakeJobInProgress(JobID jId, JobConf jobConf,
        FakeTaskTrackerManager taskTrackerManager, String user) 
    throws IOException {
      super(jId, jobConf);
      this.taskTrackerManager = taskTrackerManager;
      this.startTime = System.currentTimeMillis();
      this.status = new JobStatus();
      this.status.setRunState(JobStatus.PREP);
      if (null == jobConf.getQueueName()) {
        this.profile = new JobProfile(user, jId, 
            null, null, null);
      }
      else {
        this.profile = new JobProfile(user, jId, 
            null, null, null, jobConf.getQueueName());
      }
      mapTaskCtr = 0;
      redTaskCtr = 0;
    }
    
    @Override
    public synchronized void initTasks() throws IOException {
      getStatus().setRunState(JobStatus.RUNNING);
    }

    @Override
    public Task obtainNewMapTask(final TaskTrackerStatus tts, int clusterSize,
        int ignored) throws IOException {
      if (runningMapTasks == numMapTasks) return null;
      TaskAttemptID attemptId = getTaskAttemptID(true);
      Task task = new MapTask("", attemptId, 0, "", new BytesWritable()) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      taskTrackerManager.startTask(tts.getTrackerName(), task);
      runningMapTasks++;
      // create a fake TIP and keep track of it
      mapTips.add(new FakeTaskInProgress(getJobID(), 
          getJobConf(), task, true, this));
      return task;
    }
    
    @Override
    public Task obtainNewReduceTask(final TaskTrackerStatus tts,
        int clusterSize, int ignored) throws IOException {
      if (runningReduceTasks == numReduceTasks) return null;
      TaskAttemptID attemptId = getTaskAttemptID(false);
      Task task = new ReduceTask("", attemptId, 0, 10) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      taskTrackerManager.startTask(tts.getTrackerName(), task);
      runningReduceTasks++;
      // create a fake TIP and keep track of it
      reduceTips.add(new FakeTaskInProgress(getJobID(), 
          getJobConf(), task, false, this));
      return task;
    }
    
    public void mapTaskFinished() {
      runningMapTasks--;
      finishedMapTasks++;
    }
    
    public void reduceTaskFinished() {
      runningReduceTasks--;
      finishedReduceTasks++;
    }
    
    private TaskAttemptID getTaskAttemptID(boolean isMap) {
      JobID jobId = getJobID();
      return new TaskAttemptID(jobId.getJtIdentifier(),
          jobId.getId(), isMap, (isMap)?++mapTaskCtr: ++redTaskCtr, 0);
    }
    
    @Override
    Set<TaskInProgress> getNonLocalRunningMaps() {
      return (Set<TaskInProgress>)mapTips;
    }
    @Override
    Set<TaskInProgress> getRunningReduces() {
      return (Set<TaskInProgress>)reduceTips;
    }
  }
  
  static class FakeTaskInProgress extends TaskInProgress {
    private boolean isMap;
    private FakeJobInProgress fakeJob;
    private TreeMap<TaskAttemptID, String> activeTasks;
    private TaskStatus taskStatus;
    FakeTaskInProgress(JobID jId, JobConf jobConf, Task t, 
        boolean isMap, FakeJobInProgress job) {
      super(jId, "", new JobClient.RawSplit(), null, jobConf, job, 0);
      this.isMap = isMap;
      this.fakeJob = job;
      activeTasks = new TreeMap<TaskAttemptID, String>();
      activeTasks.put(t.getTaskID(), "tt");
      // create a fake status for a task that is running for a bit
      this.taskStatus = TaskStatus.createTaskStatus(isMap);
      taskStatus.setProgress(0.5f);
      taskStatus.setRunState(TaskStatus.State.RUNNING);
    }
    
    @Override
    TreeMap<TaskAttemptID, String> getActiveTasks() {
      return activeTasks;
    }
    @Override
    public TaskStatus getTaskStatus(TaskAttemptID taskid) {
      // return a status for a task that has run a bit
      return taskStatus;
    }
    @Override
    boolean killTask(TaskAttemptID taskId, boolean shouldFail) {
      if (isMap) {
        fakeJob.mapTaskFinished();
      }
      else {
        fakeJob.reduceTaskFinished();
      }
      return true;
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
    int maxReduceTasksPerTracker = 1;
    List<JobInProgressListener> listeners =
      new ArrayList<JobInProgressListener>();
    FakeQueueManager qm = new FakeQueueManager();
    
    private Map<String, TaskTrackerStatus> trackers =
      new HashMap<String, TaskTrackerStatus>();
    private Map<String, TaskStatus> taskStatuses = 
      new HashMap<String, TaskStatus>();

    public FakeTaskTrackerManager() {
      trackers.put("tt1", new TaskTrackerStatus("tt1", "tt1.host", 1,
          new ArrayList<TaskStatus>(), 0,
          maxMapTasksPerTracker, maxReduceTasksPerTracker));
      trackers.put("tt2", new TaskTrackerStatus("tt2", "tt2.host", 2,
          new ArrayList<TaskStatus>(), 0,
          maxMapTasksPerTracker, maxReduceTasksPerTracker));
    }
    
    public void addTaskTracker(String ttName) {
      trackers.put(ttName, new TaskTrackerStatus(ttName, ttName + ".host", 1,
          new ArrayList<TaskStatus>(), 0,
          maxMapTasksPerTracker, maxReduceTasksPerTracker));
    }
    
    public ClusterStatus getClusterStatus() {
      int numTrackers = trackers.size();
      return new ClusterStatus(numTrackers, maps, reduces,
          numTrackers * maxMapTasksPerTracker,
          numTrackers * maxReduceTasksPerTracker,
          JobTracker.State.RUNNING);
    }

    public int getNumberOfUniqueHosts() {
      return 0;
    }

    public int getNextHeartbeatInterval() {
      return MRConstants.HEARTBEAT_INTERVAL_MIN;
    }
    
    public Collection<TaskTrackerStatus> taskTrackers() {
      return trackers.values();
    }


    public void addJobInProgressListener(JobInProgressListener listener) {
      listeners.add(listener);
    }

    public void removeJobInProgressListener(JobInProgressListener listener) {
      listeners.remove(listener);
    }
    
    public void submitJob(JobInProgress job) {
      for (JobInProgressListener listener : listeners) {
        listener.jobAdded(job);
      }
    }
    
    public TaskTrackerStatus getTaskTracker(String trackerID) {
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
      trackers.get(taskTrackerName).getTaskReports().add(status);
    }
    
    public void finishTask(String taskTrackerName, String tipId, 
        FakeJobInProgress j) {
      TaskStatus status = taskStatuses.get(tipId);
      if (status.getIsMap()) {
        maps--;
        j.mapTaskFinished();
      } else {
        reduces--;
        j.reduceTaskFinished();
      }
      status.setRunState(TaskStatus.State.SUCCEEDED);
    }
    
    void addQueues(String[] arr) {
      Set<String> queues = new HashSet<String>();
      for (String s: arr) {
        queues.add(s);
      }
      qm.setQueues(queues);
    }
    
    public QueueManager getQueueManager() {
      return qm;
    }
  }
  
  // represents a fake queue configuration info
  static class FakeQueueInfo {
    String queueName;
    float gc;
    int reclaimTimeLimit;
    boolean supportsPrio;
    int ulMin;

    public FakeQueueInfo(String queueName, float gc,
        int reclaimTimeLimit, boolean supportsPrio, int ulMin) {
      this.queueName = queueName;
      this.gc = gc;
      this.reclaimTimeLimit = reclaimTimeLimit;
      this.supportsPrio = supportsPrio;
      this.ulMin = ulMin;
    }
  }
  
  static class FakeResourceManagerConf extends CapacitySchedulerConf {
  
    // map of queue names to queue info
    private Map<String, FakeQueueInfo> queueMap = 
      new LinkedHashMap<String, FakeQueueInfo>();
    String firstQueue;
    
    void setFakeQueues(List<FakeQueueInfo> queues) {
      for (FakeQueueInfo q: queues) {
        queueMap.put(q.queueName, q);
      }
      firstQueue = new String(queues.get(0).queueName);
    }
    
    public synchronized Set<String> getQueues() {
      return queueMap.keySet();
    }
    
    /*public synchronized String getFirstQueue() {
      return firstQueue;
    }*/
    
    public float getGuaranteedCapacity(String queue) {
      return queueMap.get(queue).gc;
    }
    
    public int getReclaimTimeLimit(String queue) {
      return queueMap.get(queue).reclaimTimeLimit;
    }
    
    public int getMinimumUserLimitPercent(String queue) {
      return queueMap.get(queue).ulMin;
    }
    
    public boolean isPrioritySupported(String queue) {
      return queueMap.get(queue).supportsPrio;
    }
  }

  protected class FakeClock extends CapacityTaskScheduler.Clock {
    private long time = 0;
    
    public void advance(long millis) {
      time += millis;
    }

    @Override
    long getTime() {
      return time;
    }
  }

  
  protected JobConf conf;
  protected CapacityTaskScheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;
  private FakeResourceManagerConf resConf;
  private FakeClock clock;

  @Override
  protected void setUp() throws Exception {
    jobCounter = 0;
    taskTrackerManager = new FakeTaskTrackerManager();
    clock = new FakeClock();
    scheduler = new CapacityTaskScheduler(clock);
    scheduler.setTaskTrackerManager(taskTrackerManager);

    conf = new JobConf();
    // set interval to a large number so thread doesn't interfere with us
    conf.setLong("mapred.capacity-scheduler.reclaimCapacity.interval", 500);
    scheduler.setConf(conf);
    
  }
  
  @Override
  protected void tearDown() throws Exception {
    if (scheduler != null) {
      scheduler.terminate();
    }
  }
  
  private FakeJobInProgress submitJob(int state, int maps, int reduces, 
      String queue, String user) throws IOException {
    JobConf jobConf = new JobConf(conf);
    jobConf.setNumMapTasks(maps);
    jobConf.setNumReduceTasks(reduces);
    if (queue != null)
      jobConf.setQueueName(queue);
    FakeJobInProgress job = new FakeJobInProgress(
        new JobID("test", ++jobCounter), jobConf, taskTrackerManager, user);
    job.getStatus().setRunState(state);
    taskTrackerManager.submitJob(job);
    return job;
  }
  
  /*protected void submitJobs(int number, int state, int maps, int reduces)
    throws IOException {
    for (int i = 0; i < number; i++) {
      submitJob(state, maps, reduces);
    }
  }*/
  
  // basic tests, should be able to submit to queues
  public void testSubmitToQueues() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    resConf = new FakeResourceManagerConf();
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, 5000, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, 5000, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job with no queue specified. It should be accepted
    // and given to the default queue. 
    JobInProgress j = submitJob(JobStatus.PREP, 10, 10, null, "u1");
    // when we ask for a task, we should get one, from the job submitted
    Task t;
    t = checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    // submit another job, to a different queue
    j = submitJob(JobStatus.PREP, 10, 10, "q2", "u1");
    // now when we get a task, it should be from the second job
    t = checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
  }
  
  public void testGetJobs() throws Exception {
    // need only one queue
    String[] qs = { "default" };
    taskTrackerManager.addQueues(qs);
    resConf = new FakeResourceManagerConf();
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, 300, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    
    // submit a job
    JobInProgress j1 = submitJob(JobStatus.PREP, 10, 10, "default", "u1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    
    // submit another job
    JobInProgress j2 = submitJob(JobStatus.PREP, 10, 10, "default", "u1");
    
    Collection<JobInProgress> jobs = scheduler.getJobs("default");
    assertEquals(2, jobs.size());
    Iterator<JobInProgress> iter = jobs.iterator();
    assertEquals(j1, iter.next());
    assertEquals(j2, iter.next());
    
    assertEquals(1, scheduler.jobQueuesManager.
                        getRunningJobQueue("default").size());
    assertEquals(1, scheduler.jobQueuesManager.
                        getWaitingJobQueue("default").size());
  }
  
  // test capacity transfer
  public void testCapacityTransfer() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    resConf = new FakeResourceManagerConf();
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, 5000, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, 5000, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    submitJob(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the GC for maps is 2. Since we're the only user, 
    // we should get a task 
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    // I should get another map task. 
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    // Now we're at full capacity for maps. If I ask for another map task,
    // I should get a map task from the default queue's capacity. 
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    // and another
    checkAssignment("tt2", "attempt_test_0001_m_000004_0 on tt2");
  }

  // test user limits
  public void testUserLimits() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    resConf = new FakeResourceManagerConf();
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, 5000, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, 5000, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    submitJob(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the GC for maps is 2. Since we're the only user, 
    // we should get a task 
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    // Submit another job, from a different user
    submitJob(JobStatus.PREP, 10, 10, "q2", "u2");
    // Now if I ask for a map task, it should come from the second job 
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    // Now we're at full capacity for maps. If I ask for another map task,
    // I should get a map task from the default queue's capacity. 
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    // and another
    checkAssignment("tt2", "attempt_test_0002_m_000002_0 on tt2");
  }

  // test user limits when a 2nd job is submitted much after first job 
  public void testUserLimits2() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    resConf = new FakeResourceManagerConf();
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, 5000, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, 5000, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    submitJob(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the GC for maps is 2. Since we're the only user, 
    // we should get a task 
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    // since we're the only job, we get another map
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    // Submit another job, from a different user
    submitJob(JobStatus.PREP, 10, 10, "q2", "u2");
    // Now if I ask for a map task, it should come from the second job 
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    // and another
    checkAssignment("tt2", "attempt_test_0002_m_000002_0 on tt2");
  }

  // test user limits when a 2nd job is submitted much after first job 
  // and we need to wait for first job's task to complete
  public void testUserLimits3() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    resConf = new FakeResourceManagerConf();
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, 5000, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, 5000, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    FakeJobInProgress j1 = submitJob(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the GC for maps is 2. Since we're the only user, 
    // we should get a task 
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    // since we're the only job, we get another map
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    // we get two more maps from 'default queue'
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000004_0 on tt2");
    // Submit another job, from a different user
    FakeJobInProgress j2 = submitJob(JobStatus.PREP, 10, 10, "q2", "u2");
    // one of the task finishes
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", j1);
    // Now if I ask for a map task, it should come from the second job 
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    // another task from job1 finishes, another new task to job2
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000002_0", j1);
    checkAssignment("tt1", "attempt_test_0002_m_000002_0 on tt1");
    // now we have equal number of tasks from each job. Whichever job's
    // task finishes, that job gets a new task
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000003_0", j1);
    checkAssignment("tt2", "attempt_test_0001_m_000005_0 on tt2");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000001_0", j2);
    checkAssignment("tt1", "attempt_test_0002_m_000003_0 on tt1");
  }

  // test user limits with many users, more slots
  public void testUserLimits4() throws Exception {
    // set up one queue, with 10 slots
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    resConf = new FakeResourceManagerConf();
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, 10000, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    // add some more TTs 
    taskTrackerManager.addTaskTracker("tt3");
    taskTrackerManager.addTaskTracker("tt4");
    taskTrackerManager.addTaskTracker("tt5");

    // u1 submits job
    FakeJobInProgress j1 = submitJob(JobStatus.PREP, 10, 10, null, "u1");
    // it gets the first 5 slots
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000004_0 on tt2");
    checkAssignment("tt3", "attempt_test_0001_m_000005_0 on tt3");
    // u2 submits job with 4 slots
    FakeJobInProgress j2 = submitJob(JobStatus.PREP, 4, 4, null, "u2");
    // u2 should get next 4 slots
    checkAssignment("tt3", "attempt_test_0002_m_000001_0 on tt3");
    checkAssignment("tt4", "attempt_test_0002_m_000002_0 on tt4");
    checkAssignment("tt4", "attempt_test_0002_m_000003_0 on tt4");
    checkAssignment("tt5", "attempt_test_0002_m_000004_0 on tt5");
    // last slot should go to u1, since u2 has no more tasks
    checkAssignment("tt5", "attempt_test_0001_m_000006_0 on tt5");
    // u1 finishes a task
    taskTrackerManager.finishTask("tt5", "attempt_test_0001_m_000006_0", j1);
    // u1 submits a few more jobs 
    submitJob(JobStatus.PREP, 10, 10, null, "u1");
    submitJob(JobStatus.PREP, 10, 10, null, "u1");
    submitJob(JobStatus.PREP, 10, 10, null, "u1");
    // u2 also submits a job
    submitJob(JobStatus.PREP, 10, 10, null, "u2");
    // now u3 submits a job
    submitJob(JobStatus.PREP, 2, 2, null, "u3");
    // next slot should go to u3, even though u2 has an earlier job, since
    // user limits have changed and u1/u2 are over limits
    checkAssignment("tt5", "attempt_test_0007_m_000001_0 on tt5");
    // some other task finishes and u3 gets it
    taskTrackerManager.finishTask("tt5", "attempt_test_0002_m_000004_0", j1);
    checkAssignment("tt5", "attempt_test_0007_m_000002_0 on tt5");
    // now, u2 finishes a task
    taskTrackerManager.finishTask("tt4", "attempt_test_0002_m_000002_0", j1);
    // next slot will go to u1, since u3 has nothing to run and u1's job is 
    // first in the queue
    checkAssignment("tt4", "attempt_test_0001_m_000007_0 on tt4");
  }

  // test code to reclaim capacity
  public void testReclaimCapacity() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2", "q3"};
    taskTrackerManager.addQueues(qs);
    resConf = new FakeResourceManagerConf();
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, 1000000, true, 25));
    queues.add(new FakeQueueInfo("q2", 25.0f, 1000000, true, 25));
    queues.add(new FakeQueueInfo("q3", 25.0f, 1000000, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // set up a situation where q2 is under capacity, and default & q3
    // are at/over capacity
    FakeJobInProgress j1 = submitJob(JobStatus.PREP, 10, 10, null, "u1");
    FakeJobInProgress j2 = submitJob(JobStatus.PREP, 10, 10, "q3", "u1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    // now submit a job to q2
    FakeJobInProgress j3 = submitJob(JobStatus.PREP, 10, 10, "q2", "u1");
    // update our structures
    scheduler.updateQSIInfo();
    // get scheduler to notice that q2 needs to reclaim
    scheduler.reclaimCapacity();
    // our queue reclaim time is 1000s, heartbeat interval is 5 sec, so 
    // we start reclaiming when 15 secs are left. 
    clock.advance(400000);
    scheduler.reclaimCapacity();
    // no tasks should have been killed yet
    assertEquals(j1.runningMapTasks, 3);
    assertEquals(j2.runningMapTasks, 1);
    clock.advance(200000);
    scheduler.reclaimCapacity();
    // task from j1 will be killed
    assertEquals(j1.runningMapTasks, 2);
    assertEquals(j2.runningMapTasks, 1);
    
  }

  // test code to reclaim multiple capacity 
  public void testReclaimCapacity2() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2", "q3", "q4"};
    taskTrackerManager.addQueues(qs);
    resConf = new FakeResourceManagerConf();
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, 1000000, true, 25));
    queues.add(new FakeQueueInfo("q2", 20.0f, 1000000, true, 25));
    queues.add(new FakeQueueInfo("q3", 20.0f, 1000000, true, 25));
    queues.add(new FakeQueueInfo("q4", 10.0f, 1000000, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    
    // add some more TTs so our total map capacity is 10
    taskTrackerManager.addTaskTracker("tt3");
    taskTrackerManager.addTaskTracker("tt4");
    taskTrackerManager.addTaskTracker("tt5");

    // q2 has nothing running, default is under cap, q3 and q4 are over cap
    FakeJobInProgress j1 = submitJob(JobStatus.PREP, 2, 2, null, "u1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    FakeJobInProgress j2 = submitJob(JobStatus.PREP, 10, 10, "q3", "u1");
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    FakeJobInProgress j3 = submitJob(JobStatus.PREP, 10, 10, "q4", "u1");
    checkAssignment("tt2", "attempt_test_0003_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt3", "attempt_test_0002_m_000002_0 on tt3");
    checkAssignment("tt3", "attempt_test_0002_m_000003_0 on tt3");
    checkAssignment("tt4", "attempt_test_0003_m_000002_0 on tt4");
    checkAssignment("tt4", "attempt_test_0002_m_000004_0 on tt4");
    checkAssignment("tt5", "attempt_test_0002_m_000005_0 on tt5");
    checkAssignment("tt5", "attempt_test_0003_m_000003_0 on tt5");
    // at this point, q3 is running 5 tasks (with a cap of 2), q4 is
    // running 3 tasks (with a cap of 1). 
    // If we submit a job to 'default', we need to get 3 slots back. 
    FakeJobInProgress j4 = submitJob(JobStatus.PREP, 10, 10, null, "u1");
    // update our structures
    scheduler.updateQSIInfo();
    // get scheduler to notice that q2 needs to reclaim
    scheduler.reclaimCapacity();
    // our queue reclaim time is 1000s, heartbeat interval is 5 sec, so 
    // we start reclaiming when 15 secs are left. 
    clock.advance(400000);
    scheduler.reclaimCapacity();
    // nothing should have happened
    assertEquals(j2.runningMapTasks, 5);
    assertEquals(j3.runningMapTasks, 3);
    // 3 tasks to kill, 5 running over cap. q3 should give up 3*3/5 = 2 slots.
    // q4 should give up 2*3/5 = 1 slot. 
    clock.advance(200000);
    scheduler.reclaimCapacity();
    assertEquals(j2.runningMapTasks, 3);
    assertEquals(j3.runningMapTasks, 2);
    
  }

  // test code to reclaim capacity in steps
  public void testReclaimCapacityInSteps() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    resConf = new FakeResourceManagerConf();
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, 1000000, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, 1000000, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // set up a situation where q2 is under capacity, and default is
    // at/over capacity
    FakeJobInProgress j1 = submitJob(JobStatus.PREP, 10, 10, null, "u1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000004_0 on tt2");
    // now submit a job to q2
    FakeJobInProgress j2 = submitJob(JobStatus.PREP, 1, 1, "q2", "u1");
    // update our structures
    scheduler.updateQSIInfo();
    // get scheduler to notice that q2 needs to reclaim
    scheduler.reclaimCapacity();
    // our queue reclaim time is 1000s, heartbeat interval is 5 sec, so 
    // we start reclaiming when 15 secs are left. 
    clock.advance(400000);
    // submit another job to q2 which causes more capacity to be reclaimed
    j2 = submitJob(JobStatus.PREP, 10, 10, "q2", "u2");
    // update our structures
    scheduler.updateQSIInfo();
    clock.advance(200000);
    scheduler.reclaimCapacity();
    // one task from j1 will be killed
    assertEquals(j1.runningMapTasks, 3);
    clock.advance(300000);
    scheduler.reclaimCapacity();
    // timer for 2nd job hasn't fired, so nothing killed
    assertEquals(j1.runningMapTasks, 3);
    clock.advance(400000);
    scheduler.reclaimCapacity();
    // one task from j1 will be killed
    assertEquals(j1.runningMapTasks, 2);
    
  }

  protected TaskTrackerStatus tracker(String taskTrackerName) {
    return taskTrackerManager.getTaskTracker(taskTrackerName);
  }
  
  protected Task checkAssignment(String taskTrackerName,
      String expectedTaskString) throws IOException {
    List<Task> tasks = scheduler.assignTasks(tracker(taskTrackerName));
    assertNotNull(expectedTaskString, tasks);
    assertEquals(expectedTaskString, 1, tasks.size());
    assertEquals(expectedTaskString, tasks.get(0).toString());
    return tasks.get(0);
  }
  
}
