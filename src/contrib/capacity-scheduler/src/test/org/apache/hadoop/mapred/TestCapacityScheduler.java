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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

public class TestCapacityScheduler extends TestCase {

  static final Log LOG =
      LogFactory.getLog(org.apache.hadoop.mapred.TestCapacityScheduler.class);

  private static int jobCounter;

  /**
   * Test class that removes the asynchronous nature of job initialization.
   * 
   * The run method is a dummy which just waits for completion. It is
   * expected that test code calls the main method, initializeJobs, directly
   * to trigger initialization.
   */
  class ControlledJobInitializer extends 
                              JobInitializationPoller.JobInitializationThread {
    
    boolean stopRunning;
    
    public ControlledJobInitializer(JobInitializationPoller p) {
      p.super();
    }
    
    @Override
    public void run() {
      while (!stopRunning) {
        try {
          synchronized(this) {
            this.wait();  
          }
        } catch (InterruptedException ie) {
          break;
        }
      }
    }
    
    void stopRunning() {
      stopRunning = true;
    }
  }
  
  /**
   * Test class that removes the asynchronous nature of job initialization.
   * 
   * The run method is a dummy which just waits for completion. It is
   * expected that test code calls the main method, selectJobsToInitialize,
   * directly to trigger initialization.
   * 
   * The class also creates the test worker thread objects of type 
   * ControlledJobInitializer instead of the objects of the actual class
   */
  class ControlledInitializationPoller extends JobInitializationPoller {
    
    private boolean stopRunning;
    private ArrayList<ControlledJobInitializer> workers;
    
    public ControlledInitializationPoller(JobQueuesManager mgr,
                                          CapacitySchedulerConf rmConf,
                                          Set<String> queues,
                                          TaskTrackerManager ttm) {
      super(mgr, rmConf, queues, ttm);
    }
    
    @Override
    public void run() {
      // don't do anything here.
      while (!stopRunning) {
        try {
          synchronized (this) {
            this.wait();
          }
        } catch (InterruptedException ie) {
          break;
        }
      }
    }
    
    @Override
    JobInitializationThread createJobInitializationThread() {
      ControlledJobInitializer t = new ControlledJobInitializer(this);
      if (workers == null) {
        workers = new ArrayList<ControlledJobInitializer>();
      }
      workers.add(t);
      return t;
    }

    @Override
    void selectJobsToInitialize() {
      super.cleanUpInitializedJobsList();
      super.selectJobsToInitialize();
      for (ControlledJobInitializer t : workers) {
        t.initializeJobs();
      }
    }
    
    void stopRunning() {
      stopRunning = true;
      for (ControlledJobInitializer t : workers) {
        t.stopRunning();
        t.interrupt();
      }
    }
  }

  private ControlledInitializationPoller controlledInitializationPoller;
  /*
   * Fake job in progress object used for testing the schedulers scheduling
   * decisions. The JobInProgress objects returns out FakeTaskInProgress
   * objects when assignTasks is called. If speculative maps and reduces
   * are configured then JobInProgress returns exactly one Speculative
   * map and reduce task.
   */
  static class FakeJobInProgress extends JobInProgress {
    
    protected FakeTaskTrackerManager taskTrackerManager;
    private int mapTaskCtr;
    private int redTaskCtr;
    private Set<TaskInProgress> mapTips = 
      new HashSet<TaskInProgress>();
    private Set<TaskInProgress> reduceTips = 
      new HashSet<TaskInProgress>();
    private int speculativeMapTaskCounter = 0;
    private int speculativeReduceTaskCounter = 0;
    public FakeJobInProgress(JobID jId, JobConf jobConf,
        FakeTaskTrackerManager taskTrackerManager, String user) {
      super(jId, jobConf);
      this.taskTrackerManager = taskTrackerManager;
      this.startTime = System.currentTimeMillis();
      this.status = new JobStatus(jId, 0f, 0f, JobStatus.PREP);
      this.status.setJobPriority(JobPriority.NORMAL);
      this.status.setStartTime(startTime);
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
      boolean areAllMapsRunning = (mapTaskCtr == numMapTasks);
      if (areAllMapsRunning){
        if(!getJobConf().getMapSpeculativeExecution() || 
            speculativeMapTasks > 0) {
          return null;
        }
      }
      TaskAttemptID attemptId = getTaskAttemptID(true, areAllMapsRunning);
      Task task = new MapTask("", attemptId, 0, "", new BytesWritable(), 
                              super.numSlotsPerMap, getJobConf().getUser()) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      taskTrackerManager.startTask(tts.getTrackerName(), task);
      runningMapTasks++;
      // create a fake TIP and keep track of it
      FakeTaskInProgress mapTip = new FakeTaskInProgress(getJobID(), 
          getJobConf(), task, true, this);
      mapTip.taskStatus.setRunState(TaskStatus.State.RUNNING);
      if(areAllMapsRunning) {
        speculativeMapTasks++;
        //you have scheduled a speculative map. Now set all tips in the
        //map tips not to have speculative task.
        for(TaskInProgress t : mapTips) {
          if (t instanceof FakeTaskInProgress) {
            FakeTaskInProgress mt = (FakeTaskInProgress) t;
            mt.hasSpeculativeMap = false;
          }
        }
      } else {
        //add only non-speculative tips.
        mapTips.add(mapTip);
        //add the tips to the JobInProgress TIPS
        maps = mapTips.toArray(new TaskInProgress[mapTips.size()]);
      }
      return task;
    }

    @Override
    public Task obtainNewReduceTask(final TaskTrackerStatus tts,
        int clusterSize, int ignored) throws IOException {
      boolean areAllReducesRunning = (redTaskCtr == numReduceTasks);
      if (areAllReducesRunning){
        if(!getJobConf().getReduceSpeculativeExecution() || 
            speculativeReduceTasks > 0) {
          return null;
        }
      }
      TaskAttemptID attemptId = getTaskAttemptID(false, areAllReducesRunning);
      Task task = new ReduceTask("", attemptId, 0, 10, super.numSlotsPerReduce, getJobConf().getUser()) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      taskTrackerManager.startTask(tts.getTrackerName(), task);
      runningReduceTasks++;
      // create a fake TIP and keep track of it
      FakeTaskInProgress reduceTip = new FakeTaskInProgress(getJobID(), 
          getJobConf(), task, false, this);
      reduceTip.taskStatus.setRunState(TaskStatus.State.RUNNING);
      if(areAllReducesRunning) {
        speculativeReduceTasks++;
        //you have scheduled a speculative map. Now set all tips in the
        //map tips not to have speculative task.
        for(TaskInProgress t : reduceTips) {
          if (t instanceof FakeTaskInProgress) {
            FakeTaskInProgress rt = (FakeTaskInProgress) t;
            rt.hasSpeculativeReduce = false;
          }
        }
      } else {
        //add only non-speculative tips.
        reduceTips.add(reduceTip);
        //add the tips to the JobInProgress TIPS
        reduces = reduceTips.toArray(new TaskInProgress[reduceTips.size()]);
      }
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
    
    private TaskAttemptID getTaskAttemptID(boolean isMap, boolean isSpeculative) {
      JobID jobId = getJobID();
      if (!isSpeculative) {
        return new TaskAttemptID(jobId.getJtIdentifier(), jobId.getId(), isMap,
            (isMap) ? ++mapTaskCtr : ++redTaskCtr, 0);
      } else  {
        return new TaskAttemptID(jobId.getJtIdentifier(), jobId.getId(), isMap,
            (isMap) ? mapTaskCtr : redTaskCtr, 1);
      }
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
  
  static class FakeFailingJobInProgress extends FakeJobInProgress {

    public FakeFailingJobInProgress(JobID id, JobConf jobConf,
        FakeTaskTrackerManager taskTrackerManager, String user) {
      super(id, jobConf, taskTrackerManager, user);
    }
    
    @Override
    public synchronized void initTasks() throws IOException {
      throw new IOException("Failed Initalization");
    }
    
    @Override
    synchronized void fail() {
      this.status.setRunState(JobStatus.FAILED);
    }
  }
 
  static class FakeTaskInProgress extends TaskInProgress {
    private boolean isMap;
    private FakeJobInProgress fakeJob;
    private TreeMap<TaskAttemptID, String> activeTasks;
    private TaskStatus taskStatus;
    boolean hasSpeculativeMap;
    boolean hasSpeculativeReduce;
    
    FakeTaskInProgress(JobID jId, JobConf jobConf, Task t, 
        boolean isMap, FakeJobInProgress job) {
      super(jId, "", new JobClient.RawSplit(), null, jobConf, job, 0, 1);
      this.isMap = isMap;
      this.fakeJob = job;
      activeTasks = new TreeMap<TaskAttemptID, String>();
      activeTasks.put(t.getTaskID(), "tt");
      // create a fake status for a task that is running for a bit
      this.taskStatus = TaskStatus.createTaskStatus(isMap);
      taskStatus.setProgress(0.5f);
      taskStatus.setRunState(TaskStatus.State.RUNNING);
      if (jobConf.getMapSpeculativeExecution()) {
        //resetting of the hasSpeculativeMap is done
        //when speculative map is scheduled by the job.
        hasSpeculativeMap = true;
      } 
      if (jobConf.getReduceSpeculativeExecution()) {
        //resetting of the hasSpeculativeReduce is done
        //when speculative reduce is scheduled by the job.
        hasSpeculativeReduce = true;
      }
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
    
    @Override
    /*
     *hasSpeculativeMap and hasSpeculativeReduce is reset by FakeJobInProgress
     *after the speculative tip has been scheduled.
     */
    boolean hasSpeculativeTask(long currentTime, double averageProgress) {
      if(isMap && hasSpeculativeMap) {
        return fakeJob.getJobConf().getMapSpeculativeExecution();
      } 
      if (!isMap && hasSpeculativeReduce) {
        return fakeJob.getJobConf().getReduceSpeculativeExecution();
      }
      return false;
    }
    
    @Override
    public boolean isRunning() {
      return !activeTasks.isEmpty();
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
    
    private Map<String, TaskTracker> trackers =
      new HashMap<String, TaskTracker>();
    private Map<String, TaskStatus> taskStatuses = 
      new HashMap<String, TaskStatus>();
    private Map<JobID, JobInProgress> jobs =
        new HashMap<JobID, JobInProgress>();

    public FakeTaskTrackerManager() {
      this(2, 2, 1);
    }

    public FakeTaskTrackerManager(int numTaskTrackers,
        int maxMapTasksPerTracker, int maxReduceTasksPerTracker) {
      this.maxMapTasksPerTracker = maxMapTasksPerTracker;
      this.maxReduceTasksPerTracker = maxReduceTasksPerTracker;
      for (int i = 1; i < numTaskTrackers + 1; i++) {
        String ttName = "tt" + i;
        TaskTracker tt = new TaskTracker(ttName);
        tt.setStatus(new TaskTrackerStatus(ttName, ttName + ".host", i,
                                           new ArrayList<TaskStatus>(), 0, 
                                           maxMapTasksPerTracker,
                                           maxReduceTasksPerTracker));
        trackers.put(ttName, tt);
      }
    }
    
    public void addTaskTracker(String ttName) {
      TaskTracker tt = new TaskTracker(ttName);
      tt.setStatus(new TaskTrackerStatus(ttName, ttName + ".host", 1,
                                         new ArrayList<TaskStatus>(), 0,
                                         maxMapTasksPerTracker, 
                                         maxReduceTasksPerTracker));
      trackers.put(ttName, tt);
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

    @Override
    public void killJob(JobID jobid) throws IOException {
      JobInProgress job = jobs.get(jobid);
      finalizeJob(job, JobStatus.KILLED);
      job.kill();
    }

    @Override
    public synchronized void failJob(JobInProgress job) {
      finalizeJob(job, JobStatus.FAILED);
      job.fail();
    }
    
    public void initJob(JobInProgress jip) {
      try {
        JobStatus oldStatus = (JobStatus)jip.getStatus().clone();
        jip.initTasks();
        JobStatus newStatus = (JobStatus)jip.getStatus().clone();
        JobStatusChangeEvent event = new JobStatusChangeEvent(jip, 
            EventType.RUN_STATE_CHANGED, oldStatus, newStatus);
        for (JobInProgressListener listener : listeners) {
          listener.jobUpdated(event);
        }
      } catch (Exception ioe) {
        failJob(jip);
      }
    }
    
    public void removeJob(JobID jobid) {
      jobs.remove(jobid);
    }
    
    @Override
    public JobInProgress getJob(JobID jobid) {
      return jobs.get(jobid);
    }

    Collection<JobInProgress> getJobs() {
      return jobs.values();
    }

    public Collection<TaskTrackerStatus> taskTrackers() {
      List<TaskTrackerStatus> statuses = new ArrayList<TaskTrackerStatus>();
      for (TaskTracker tt : trackers.values()) {
        statuses.add(tt.getStatus());
      }
      return statuses;
    }


    public void addJobInProgressListener(JobInProgressListener listener) {
      listeners.add(listener);
    }

    public void removeJobInProgressListener(JobInProgressListener listener) {
      listeners.remove(listener);
    }
    
    public void submitJob(JobInProgress job) throws IOException {
      jobs.put(job.getJobID(), job);
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
        public TaskAttemptID getTaskID() {
          return t.getTaskID();
        }

        @Override
        public boolean getIsMap() {
          return t.isMapTask();
        }
        
        @Override
        public int getNumSlots() {
          return t.getNumSlotsRequired();
        }
      };
      taskStatuses.put(t.getTaskID().toString(), status);
      status.setRunState(TaskStatus.State.RUNNING);
      trackers.get(taskTrackerName).getStatus().getTaskReports().add(status);
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
    
    void finalizeJob(FakeJobInProgress fjob) {
      finalizeJob(fjob, JobStatus.SUCCEEDED);
    }

    void finalizeJob(JobInProgress fjob, int state) {
      // take a snapshot of the status before changing it
      JobStatus oldStatus = (JobStatus)fjob.getStatus().clone();
      fjob.getStatus().setRunState(state);
      JobStatus newStatus = (JobStatus)fjob.getStatus().clone();
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent (fjob, EventType.RUN_STATE_CHANGED, oldStatus, 
                                  newStatus);
      for (JobInProgressListener listener : listeners) {
        listener.jobUpdated(event);
      }
    }
    
    public void setPriority(FakeJobInProgress fjob, JobPriority priority) {
      // take a snapshot of the status before changing it
      JobStatus oldStatus = (JobStatus)fjob.getStatus().clone();
      fjob.setPriority(priority);
      JobStatus newStatus = (JobStatus)fjob.getStatus().clone();
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent (fjob, EventType.PRIORITY_CHANGED, oldStatus, 
                                  newStatus);
      for (JobInProgressListener listener : listeners) {
        listener.jobUpdated(event);
      }
    }
    
    public void setStartTime(FakeJobInProgress fjob, long start) {
      // take a snapshot of the status before changing it
      JobStatus oldStatus = (JobStatus)fjob.getStatus().clone();
      
      fjob.startTime = start; // change the start time of the job
      fjob.status.setStartTime(start); // change the start time of the jobstatus
      
      JobStatus newStatus = (JobStatus)fjob.getStatus().clone();
      
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent (fjob, EventType.START_TIME_CHANGED, oldStatus,
                                  newStatus);
      for (JobInProgressListener listener : listeners) {
        listener.jobUpdated(event);
      }
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
    float capacity;
    boolean supportsPrio;
    int ulMin;

    public FakeQueueInfo(String queueName, float capacity, boolean supportsPrio, int ulMin) {
      this.queueName = queueName;
      this.capacity = capacity;
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
    
    public float getCapacity(String queue) {
      if(queueMap.get(queue).capacity == -1) {
        return super.getCapacity(queue);
      }
      return queueMap.get(queue).capacity;
    }
    
    public int getMinimumUserLimitPercent(String queue) {
      return queueMap.get(queue).ulMin;
    }
    
    public boolean isPrioritySupported(String queue) {
      return queueMap.get(queue).supportsPrio;
    }
    
    @Override
    public long getSleepInterval() {
      return 1;
    }
    
    @Override
    public int getMaxWorkerThreads() {
      return 1;
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
  protected void setUp() {
    setUp(2, 2, 1);
  }

  private void setUp(int numTaskTrackers, int numMapTasksPerTracker,
      int numReduceTasksPerTracker) {
    jobCounter = 0;
    taskTrackerManager =
        new FakeTaskTrackerManager(numTaskTrackers, numMapTasksPerTracker,
            numReduceTasksPerTracker);
    clock = new FakeClock();
    scheduler = new CapacityTaskScheduler(clock);
    scheduler.setAssignMultipleTasks(false);
    scheduler.setTaskTrackerManager(taskTrackerManager);

    conf = new JobConf();
    // Don't let the JobInitializationPoller come in our way.
    resConf = new FakeResourceManagerConf();
    controlledInitializationPoller = new ControlledInitializationPoller(
        scheduler.jobQueuesManager,
        resConf,
        resConf.getQueues(), taskTrackerManager);
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

  private FakeJobInProgress submitJob(int state, JobConf jobConf) throws IOException {
    FakeJobInProgress job =
        new FakeJobInProgress(new JobID("test", ++jobCounter),
            (jobConf == null ? new JobConf(conf) : jobConf), taskTrackerManager,
            jobConf.getUser());
    job.getStatus().setRunState(state);
    taskTrackerManager.submitJob(job);
    return job;
  }

  private FakeJobInProgress submitJobAndInit(int state, JobConf jobConf)
      throws IOException {
    FakeJobInProgress j = submitJob(state, jobConf);
    taskTrackerManager.initJob(j);
    return j;
  }

  private FakeJobInProgress submitJob(int state, int maps, int reduces, 
      String queue, String user) throws IOException {
    JobConf jobConf = new JobConf(conf);
    jobConf.setNumMapTasks(maps);
    jobConf.setNumReduceTasks(reduces);
    if (queue != null)
      jobConf.setQueueName(queue);
    jobConf.setUser(user);
    return submitJob(state, jobConf);
  }
  
  // Submit a job and update the listeners
  private FakeJobInProgress submitJobAndInit(int state, int maps, int reduces,
                                             String queue, String user) 
  throws IOException {
    FakeJobInProgress j = submitJob(state, maps, reduces, queue, user);
    taskTrackerManager.initJob(j);
    return j;
  }
  
  // test job run-state change
  public void testJobRunStateChange() throws IOException {
    // start the scheduler
    taskTrackerManager.addQueues(new String[] {"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 1));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    
    // submit the job
    FakeJobInProgress fjob1 = 
      submitJob(JobStatus.PREP, 1, 0, "default", "user");
    
    FakeJobInProgress fjob2 = 
      submitJob(JobStatus.PREP, 1, 0, "default", "user");
    
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
    assertEquals("Waiting queue is garbled on job init", 2, 
                 scheduler.jobQueuesManager.getWaitingJobs("default")
                          .size());
    
    // test if changing the job priority/start-time works as expected in the 
    // running queue
    testJobOrderChange(fjob1, fjob2, false);
    
    // schedule a task
    List<Task> tasks = scheduler.assignTasks(tracker("tt1"));
    
    // complete the job
    taskTrackerManager.finishTask("tt1", tasks.get(0).getTaskID().toString(), 
                                  fjob1);
    
    // mark the job as complete
    taskTrackerManager.finalizeJob(fjob1);
    
    Collection<JobInProgress> rqueue = 
      scheduler.jobQueuesManager.getRunningJobQueue("default");
    
    // check if the job is removed from the scheduler
    assertFalse("Scheduler contains completed job", 
                rqueue.contains(fjob1));
    
    // check if the running queue size is correct
    assertEquals("Job finish garbles the queue", 
                 1, rqueue.size());

  }

  /**
   * Test the max Capacity for map and reduce
   * @throws IOException
   */
  public void testMaxCapacities() throws IOException {
    this.setUp(4,1,1);
    taskTrackerManager.addQueues(new String[] {"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 25.0f, false, 1));

    resConf.setFakeQueues(queues);
    resConf.setMaxCapacity("default", 50.0f);
    scheduler.setResourceManagerConf(resConf);
    scheduler.setAssignMultipleTasks(true);
    scheduler.start();

    //submit the Job
    FakeJobInProgress fjob1 =
      submitJobAndInit(JobStatus.PREP, 4, 4, "default", "user");

    //default queue has min capacity of 1 and max capacity of 2

    //first call of assign task should give task from default queue.
    //default uses 1 map and 1 reduce slots are used
    checkMultipleAssignment(
      "tt1", "attempt_test_0001_m_000001_0 on tt1",
      "attempt_test_0001_r_000001_0 on tt1");

    //second call of assign task
    //default uses 2 map and 2 reduce slots
    checkMultipleAssignment(
      "tt2", "attempt_test_0001_m_000002_0 on tt2",
      "attempt_test_0001_r_000002_0 on tt2");


    //Now we have reached the max capacity limit for default ,
    //no further tasks would be assigned to this queue.
    checkMultipleAssignment(
      "tt3", null,
      null);
  }
  
  // test if the queue reflects the changes
  private void testJobOrderChange(FakeJobInProgress fjob1, 
                                  FakeJobInProgress fjob2, 
                                  boolean waiting) {
    String queueName = waiting ? "waiting" : "running";
    
    // check if the jobs in the queue are the right order
    JobInProgress[] jobs = getJobsInQueue(waiting);
    assertTrue(queueName + " queue doesnt contain job #1 in right order", 
                jobs[0].getJobID().equals(fjob1.getJobID()));
    assertTrue(queueName + " queue doesnt contain job #2 in right order", 
                jobs[1].getJobID().equals(fjob2.getJobID()));
    
    // I. Check the start-time change
    // Change job2 start-time and check if job2 bumps up in the queue 
    taskTrackerManager.setStartTime(fjob2, fjob1.startTime - 1);
    
    jobs = getJobsInQueue(waiting);
    assertTrue("Start time change didnt not work as expected for job #2 in "
               + queueName + " queue", 
                jobs[0].getJobID().equals(fjob2.getJobID()));
    assertTrue("Start time change didnt not work as expected for job #1 in"
               + queueName + " queue", 
                jobs[1].getJobID().equals(fjob1.getJobID()));
    
    // check if the queue is fine
    assertEquals("Start-time change garbled the " + queueName + " queue", 
                 2, jobs.length);
    
    // II. Change job priority change
    // Bump up job1's priority and make sure job1 bumps up in the queue
    taskTrackerManager.setPriority(fjob1, JobPriority.HIGH);
    
    // Check if the priority changes are reflected
    jobs = getJobsInQueue(waiting);
    assertTrue("Priority change didnt not work as expected for job #1 in "
               + queueName + " queue",  
                jobs[0].getJobID().equals(fjob1.getJobID()));
    assertTrue("Priority change didnt not work as expected for job #2 in "
               + queueName + " queue",  
                jobs[1].getJobID().equals(fjob2.getJobID()));
    
    // check if the queue is fine
    assertEquals("Priority change has garbled the " + queueName + " queue", 
                 2, jobs.length);
    
    // reset the queue state back to normal
    taskTrackerManager.setStartTime(fjob1, fjob2.startTime - 1);
    taskTrackerManager.setPriority(fjob1, JobPriority.NORMAL);
  }
  
  private JobInProgress[] getJobsInQueue(boolean waiting) {
    Collection<JobInProgress> queue = 
      waiting 
      ? scheduler.jobQueuesManager.getWaitingJobs("default")
      : scheduler.jobQueuesManager.getRunningJobQueue("default");
    return queue.toArray(new JobInProgress[0]);
  }
  
  // tests if tasks can be assinged when there are multiple jobs from a same
  // user
  public void testJobFinished() throws Exception {
    taskTrackerManager.addQueues(new String[] {"default"});
    
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit 2 jobs
    FakeJobInProgress j1 = submitJobAndInit(JobStatus.PREP, 3, 0, "default", "u1");
    FakeJobInProgress j2 = submitJobAndInit(JobStatus.PREP, 3, 0, "default", "u1");
    
    // I. Check multiple assignments with running tasks within job
    // ask for a task from first job
    Task t = checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    //  ask for another task from the first job
    t = checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    
    // complete tasks
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", j1);
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000002_0", j1);
    
    // II. Check multiple assignments with running tasks across jobs
    // ask for a task from first job
    t = checkAssignment("tt1", "attempt_test_0001_m_000003_0 on tt1");
    
    //  ask for a task from the second job
    t = checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    
    // complete tasks
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000001_0", j2);
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000003_0", j1);
    
    // III. Check multiple assignments with completed tasks across jobs
    // ask for a task from the second job
    t = checkAssignment("tt1", "attempt_test_0002_m_000002_0 on tt1");
    
    // complete task
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000002_0", j2);
    
    // IV. Check assignment with completed job
    // finish first job
    scheduler.jobCompleted(j1);
    
    // ask for another task from the second job
    // if tasks can be assigned then the structures are properly updated 
    t = checkAssignment("tt1", "attempt_test_0002_m_000003_0 on tt1");
    
    // complete task
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000003_0", j2);
  }

  /**
   * Tests whether a map and reduce task are assigned when there's
   * a single queue and multiple task assignment is enabled.
   * @throws Exception
   */
  public void testMultiTaskAssignmentInSingleQueue() throws Exception {
    try {
      setUp(1, 6, 2);
      // set up some queues
      String[] qs = {"default"};
      taskTrackerManager.addQueues(qs);
      ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
      queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
      resConf.setFakeQueues(queues);
      scheduler.setResourceManagerConf(resConf);
      scheduler.start();
      scheduler.setAssignMultipleTasks(true);

      //Submit the job with 6 maps and 2 reduces
      FakeJobInProgress j1 = submitJobAndInit(
        JobStatus.PREP, 6, 2, "default", "u1");

      List<Task> tasks = scheduler.assignTasks(tracker("tt1"));
      assertEquals(tasks.size(), 2);
      for (Task task : tasks) {
        if (task.toString().contains("_m_")) {
          LOG.info(" map task assigned " + task.toString());
          assertEquals(task.toString(), "attempt_test_0001_m_000001_0 on tt1");
        } else if (task.toString().contains("_r_")) {
          LOG.info(" reduce task assigned " + task.toString());
          assertEquals(task.toString(), "attempt_test_0001_r_000001_0 on tt1");
        } else {
          fail(" should not have come here " + task.toString());
        }
      }

      for (Task task : tasks) {
        if (task.toString().equals("attempt_test_0001_m_000001_0 on tt1")) {
          //Now finish the task
          taskTrackerManager.finishTask(
            "tt1", task.getTaskID().toString(),
            j1);
        }
      }

      tasks = scheduler.assignTasks(tracker("tt1"));
      assertEquals(tasks.size(), 2);
      for (Task task : tasks) {
        if (task.toString().contains("_m_")) {
          LOG.info(" map task assigned " + task.toString());
          assertEquals(task.toString(), "attempt_test_0001_m_000002_0 on tt1");
        } else if (task.toString().contains("_r_")) {
          LOG.info(" reduce task assigned " + task.toString());
          assertEquals(task.toString(), "attempt_test_0001_r_000002_0 on tt1");
        } else {
          fail(" should not have come here " + task.toString());
        }
      }

      //now both the reduce slots are being used , hence we should not 
      // get only 1 map task in this assignTasks call.
      tasks = scheduler.assignTasks(tracker("tt1"));
      assertEquals(tasks.size(), 1);
      for (Task task : tasks) {
        if (task.toString().contains("_m_")) {
          LOG.info(" map task assigned " + task.toString());
          assertEquals(task.toString(), "attempt_test_0001_m_000003_0 on tt1");
        } else if (task.toString().contains("_r_")) {
          LOG.info(" reduce task assigned " + task.toString());
          fail("should not give reduce task " + task.toString());
        } else {
          fail(" should not have come here " + task.toString());
        }
      }
    } finally {
      scheduler.setAssignMultipleTasks(false);
    }
  }

  public void testMultiTaskAssignmentInMultipleQueues() throws Exception {
    try {
      setUp(1, 6, 2);
      // set up some queues
      String[] qs = {"default","q1"};
      taskTrackerManager.addQueues(qs);
      ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
      queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
      queues.add(new FakeQueueInfo("q1", 50.0f, true, 25));
      resConf.setFakeQueues(queues);
      scheduler.setResourceManagerConf(resConf);
      scheduler.start();
      scheduler.setAssignMultipleTasks(true);

      //Submit the job with 6 maps and 2 reduces
      submitJobAndInit(
        JobStatus.PREP, 6, 1, "default", "u1");

      FakeJobInProgress j2 = submitJobAndInit(JobStatus.PREP,2,1,"q1","u2");

      List<Task> tasks = scheduler.assignTasks(tracker("tt1"));
      assertEquals(tasks.size(), 2);
      for (Task task : tasks) {
        if (task.toString().contains("_m_")) {
          LOG.info(" map task assigned " + task.toString());
          assertEquals(task.toString(), "attempt_test_0001_m_000001_0 on tt1");
        } else if (task.toString().contains("_r_")) {
          LOG.info(" reduce task assigned " + task.toString());
          assertEquals(task.toString(), "attempt_test_0001_r_000001_0 on tt1");
        } else {
          fail(" should not have come here " + task.toString());
        }
      }
      
      // next assignment will be for job in second queue.
      tasks = scheduler.assignTasks(tracker("tt1"));
      assertEquals(tasks.size(), 2);
      for (Task task : tasks) {
        if (task.toString().contains("_m_")) {
          LOG.info(" map task assigned " + task.toString());
          assertEquals(task.toString(), "attempt_test_0002_m_000001_0 on tt1");
        } else if (task.toString().contains("_r_")) {
          LOG.info(" reduce task assigned " + task.toString());
          assertEquals(task.toString(), "attempt_test_0002_r_000001_0 on tt1");
        } else {
          fail(" should not have come here " + task.toString());
        }
      }

      //now both the reduce slots are being used , hence we sholdnot get only 1
      //map task in this assignTasks call.
      tasks = scheduler.assignTasks(tracker("tt1"));
      assertEquals(tasks.size(), 1);
      for (Task task : tasks) {
        if (task.toString().contains("_m_")) {
          LOG.info(" map task assigned " + task.toString());
          // we get from job 2 because the queues are equal in capacity usage
          // and sorting leaves order unchanged.
          assertEquals(task.toString(), "attempt_test_0002_m_000002_0 on tt1");
        } else if (task.toString().contains("_r_")) {
          LOG.info(" reduce task assigned " + task.toString());
          fail("should not give reduce task " + task.toString());
        } else {
          fail(" should not have come here " + task.toString());
        }
      }

      tasks = scheduler.assignTasks(tracker("tt1"));
      assertEquals(tasks.size(), 1);
      for (Task task : tasks) {
        if (task.toString().contains("_m_")) {
          LOG.info(" map task assigned " + task.toString());
          assertEquals(task.toString(), "attempt_test_0001_m_000002_0 on tt1");
        } else if (task.toString().contains("_r_")) {
          LOG.info(" reduce task assigned " + task.toString());
          fail("should not give reduce task " + task.toString());
        } else {
          fail(" should not have come here " + task.toString());
        }
      }
    } finally {
      scheduler.setAssignMultipleTasks(false);
    }
  }

  // basic tests, should be able to submit to queues
  public void testSubmitToQueues() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job with no queue specified. It should be accepted
    // and given to the default queue. 
    JobInProgress j = submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    
    // when we ask for a task, we should get one, from the job submitted
    Task t;
    t = checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    // submit another job, to a different queue
    j = submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // now when we get a task, it should be from the second job
    t = checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
  }
  
  public void testGetJobs() throws Exception {
    // need only one queue
    String[] qs = { "default" };
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    HashMap<String, ArrayList<FakeJobInProgress>> subJobsList = 
      submitJobs(1, 4, "default");
   
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    
    while(mgr.getWaitingJobs("default").size() < 4){
      Thread.sleep(1);
    }
    //Raise status change events for jobs submitted.
    raiseStatusChangeEvents(mgr);
    Collection<JobInProgress> jobs = scheduler.getJobs("default");
    
    assertTrue("Number of jobs returned by scheduler is wrong" 
        ,jobs.size() == 4);
    
    assertTrue("Submitted jobs and Returned jobs are not same",
        subJobsList.get("u1").containsAll(jobs));
  }
  
  //Basic test to test capacity allocation across the queues which have no
  //capacity configured.
  
  public void testCapacityAllocationToQueues() throws Exception {
    String[] qs = {"default","q1","q2","q3","q4"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default",25.0f,true,25));
    queues.add(new FakeQueueInfo("q1",-1.0f,true,25));
    queues.add(new FakeQueueInfo("q2",-1.0f,true,25));
    queues.add(new FakeQueueInfo("q3",-1.0f,true,25));
    queues.add(new FakeQueueInfo("q4",-1.0f,true,25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start(); 
    assertEquals(18.75f, resConf.getCapacity("q1"));
    assertEquals(18.75f, resConf.getCapacity("q2"));
    assertEquals(18.75f, resConf.getCapacity("q3"));
    assertEquals(18.75f, resConf.getCapacity("q4"));
  }

  public void testCapacityAllocFailureWithLowerMaxCapacity()
    throws Exception {
    String[] qs = {"default", "q1"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 50));
    queues.add(new FakeQueueInfo("q1", -1.0f, true, 50));
    resConf.setFakeQueues(queues);
    resConf.setMaxCapacity("q1", 40.0f);
    scheduler.setResourceManagerConf(resConf);
    try {
      scheduler.start();
      fail("Scheduler start should fail ");
    } catch (IllegalStateException ise) {
      assertEquals(
        ise.getMessage(),
        " Allocated capacity of " + 50.0f + " to unconfigured queue " +
          "q1" + " is greater than maximum Capacity " + 40.0f);
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
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
   
    // submit a job to the default queue
    submitJobAndInit(JobStatus.PREP, 10, 0, "default", "u1");
    
    // submit a job to the second queue
    submitJobAndInit(JobStatus.PREP, 10, 0, "q2", "u1");
    
    // job from q2 runs first because it has some non-zero capacity.
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    verifyCapacity("0", "default");
    verifyCapacity("3", "q2");
    
    // add another tt to increase tt slots
    taskTrackerManager.addTaskTracker("tt3");
    checkAssignment("tt2", "attempt_test_0002_m_000002_0 on tt2");
    verifyCapacity("0", "default");
    verifyCapacity("5", "q2");
    
    // add another tt to increase tt slots
    taskTrackerManager.addTaskTracker("tt4");
    checkAssignment("tt3", "attempt_test_0002_m_000003_0 on tt3");
    verifyCapacity("0", "default");
    verifyCapacity("7", "q2");
    
    // add another tt to increase tt slots
    taskTrackerManager.addTaskTracker("tt5");
    // now job from default should run, as it is furthest away
    // in terms of runningMaps / capacity.
    checkAssignment("tt4", "attempt_test_0001_m_000001_0 on tt4");
    verifyCapacity("1", "default");
    verifyCapacity("9", "q2");
  }
  
  private void verifyCapacity(String expectedCapacity,
                                          String queue) throws IOException {
    String schedInfo = taskTrackerManager.getQueueManager().
                          getSchedulerInfo(queue).toString();    
    assertTrue(schedInfo.contains("Map tasks\nCapacity: " 
        + expectedCapacity + " slots"));
  }
  
  // test capacity transfer
  public void testCapacityTransfer() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
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

  /**
   * Creates a queue with max capacity  of 50%
   * submit 1 job in the queue which is high ram(2 slots) . As 2 slots are
   * given to high ram job and are reserved , no other tasks are accepted .
   *
   * @throws IOException
   */
  public void testHighMemoryBlockingWithMaxCapacity()
      throws IOException {

    taskTrackerManager = new FakeTaskTrackerManager(2, 2, 2);

    taskTrackerManager.addQueues(new String[] { "defaultXYZ" });
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("defaultXYZ", 25.0f, true, 50));
    resConf.setFakeQueues(queues);

    //defaultXYZ can go up to 2 map and 2 reduce slots
    resConf.setMaxCapacity("defaultXYZ", 50.0f);

    scheduler.setTaskTrackerManager(taskTrackerManager);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
        2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
        2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    scheduler.setAssignMultipleTasks(true);

    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("defaultXYZ");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("defaultXYZ");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);


    //high ram map from job 1 and normal reduce task from job 1
    List<Task> tasks = checkMultipleAssignment(
      "tt1", "attempt_test_0001_m_000001_0 on tt1",
      "attempt_test_0001_r_000001_0 on tt1");

    checkOccupiedSlots("defaultXYZ", TaskType.MAP, 1, 2, 200.0f,1,0);
    checkOccupiedSlots("defaultXYZ", TaskType.REDUCE, 1, 1, 100.0f,0,2);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 1 * 1024L);

    //we have reached the maximum limit for map, so no more map tasks.
    //we have used 1 reduce already and 1 more reduce slot is left for the
    //before we reach maxcapacity for reduces.
    // But current 1 slot + 2 slots for high ram reduce would
    //mean we are crossing the maxium capacity.hence nothing would be assigned
    //in this call
    checkMultipleAssignment("tt2",null,null);

    //complete the high ram job on tt1.
    for (Task task : tasks) {
      taskTrackerManager.finishTask(
        "tt1", task.getTaskID().toString(),
        job1);
    }

    //At this point we have 1 high ram map and 1 high ram reduce.
    List<Task> t2 = checkMultipleAssignment(
      "tt2", "attempt_test_0001_m_000002_0 on tt2",
      "attempt_test_0002_r_000001_0 on tt2");

    checkOccupiedSlots("defaultXYZ", TaskType.MAP, 1, 2, 200.0f,1,0);
    checkOccupiedSlots("defaultXYZ", TaskType.REDUCE, 1, 2, 200.0f,0,2);
    checkMemReservedForTasksOnTT("tt2", 2 * 1024L, 2 * 1024L);

    //complete the high ram job on tt1.
    for (Task task : t2) {
      taskTrackerManager.finishTask(
        "tt2", task.getTaskID().toString(),
        job2);
    }

    //1st map & 2nd reduce from job2
    checkMultipleAssignment(
      "tt2", "attempt_test_0002_m_000001_0 on tt2",
      "attempt_test_0002_r_000002_0 on tt2");
  }

  /**
   *   test if user limits automatically adjust to max map or reduce limit
   */
  public void testUserLimitsWithMaxCapacities() throws Exception {
    setUp(2, 2, 2);
    // set up some queues
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 50));
    resConf.setFakeQueues(queues);
    resConf.setMaxCapacity("default", 75.0f);
    scheduler.setResourceManagerConf(resConf);
    scheduler.setAssignMultipleTasks(true);
    scheduler.start();

    // submit a job
    FakeJobInProgress fjob1 =
      submitJobAndInit(JobStatus.PREP, 10, 10, "default", "u1");
    FakeJobInProgress fjob2 =
      submitJobAndInit(JobStatus.PREP, 10, 10, "default", "u2");

    // for queue 'default', maxCapacity for map and reduce is 3.
    // initial user limit for 50% assuming there are 2 users/queue is.
    //  1 map and 1 reduce.
    // after max capacity it is 1.5 each.

    //first job would be given 1 job each.
    List<Task> t1 = this.checkMultipleAssignment(
      "tt1", "attempt_test_0001_m_000001_0 on tt1",
      "attempt_test_0001_r_000001_0 on tt1");

    //for user u1 we have reached the limit. that is 1 job.
    //1 more map and reduce tasks.
    List<Task> t2 = this.checkMultipleAssignment(
      "tt1", "attempt_test_0002_m_000001_0 on tt1",
      "attempt_test_0002_r_000001_0 on tt1");

    t1 = this.checkMultipleAssignment(
      "tt2", "attempt_test_0001_m_000002_0 on tt2",
      "attempt_test_0001_r_000002_0 on tt2");

    t1 = this.checkMultipleAssignment("tt2", null,null);
  }


  // test user limits
  public void testUserLimits() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
    // we should get a task 
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    // Submit another job, from a different user
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u2");
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
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
    // we should get a task 
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    // since we're the only job, we get another map
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    // Submit another job, from a different user
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u2");
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
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    FakeJobInProgress j1 = submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
    // we should get a task 
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    // since we're the only job, we get another map
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    // we get two more maps from 'default queue'
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000004_0 on tt2");
    // Submit another job, from a different user
    FakeJobInProgress j2 = submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u2");
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
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    // add some more TTs 
    taskTrackerManager.addTaskTracker("tt3");
    taskTrackerManager.addTaskTracker("tt4");
    taskTrackerManager.addTaskTracker("tt5");

    // u1 submits job
    FakeJobInProgress j1 = submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    // it gets the first 5 slots
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000004_0 on tt2");
    checkAssignment("tt3", "attempt_test_0001_m_000005_0 on tt3");
    // u2 submits job with 4 slots
    FakeJobInProgress j2 = submitJobAndInit(JobStatus.PREP, 4, 4, null, "u2");
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
    // All the jobs are inited when submitted
    // because of addition of Eager Job Initializer all jobs in this
    //case would e initialised.
    submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    // u2 also submits a job
    submitJobAndInit(JobStatus.PREP, 10, 10, null, "u2");
    // now u3 submits a job
    submitJobAndInit(JobStatus.PREP, 2, 2, null, "u3");
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
    String[] qs = { "default" };
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 50));
    resConf.setFakeQueues(queues);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // Submit one normal job to the other queue.
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setUser("u1");
    jConf.setQueueName("default");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    LOG.debug("Submit one high memory(2GB maps, 2GB reduces) job of "
        + "6 map and 6 reduce tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setQueueName("default");
    jConf.setUser("u2");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    // Verify that normal job takes 3 task assignments to hit user limits
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000003_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000003_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000004_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000004_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000005_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000005_0 on tt1");
    // u1 has 5 map slots and 5 reduce slots. u2 has none. So u1's user limits
    // are hit. So u2 should get slots

    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000002_0 on tt1");

    // u1 has 5 map slots and 5 reduce slots. u2 has 4 map slots and 4 reduce
    // slots. Because of high memory tasks, giving u2 another task would
    // overflow limits. So, no more tasks should be given to anyone.
    assertNull(scheduler.assignTasks(tracker("tt1")));
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
   * - Assign one task to a task tracker. (Map)
   * - Check waiting job count, it should be 4 now and used map (%) = 100
   * - Assign another one task (Reduce)
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
   * - Then schedule a map of the job in running queue.
   * - Run the poller because the poller is responsible for waiting
   * jobs count. Check the count, it should be using 100% map and one
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
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
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
    assertEquals(infoStrings[6], "Capacity: " + totalMaps * 50 / 100
        + " slots");
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[9], "-------------");
    assertEquals(infoStrings[10], "Reduce tasks");
    assertEquals(infoStrings[11], "Capacity: " + totalReduces * 50 / 100
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
      submitJobs(1, 5, "default").get("u1");
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
    Task t1 = checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    //Initalize extra job.
    controlledInitializationPoller.selectJobsToInitialize();

    //Get scheduling information, now the number of waiting job should have
    //changed to 4 as one is scheduled and has become running.
    // make sure we update our stats
    scheduler.updateQSIInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 20);
    assertEquals(infoStrings[7], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 1");
    assertEquals(infoStrings[9], "Active users:");
    assertEquals(infoStrings[10], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[14], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[15], "Running tasks: 0");
    assertEquals(infoStrings[18], "Number of Waiting Jobs: 4");

    //assign a reduce task
    Task t2 = checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    // make sure we update our stats
    scheduler.updateQSIInfoForTests();
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
    taskTrackerManager.finishTask("tt1", t1.getTaskID().toString(), u1j1);
    taskTrackerManager.finishTask("tt1", t2.getTaskID().toString(), u1j1);
    taskTrackerManager.finalizeJob(u1j1);

    // make sure we update our stats
    scheduler.updateQSIInfoForTests();
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
    assertTrue("User1 job 2 not initalized ",
        u1j2.getStatus().getRunState() == JobStatus.RUNNING);
    taskTrackerManager.finalizeJob(u1j2, JobStatus.FAILED);
    //Run initializer to clean up failed jobs
    controlledInitializationPoller.selectJobsToInitialize();
    // make sure we update our stats
    scheduler.updateQSIInfoForTests();
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
    assertFalse("User1 job 5 initalized ",
        u1j5.getStatus().getRunState() == JobStatus.RUNNING);

    taskTrackerManager.finalizeJob(u1j5, JobStatus.FAILED);
    //run initializer to clean up failed job
    controlledInitializationPoller.selectJobsToInitialize();
    // make sure we update our stats
    scheduler.updateQSIInfoForTests();
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
    t1 = checkAssignment("tt1", "attempt_test_0003_m_000001_0 on tt1");
    FakeJobInProgress u1j3 = userJobs.get(2);
    assertTrue("User Job 3 not running ",
        u1j3.getStatus().getRunState() == JobStatus.RUNNING);

    //now the running count of map should be one and waiting jobs should be
    //one. run the poller as it is responsible for waiting count
    controlledInitializationPoller.selectJobsToInitialize();
    // make sure we update our stats
    scheduler.updateQSIInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 20);
    assertEquals(infoStrings[7], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 1");
    assertEquals(infoStrings[9], "Active users:");
    assertEquals(infoStrings[10], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[18], "Number of Waiting Jobs: 1");

    //Fail the executing job
    taskTrackerManager.finalizeJob(u1j3, JobStatus.FAILED);
    // make sure we update our stats
    scheduler.updateQSIInfoForTests();
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
   * @throws IOException
   */
  public void testDisabledMemoryBasedScheduling()
      throws IOException {

    LOG.debug("Starting the scheduler.");
    taskTrackerManager = new FakeTaskTrackerManager(1, 1, 1);

    taskTrackerManager.addQueues(new String[] { "default" });
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // memory-based scheduling disabled by default.
    scheduler.start();

    LOG.debug("Submit one high memory job of 1 3GB map task "
        + "and 1 1GB reduce task.");
    JobConf jConf = new JobConf();
    jConf.setMemoryForMapTask(3 * 1024L); // 3GB
    jConf.setMemoryForReduceTask(1 * 1024L); // 1 GB
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    submitJobAndInit(JobStatus.RUNNING, jConf);

    // assert that all tasks are launched even though they transgress the
    // scheduling limits.

    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
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
    taskTrackerManager = new FakeTaskTrackerManager(1, 2, 1);

    taskTrackerManager.addQueues(new String[] { "default" });
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
        2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
        1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // The situation : Two jobs in the queue. First job with only maps and no
    // reduces and is a high memory job. Second job is a normal job with both
    // maps and reduces.
    // First job cannot run for want of memory for maps. In this case, second
    // job's reduces should run.
    
    LOG.debug("Submit one high memory(2GB maps, 0MB reduces) job of "
        + "2 map tasks");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(0);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    LOG.debug("Submit another regular memory(1GB vmem maps/reduces) job of "
        + "2 map/red tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);
    
    // first, a map from j1 will run
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    // Total 2 map slots should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 1, 2, 100.0f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 0L);

    // at this point, the scheduler tries to schedule another map from j1. 
    // there isn't enough space. The second job's reduce should be scheduled.
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    // Total 1 reduce slot should be accounted for.
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 1,
        100.0f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 1 * 1024L);
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
    taskTrackerManager.addQueues(new String[] { "default" });
    resConf.setFakeQueues(queues);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal jobs 1GB maps/reduces. 2GB limit on maps/reduces
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
        2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
        2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    LOG.debug("Submit one normal memory(1GB maps/reduces) job of "
        + "2 map, 2 reduce tasks.");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    // Fill a tt with this job's tasks.
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    // Total 1 map slot should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 1, 1, 16.7f);
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 
        1, 1, 0, 0, 0, 0),
        (String) job1.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt1", 1 * 1024L, 0L);

    // same for reduces.
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 1, 16.7f);
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 
        1, 1, 0, 1, 1, 0),
        (String) job1.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt1", 1 * 1024L, 1 * 1024L);

    // fill another TT with the rest of the tasks of the job
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");

    LOG.debug("Submit one high memory(2GB maps/reduces) job of "
        + "2 map, 2 reduce tasks.");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    // Have another TT run one task of each type of the high RAM
    // job. This will fill up the TT. 
    checkAssignment("tt3", "attempt_test_0002_m_000001_0 on tt3");
    checkOccupiedSlots("default", TaskType.MAP, 1, 4, 66.7f);
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 
        1, 2, 0, 0, 0, 0),
        (String) job2.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt3", 2 * 1024L, 0L);

    checkAssignment("tt3", "attempt_test_0002_r_000001_0 on tt3");
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 4, 66.7f);
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 
        1, 2, 0, 1, 2, 0),
        (String) job2.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt3", 2 * 1024L, 2 * 1024L);

    LOG.debug("Submit one normal memory(1GB maps/reduces) job of "
        + "1 map, 1 reduce tasks.");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job3 = submitJobAndInit(JobStatus.PREP, jConf);

    // Send a TT with insufficient space for task assignment,
    // This will cause a reservation for the high RAM job.
    assertNull(scheduler.assignTasks(tracker("tt1")));

    // reserved tasktrackers contribute to occupied slots for maps and reduces
    checkOccupiedSlots("default", TaskType.MAP, 1, 6, 100.0f);
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 6, 100.0f);
    checkMemReservedForTasksOnTT("tt1", 1 * 1024L, 1 * 1024L);
    LOG.info(job2.getSchedulingInfo());
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 
        1, 2, 2, 1, 2, 2),
        (String) job2.getSchedulingInfo());
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 
        0, 0, 0, 0, 0, 0),
        (String) job3.getSchedulingInfo());
    
    // Reservations are already done for job2. So job3 should go ahead.
    checkAssignment("tt2", "attempt_test_0003_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000001_0 on tt2");
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
    taskTrackerManager.addQueues(new String[] { "default" });
    resConf.setFakeQueues(queues);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    LOG.debug("Assume TT has 2GB for maps and 2GB for reduces");
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
        2 * 1024L);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 512);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
        2 * 1024L);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 512);
    scheduler.setResourceManagerConf(resConf);
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
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    // 1st cycle - 1 map gets assigned.
    Task t = checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    // Total 1 map slot should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 1, 1, 50.0f);
    checkMemReservedForTasksOnTT("tt1",  512L, 0L);

    // 1st cycle of reduces - 1 reduce gets assigned.
    Task t1 = checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    // Total 1 reduce slot should be accounted for.
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 1,
        50.0f);
    checkMemReservedForTasksOnTT("tt1",  512L, 512L);
    
    // kill this job !
    taskTrackerManager.killJob(job1.getJobID());
    // No more map/reduce slots should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 0, 0, 0.0f);
    checkOccupiedSlots("default", TaskType.REDUCE, 0, 0,
        0.0f);
    
    // retire the job
    taskTrackerManager.removeJob(job1.getJobID());
    
    // submit another job.
    LOG.debug("Submitting another normal job with 2 maps and 2 reduces");
    jConf = new JobConf();
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setMemoryForMapTask(512);
    jConf.setMemoryForReduceTask(512);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);
    
    // since with HADOOP-5964, we don't rely on a job conf to get
    // the memory occupied, scheduling should be able to work correctly.
    t1 = checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkOccupiedSlots("default", TaskType.MAP, 1, 1, 50);
    checkMemReservedForTasksOnTT("tt1", 1024L, 512L);

    // assign a reduce now.
    t1 = checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 1, 50);
    checkMemReservedForTasksOnTT("tt1", 1024L, 1024L);
    
    // now, no more can be assigned because all the slots are blocked.
    assertNull(scheduler.assignTasks(tracker("tt1")));

    // finish the tasks on the tracker.
    taskTrackerManager.finishTask("tt1", t.getTaskID().toString(), job1);
    taskTrackerManager.finishTask("tt1", t1.getTaskID().toString(), job1);
    
    // now a new task can be assigned.
    t = checkAssignment("tt1", "attempt_test_0002_m_000002_0 on tt1");
    checkOccupiedSlots("default", TaskType.MAP, 1, 2, 100.0f);
    // memory used will change because of the finished task above.
    checkMemReservedForTasksOnTT("tt1", 1024L, 512L);
    
    // reduce can be assigned.
    t = checkAssignment("tt1", "attempt_test_0002_r_000002_0 on tt1");
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 2, 100.0f);
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
    String[] qs = { "default" };
    taskTrackerManager = new FakeTaskTrackerManager(2, 1, 1);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
  
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller initPoller = scheduler.getInitializationPoller();

    // submit 4 jobs each for 3 users.
    HashMap<String, ArrayList<FakeJobInProgress>> userJobs = submitJobs(3,
        4, "default");

    // get the jobs submitted.
    ArrayList<FakeJobInProgress> u1Jobs = userJobs.get("u1");
    ArrayList<FakeJobInProgress> u2Jobs = userJobs.get("u2");
    ArrayList<FakeJobInProgress> u3Jobs = userJobs.get("u3");
    
    // reference to the initializedJobs data structure
    // changes are reflected in the set as they are made by the poller
    Set<JobID> initializedJobs = initPoller.getInitializedJobList();
    
    // we should have 12 (3 x 4) jobs in the job queue
    assertEquals(mgr.getWaitingJobs("default").size(), 12);

    // run one poller iteration.
    controlledInitializationPoller.selectJobsToInitialize();
    
    // the poller should initialize 6 jobs
    // 3 users and 2 jobs from each
    assertEquals(initializedJobs.size(), 6);

    assertTrue("Initialized jobs didnt contain the user1 job 1",
        initializedJobs.contains(u1Jobs.get(0).getJobID()));
    assertTrue("Initialized jobs didnt contain the user1 job 2",
        initializedJobs.contains(u1Jobs.get(1).getJobID()));
    assertTrue("Initialized jobs didnt contain the user2 job 1",
        initializedJobs.contains(u2Jobs.get(0).getJobID()));
    assertTrue("Initialized jobs didnt contain the user2 job 2",
        initializedJobs.contains(u2Jobs.get(1).getJobID()));
    assertTrue("Initialized jobs didnt contain the user3 job 1",
        initializedJobs.contains(u3Jobs.get(0).getJobID()));
    assertTrue("Initialized jobs didnt contain the user3 job 2",
        initializedJobs.contains(u3Jobs.get(1).getJobID()));
    
    // now submit one more job from another user.
    FakeJobInProgress u4j1 = 
      submitJob(JobStatus.PREP, 1, 1, "default", "u4");

    // run the poller again.
    controlledInitializationPoller.selectJobsToInitialize();
    
    // since no jobs have started running, there should be no
    // change to the initialized jobs.
    assertEquals(initializedJobs.size(), 6);
    assertFalse("Initialized jobs contains user 4 jobs",
        initializedJobs.contains(u4j1.getJobID()));
    
    // This event simulates raising the event on completion of setup task
    // and moves the job to the running list for the scheduler to pick up.
    raiseStatusChangeEvents(mgr);
    
    // get some tasks assigned.
    Task t1 = checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    Task t2 = checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    Task t3 = checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    Task t4 = checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    taskTrackerManager.finishTask("tt1", t1.getTaskID().toString(), u1Jobs.get(0));
    taskTrackerManager.finishTask("tt1", t2.getTaskID().toString(), u1Jobs.get(0));
    taskTrackerManager.finishTask("tt2", t3.getTaskID().toString(), u1Jobs.get(1));
    taskTrackerManager.finishTask("tt2", t4.getTaskID().toString(), u1Jobs.get(1));

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
    assertFalse("Initialized jobs contains the user1 job 1",
        initializedJobs.contains(u1Jobs.get(0).getJobID()));
    assertFalse("Initialized jobs contains the user1 job 2",
        initializedJobs.contains(u1Jobs.get(1).getJobID()));
    
    // finish one more job
    t1 = checkAssignment("tt1", "attempt_test_0003_m_000001_0 on tt1");
    t2 = checkAssignment("tt1", "attempt_test_0003_r_000001_0 on tt1");
    taskTrackerManager.finishTask("tt1", t1.getTaskID().toString(), u1Jobs.get(2));
    taskTrackerManager.finishTask("tt1", t2.getTaskID().toString(), u1Jobs.get(2));

    // no new jobs should be picked up, because max user limit
    // is still 3.
    controlledInitializationPoller.selectJobsToInitialize();
    
    assertEquals(initializedJobs.size(), 5);
    
    // run 1 more jobs.. 
    t1 = checkAssignment("tt1", "attempt_test_0004_m_000001_0 on tt1");
    t1 = checkAssignment("tt1", "attempt_test_0004_r_000001_0 on tt1");
    taskTrackerManager.finishTask("tt1", t1.getTaskID().toString(), u1Jobs.get(3));
    taskTrackerManager.finishTask("tt1", t2.getTaskID().toString(), u1Jobs.get(3));
    
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
    String[] qs = { "default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    JobInitializationPoller initPoller = scheduler.getInitializationPoller();
    Set<JobID> initializedJobsList = initPoller.getInitializedJobList();

    // submit 3 jobs for 3 users
    submitJobs(3,3,"default");
    controlledInitializationPoller.selectJobsToInitialize();
    assertEquals(initializedJobsList.size(), 6);
    
    // submit 2 job for a different user. one of them will be made high priority
    FakeJobInProgress u4j1 = submitJob(JobStatus.PREP, 1, 1, "default", "u4");
    FakeJobInProgress u4j2 = submitJob(JobStatus.PREP, 1, 1, "default", "u4");
    
    controlledInitializationPoller.selectJobsToInitialize();
    
    // shouldn't change
    assertEquals(initializedJobsList.size(), 6);
    
    assertFalse("Contains U4J1 high priority job " , 
        initializedJobsList.contains(u4j1.getJobID()));
    assertFalse("Contains U4J2 Normal priority job " , 
        initializedJobsList.contains(u4j2.getJobID()));

    // change priority of one job
    taskTrackerManager.setPriority(u4j1, JobPriority.VERY_HIGH);
    
    controlledInitializationPoller.selectJobsToInitialize();
    
    // the high priority job should get initialized, but not the
    // low priority job from u4, as we have already exceeded the
    // limit.
    assertEquals(initializedJobsList.size(), 7);
    assertTrue("Does not contain U4J1 high priority job " , 
        initializedJobsList.contains(u4j1.getJobID()));
    assertFalse("Contains U4J2 Normal priority job " , 
        initializedJobsList.contains(u4j2.getJobID()));
    controlledInitializationPoller.stopRunning();
  }
  
  public void testJobMovement() throws Exception {
    String[] qs = { "default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
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
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    //Start the scheduler.
    scheduler.start();
    //Submit a job and wait till it completes
    FakeJobInProgress job = 
      submitJob(JobStatus.PREP, 1, 1, "q1", "u1");
    controlledInitializationPoller.selectJobsToInitialize();
    raiseStatusChangeEvents(scheduler.jobQueuesManager, "q1");
    Task t = checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    t = checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
  }
  
  public void testFailedJobInitalizations() throws Exception {
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    
    //Submit a job whose initialization would fail always.
    FakeJobInProgress job =
      new FakeFailingJobInProgress(new JobID("test", ++jobCounter),
          new JobConf(), taskTrackerManager,"u1");
    job.getStatus().setRunState(JobStatus.PREP);
    taskTrackerManager.submitJob(job);
    //check if job is present in waiting list.
    assertEquals("Waiting job list does not contain submitted job",
        1, mgr.getWaitingJobCount("default"));
    assertTrue("Waiting job does not contain submitted job", 
        mgr.getWaitingJobs("default").contains(job));
    //initialization should fail now.
    controlledInitializationPoller.selectJobsToInitialize();
    //Check if the job has been properly cleaned up.
    assertEquals("Waiting job list contains submitted job",
        0, mgr.getWaitingJobCount("default"));
    assertFalse("Waiting job contains submitted job", 
        mgr.getWaitingJobs("default").contains(job));
    assertFalse("Waiting job contains submitted job", 
        mgr.getRunningJobQueue("default").contains(job));
  }
  
  private void checkRunningJobMovementAndCompletion() throws IOException {
    
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller p = scheduler.getInitializationPoller();
    // submit a job
    FakeJobInProgress job = 
      submitJob(JobStatus.PREP, 1, 1, "default", "u1");
    controlledInitializationPoller.selectJobsToInitialize();
    
    assertEquals(p.getInitializedJobList().size(), 1);

    // make it running.
    raiseStatusChangeEvents(mgr);
    
    // it should be there in both the queues.
    assertTrue("Job not present in Job Queue",
        mgr.getWaitingJobs("default").contains(job));
    assertTrue("Job not present in Running Queue",
        mgr.getRunningJobQueue("default").contains(job));
    
    // assign a task
    Task t = checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    t = checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    
    controlledInitializationPoller.selectJobsToInitialize();
    
    // now this task should be removed from the initialized list.
    assertTrue(p.getInitializedJobList().isEmpty());

    // the job should also be removed from the job queue as tasks
    // are scheduled
    assertFalse("Job present in Job Queue",
        mgr.getWaitingJobs("default").contains(job));
    
    // complete tasks and job
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", job);
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000001_0", job);
    taskTrackerManager.finalizeJob(job);
    
    // make sure it is removed from the run queue
    assertFalse("Job present in running queue",
        mgr.getRunningJobQueue("default").contains(job));
  }
  
  private void checkFailedRunningJobMovement() throws IOException {
    
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    
    //submit a job and initalized the same
    FakeJobInProgress job = 
      submitJobAndInit(JobStatus.RUNNING, 1, 1, "default", "u1");
    
    //check if the job is present in running queue.
    assertTrue("Running jobs list does not contain submitted job",
        mgr.getRunningJobQueue("default").contains(job));
    
    taskTrackerManager.finalizeJob(job, JobStatus.KILLED);
    
    //check if the job is properly removed from running queue.
    assertFalse("Running jobs list does not contain submitted job",
        mgr.getRunningJobQueue("default").contains(job));
    
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
   * @throws IOException
   */
  public void testSpeculativeTaskScheduling() throws IOException {
    String[] qs = {"default"};
    taskTrackerManager = new FakeTaskTrackerManager(2, 1, 1);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobConf conf = new JobConf();
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    conf.setMapSpeculativeExecution(true);
    conf.setReduceSpeculativeExecution(true);
    //Submit a job which would have one speculative map and one speculative
    //reduce.
    FakeJobInProgress fjob1 = submitJob(JobStatus.PREP, conf);
    
    conf = new JobConf();
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    //Submit a job which has no speculative map or reduce.
    FakeJobInProgress fjob2 = submitJob(JobStatus.PREP, conf);    

    //Ask the poller to initalize all the submitted job and raise status
    //change event.
    controlledInitializationPoller.selectJobsToInitialize();
    raiseStatusChangeEvents(mgr);

    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    assertTrue("Pending maps of job1 greater than zero", 
        (fjob1.pendingMaps() == 0));
    checkAssignment("tt2", "attempt_test_0001_m_000001_1 on tt2");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    assertTrue("Pending reduces of job2 greater than zero", 
        (fjob1.pendingReduces() == 0));
    checkAssignment("tt2", "attempt_test_0001_r_000001_1 on tt2");

    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", fjob1);
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000001_1", fjob1);
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000001_0", fjob1);
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_r_000001_1", fjob1);
    taskTrackerManager.finalizeJob(fjob1);
    
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000001_0", fjob2);
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000001_0", fjob2);
    taskTrackerManager.finalizeJob(fjob2);    
  }

  /**
   * Test to verify that TTs are reserved for high memory jobs, but only till a
   * TT is reserved for each of the pending task.
   * @throws IOException
   */
  public void testTTReservingWithHighMemoryJobs()
      throws IOException {
    // 3 taskTrackers, 2 map and 0 reduce slots on each TT
    taskTrackerManager = new FakeTaskTrackerManager(3, 2, 0);

    taskTrackerManager.addQueues(new String[] { "default" });
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    LOG.debug("Submit a regular memory(1GB vmem maps/reduces) job of "
        + "3 map/red tasks");
    JobConf jConf = new JobConf(conf);
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(3);
    jConf.setNumReduceTasks(3);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    // assign one map task of job1 on all the TTs
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt3", "attempt_test_0001_m_000003_0 on tt3");
    scheduler.updateQSIInfoForTests();

    LOG.info(job1.getSchedulingInfo());
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 3, 3, 0, 0,
        0, 0), (String) job1.getSchedulingInfo());

    LOG.debug("Submit one high memory(2GB maps, 0MB reduces) job of "
        + "2 map tasks");
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(0);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    LOG.debug("Submit another regular memory(1GB vmem maps/reduces) job of "
        + "2 map/red tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job3 = submitJobAndInit(JobStatus.PREP, jConf);

    // Job2, a high memory job cannot be accommodated on a any TT. But with each
    // trip to the scheduler, each of the TT should be reserved by job2.
    assertNull(scheduler.assignTasks(tracker("tt1")));
    scheduler.updateQSIInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 2, 0,
        0, 0), (String) job2.getSchedulingInfo());

    assertNull(scheduler.assignTasks(tracker("tt2")));
    scheduler.updateQSIInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 4, 0,
        0, 0), (String) job2.getSchedulingInfo());

    // Job2 has only 2 pending tasks. So no more reservations. Job3 should get
    // slots on tt3. tt1 and tt2 should not be assigned any slots with the
    // reservation stats intact.
    assertNull(scheduler.assignTasks(tracker("tt1")));
    scheduler.updateQSIInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 4, 0,
        0, 0), (String) job2.getSchedulingInfo());

    assertNull(scheduler.assignTasks(tracker("tt2")));
    scheduler.updateQSIInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 4, 0,
        0, 0), (String) job2.getSchedulingInfo());

    checkAssignment("tt3", "attempt_test_0003_m_000001_0 on tt3");
    scheduler.updateQSIInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 4, 0,
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
    String[] qs = { "default", "q1" };
    String[] reversedQs = { qs[1], qs[0] };
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 100));
    queues.add(new FakeQueueInfo("q1", 50.0f, true, 100));
    resConf.setFakeQueues(queues);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    LOG.debug("Submit one high memory(2GB maps, 2GB reduces) job of "
        + "6 map and 6 reduce tasks");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    // Submit a normal job to the other queue.
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setUser("u1");
    jConf.setQueueName("q1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    // Map 1 of high memory job
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkQueuesOrder(qs, scheduler
        .getOrderedQueues(TaskType.MAP));

    // Reduce 1 of high memory job
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    checkQueuesOrder(qs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // Map 1 of normal job
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));

    // Reduce 1 of normal job
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // Map 2 of normal job
    checkAssignment("tt1", "attempt_test_0002_m_000002_0 on tt1");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));

    // Reduce 2 of normal job
    checkAssignment("tt1", "attempt_test_0002_r_000002_0 on tt1");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // Now both the queues are equally served. But the comparator doesn't change
    // the order if queues are equally served.

    // Map 3 of normal job
    checkAssignment("tt2", "attempt_test_0002_m_000003_0 on tt2");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));

    // Reduce 3 of normal job
    checkAssignment("tt2", "attempt_test_0002_r_000003_0 on tt2");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // Map 2 of high memory job
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkQueuesOrder(qs, scheduler
        .getOrderedQueues(TaskType.MAP));

    // Reduce 2 of high memory job
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkQueuesOrder(qs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // Map 4 of normal job
    checkAssignment("tt2", "attempt_test_0002_m_000004_0 on tt2");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));

    // Reduce 4 of normal job
    checkAssignment("tt2", "attempt_test_0002_r_000004_0 on tt2");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));
  }

  private void checkFailedInitializedJobMovement() throws IOException {
    
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller p = scheduler.getInitializationPoller();
    
    //submit a job
    FakeJobInProgress job = submitJob(JobStatus.PREP, 1, 1, "default", "u1");
    //Initialize the job
    p.selectJobsToInitialize();
    //Don't raise the status change event.
    
    //check in waiting and initialized jobs list.
    assertTrue("Waiting jobs list does not contain the job",
        mgr.getWaitingJobs("default").contains(job));
    
    assertTrue("Initialized job does not contain the job",
        p.getInitializedJobList().contains(job.getJobID()));
    
    //fail the initalized job
    taskTrackerManager.finalizeJob(job, JobStatus.KILLED);
    
    //Check if the job is present in waiting queue
    assertFalse("Waiting jobs list contains failed job",
        mgr.getWaitingJobs("default").contains(job));
    
    //run the poller to do the cleanup
    p.selectJobsToInitialize();
    
    //check for failed job in the initialized job list
    assertFalse("Initialized jobs  contains failed job",
        p.getInitializedJobList().contains(job.getJobID()));
  }
  
  private void checkFailedWaitingJobMovement() throws IOException {
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    // submit a job
    FakeJobInProgress job = submitJob(JobStatus.PREP, 1, 1, "default",
        "u1");
    
    // check in waiting and initialized jobs list.
    assertTrue("Waiting jobs list does not contain the job", mgr
        .getWaitingJobs("default").contains(job));
    // fail the waiting job
    taskTrackerManager.finalizeJob(job, JobStatus.KILLED);

    // Check if the job is present in waiting queue
    assertFalse("Waiting jobs list contains failed job", mgr
        .getWaitingJobs("default").contains(job));
  }
  
  private void raiseStatusChangeEvents(JobQueuesManager mgr) {
    raiseStatusChangeEvents(mgr, "default");
  }
  
  private void raiseStatusChangeEvents(JobQueuesManager mgr, String queueName) {
    Collection<JobInProgress> jips = mgr.getWaitingJobs(queueName);
    for(JobInProgress jip : jips) {
      if(jip.getStatus().getRunState() == JobStatus.RUNNING) {
        JobStatusChangeEvent evt = new JobStatusChangeEvent(jip,
            EventType.RUN_STATE_CHANGED,jip.getStatus());
        mgr.jobUpdated(evt);
      }
    }
  }

  private HashMap<String, ArrayList<FakeJobInProgress>> submitJobs(
      int numberOfUsers, int numberOfJobsPerUser, String queue)
      throws Exception{
    HashMap<String, ArrayList<FakeJobInProgress>> userJobs = 
      new HashMap<String, ArrayList<FakeJobInProgress>>();
    for (int i = 1; i <= numberOfUsers; i++) {
      String user = String.valueOf("u" + i);
      ArrayList<FakeJobInProgress> jips = new ArrayList<FakeJobInProgress>();
      for (int j = 1; j <= numberOfJobsPerUser; j++) {
        jips.add(submitJob(JobStatus.PREP, 1, 1, queue, user));
      }
      userJobs.put(user, jips);
    }
    return userJobs;
  }

  
  protected TaskTracker tracker(String taskTrackerName) {
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

  /**
   * Get the amount of memory that is reserved for tasks on the taskTracker and
   * verify that it matches what is expected.
   * 
   * @param taskTracker
   * @param expectedMemForMapsOnTT
   * @param expectedMemForReducesOnTT
   */
  private void checkMemReservedForTasksOnTT(String taskTracker,
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
      assertEquals(observedMemForMapsOnTT, null);
    } else {
      assertEquals(observedMemForMapsOnTT, (expectedMemForMapsOnTT));
    }
    if (expectedMemForReducesOnTT == null) {
      assertEquals(observedMemForReducesOnTT, null);
    } else {
      assertEquals(observedMemForReducesOnTT, (expectedMemForReducesOnTT));
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
   * @param numActiveUsers in the queue at present.
   * @param expectedOccupiedSlots
   * @param expectedOccupiedSlotsPercent
   * @param incrMapIndex
   * @param incrReduceIndex
   */
  private void checkOccupiedSlots(
    String queue,
    TaskType type, int numActiveUsers,
    int expectedOccupiedSlots, float expectedOccupiedSlotsPercent,int incrMapIndex
    ,int incrReduceIndex
  ) {
    scheduler.updateQSIInfoForTests();
    QueueManager queueManager = scheduler.taskTrackerManager.getQueueManager();
    String schedulingInfo =
        queueManager.getJobQueueInfo(queue).getSchedulingInfo();
    String[] infoStrings = schedulingInfo.split("\n");
    int index = -1;
    if (type.equals(TaskType.MAP)) {
      index = 7+ incrMapIndex;
    } else if (type.equals(TaskType.REDUCE)) {
      index = (numActiveUsers == 0 ? 12 : 13 + numActiveUsers)+incrReduceIndex;
    }
    LOG.info(infoStrings[index]);
    assertEquals(String.format("Used capacity: %d (%.1f%% of Capacity)",
        expectedOccupiedSlots, expectedOccupiedSlotsPercent),
        infoStrings[index]);
  }

  /**
   *
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
      expectedOccupiedSlotsPercent,0,0);
  }

  private void checkQueuesOrder(String[] expectedOrder, String[] observedOrder) {
    assertTrue("Observed and expected queues are not of same length.",
        expectedOrder.length == observedOrder.length);
    int i = 0;
    for (String expectedQ : expectedOrder) {
      assertTrue("Observed and expected queues are not in the same order. "
          + "Differ at index " + i + ". Got " + observedOrder[i]
          + " instead of " + expectedQ, expectedQ.equals(observedOrder[i]));
      i++;
    }
  }

  public void testDeprecatedMemoryValues() throws IOException {
    // 2 map and 1 reduce slots
    taskTrackerManager.addQueues(new String[] { "default" });
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));  
    resConf.setFakeQueues(queues);
    JobConf conf = (JobConf)(scheduler.getConf());
    conf.set(
      JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY, String.valueOf(
        1024 * 1024 * 3));
    scheduler.setTaskTrackerManager(taskTrackerManager);
    scheduler.setResourceManagerConf(resConf);    
    scheduler.start();

    assertEquals(scheduler.getLimitMaxMemForMapSlot(),3);
    assertEquals(scheduler.getLimitMaxMemForReduceSlot(),3);
  }

  /**
   * Checks for multiple assignment.
   *
   * @param taskTrackerName
   * @param mapAttempt
   * @param reduceAttempt
   * @return
   * @throws IOException
   */
  private List<Task> checkMultipleAssignment(
    String taskTrackerName, String mapAttempt, String reduceAttempt)
    throws IOException {
    List<Task> tasks = scheduler.assignTasks(tracker(taskTrackerName));
    LOG.info(
      " mapAttempt " + mapAttempt + " reduceAttempt " + reduceAttempt +
        " assignTasks result " + tasks);

    if (tasks == null || tasks.isEmpty()) {
      if (mapAttempt != null || reduceAttempt != null ) {
        fail(
          " improper attempt " + tasks + " expected attempts are  map : " +
            mapAttempt + " reduce : " + reduceAttempt);
      } else {
        return tasks;
      }
    }
    
    if (tasks.size() == 1 && (mapAttempt != null && reduceAttempt != null)) {
      fail(
        " improper attempt " + tasks + " expected attempts are  map : " +
          mapAttempt + " reduce : " + reduceAttempt);
    }
    for (Task task : tasks) {
      if (task.toString().contains("_m_")) {
        assertEquals(task.toString(), mapAttempt);
      }

      if (task.toString().contains("_r")) {
        assertEquals(task.toString(), reduceAttempt);
      }
    }
    return tasks;
  }
}
