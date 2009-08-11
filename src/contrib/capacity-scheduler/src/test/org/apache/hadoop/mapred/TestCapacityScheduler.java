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
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configuration;

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
      Task task = new MapTask("", attemptId, 0, "", new BytesWritable()) {
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
      Task task = new ReduceTask("", attemptId, 0, 10) {
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
      super(jId, "", new JobClient.RawSplit(), null, jobConf, job, 0);
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
    
    private Map<String, TaskTrackerStatus> trackers =
      new HashMap<String, TaskTrackerStatus>();
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
        trackers.put(ttName, new TaskTrackerStatus(ttName, ttName + ".host", i,
            new ArrayList<TaskStatus>(), 0, maxMapTasksPerTracker,
            maxReduceTasksPerTracker));
      }
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
      return trackers.values();
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
        public TaskAttemptID getTaskID() {
          return t.getTaskID();
        }

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
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 1, 2, 100.0f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 0L);

    // at this point, the scheduler tries to schedule another map from j1. 
    // there isn't enough space. The second job's reduce should be scheduled.
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    // Total 1 reduce slot should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.REDUCE, 1, 1,
        100.0f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 1 * 1024L);
  }

  /**
   * Test blocking of cluster for lack of memory.
   * @throws IOException
   */
  public void testClusterBlockingForLackOfMemory()
      throws IOException {

    LOG.debug("Starting the scheduler.");
    taskTrackerManager = new FakeTaskTrackerManager(2, 2, 2);

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
        + "1 map, 1 reduce tasks.");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    // Fill the second tt with this job.
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    // Total 1 map slot should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 1, 1, 25.0f);
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 1, 1, 0, 0),
        (String) job1.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt2", 1 * 1024L, 0L);
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
    // Total 1 map slot should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.REDUCE, 1, 1,
        25.0f);
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 1, 1, 1, 1),
        (String) job1.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt2", 1 * 1024L, 1 * 1024L);

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

    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    // Total 3 map slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 1, 3, 75.0f);
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 1, 2, 0, 0),
        (String) job2.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 0L);

    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    // Total 3 reduce slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.REDUCE, 1, 3,
        75.0f);
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 1, 2, 1, 2),
        (String) job2.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 2 * 1024L);

    LOG.debug("Submit one normal memory(1GB maps/reduces) job of "
        + "1 map, 0 reduce tasks.");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job3 = submitJobAndInit(JobStatus.PREP, jConf);

    // Job2 cannot fit on tt2 or tt1. Blocking. Job3 also will not run.
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 1, 3, 75.0f);
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.REDUCE, 1, 3,
        75.0f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 2 * 1024L);
    checkMemReservedForTasksOnTT("tt2", 1 * 1024L, 1 * 1024L);
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 1, 2, 1, 2),
        (String) job2.getSchedulingInfo());
    assertEquals(String.format(
        CapacityTaskScheduler.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 0, 0),
        (String) job3.getSchedulingInfo());
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
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 1, 1, 50.0f);
    checkMemReservedForTasksOnTT("tt1",  512L, 0L);

    // 1st cycle of reduces - 1 reduce gets assigned.
    Task t1 = checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    // Total 1 reduce slot should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.REDUCE, 1, 1,
        50.0f);
    checkMemReservedForTasksOnTT("tt1",  512L, 512L);
    
    // kill this job !
    taskTrackerManager.killJob(job1.getJobID());
    // No more map/reduce slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 0, 0, 0.0f);
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.REDUCE, 0, 0,
        0.0f);
    
    // retire the job
    taskTrackerManager.removeJob(job1.getJobID());
    
    // submit another job.
    LOG.debug("Submitting another normal job with 1 map and 1 reduce");
    jConf = new JobConf();
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setMemoryForMapTask(512);
    jConf.setMemoryForReduceTask(512);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);
    
    // 2nd cycle - nothing should get assigned. Memory matching code
    // will see the job is missing and fail memory requirements.
    assertNull(scheduler.assignTasks(tracker("tt1")));
    checkMemReservedForTasksOnTT("tt1", null, null);

    // calling again should not make a difference, as the task is still running
    assertNull(scheduler.assignTasks(tracker("tt1")));
    checkMemReservedForTasksOnTT("tt1", null, null);
    
    // finish the tasks on the tracker.
    taskTrackerManager.finishTask("tt1", t.getTaskID().toString(), job1);
    taskTrackerManager.finishTask("tt1", t1.getTaskID().toString(), job1);

    // now a new task can be assigned.
    t = checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    // Total 1 map slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 1, 1, 50.0f);
    checkMemReservedForTasksOnTT("tt1", 512L, 0L);

    // reduce can be assigned.
    t = checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    // Total 1 reduce slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.REDUCE, 1, 1,
        50.0f);
    checkMemReservedForTasksOnTT("tt1", 512L, 512L);
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
   * Test case to test scheduling of
   * <ol> 
   * <li>High ram job with speculative map execution.
   * <ul>
   * <li>Submit one high ram job which has speculative map.</li>
   * <li>Submit a normal job which has no speculative map.</li>
   * <li>Scheduler should schedule first all map tasks from first job and block
   * the cluster till both maps from first job get completed.
   * </ul>
   * </li>
   * <li>High ram job with speculative reduce execution.
   * <ul>
   * <li>Submit one high ram job which has speculative reduce.</li>
   * <li>Submit a normal job which has no speculative reduce.</li>
   * <li>Scheduler should schedule first all reduce tasks from first job and
   * block the cluster till both reduces are completed.</li>
   * </ul>
   * </li>
   * </ol>
   * @throws IOException
   */
  public void testHighRamJobWithSpeculativeExecution() throws IOException {
    // 2 TTs, 3 map and 3 reduce slots on each TT
    taskTrackerManager = new FakeTaskTrackerManager(2, 3, 3);

    taskTrackerManager.addQueues(new String[] { "default" });
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // 1GB for each map, 1GB for each reduce
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
        3 * 1024L);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024L);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
        3 * 1024L);
    scheduler.getConf().setLong(
        JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024L);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    //Submit a high memory job with speculative tasks.
    JobConf jConf = new JobConf();
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(0);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    jConf.setMapSpeculativeExecution(true);
    jConf.setReduceSpeculativeExecution(false);
    FakeJobInProgress job1 =
        new FakeJobInProgress(new JobID("test", ++jobCounter), jConf,
            taskTrackerManager, "u1");
    taskTrackerManager.submitJob(job1);

    //Submit normal job
    jConf = new JobConf();
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(0);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    jConf.setMapSpeculativeExecution(false);
    jConf.setReduceSpeculativeExecution(false);
    FakeJobInProgress job2 = submitJob(JobStatus.PREP, jConf);

    controlledInitializationPoller.selectJobsToInitialize();
    raiseStatusChangeEvents(scheduler.jobQueuesManager);

    // first, a map from j1 will run
    // at this point, there is a speculative task for the same job to be
    //scheduled. This task would be scheduled. Till the tasks from job1 gets
    //complete none of the tasks from other jobs would be scheduled.
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 0L);
    assertEquals("pending maps greater than zero " , job1.pendingMaps(), 0);
    // Total 2 map slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 1, 2, 33.3f);

    //make same tracker get back, check if you are blocking. Your job
    //has speculative map task so tracker should be blocked even tho' it
    //can run job2's map.
    assertNull(scheduler.assignTasks(tracker("tt1")));
    // Total 2 map slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 1, 2, 33.3f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 0L);

    //TT2 now gets speculative map of the job1
    checkAssignment("tt2", "attempt_test_0001_m_000001_1 on tt2");
    // Total 4 map slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 1, 4, 66.7f);
    checkMemReservedForTasksOnTT("tt2", 2 * 1024L, 0L);

    // Now since the first job has no more speculative maps, it can schedule
    // the second job.
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    // Total 5 map slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 1, 5, 83.3f);
    checkMemReservedForTasksOnTT("tt1", 3 * 1024L, 0L);

    //finish everything
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0", 
        job1);
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000001_1", 
        job1);
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000001_0", 
        job2);
    taskTrackerManager.finalizeJob(job1);
    taskTrackerManager.finalizeJob(job2);
    
    //Now submit high ram job with speculative reduce and check.
    jConf = new JobConf();
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024L);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    jConf.setMapSpeculativeExecution(false);
    jConf.setReduceSpeculativeExecution(true);
    FakeJobInProgress job3 =
        new FakeJobInProgress(new JobID("test", ++jobCounter), jConf,
            taskTrackerManager, "u1");
    taskTrackerManager.submitJob(job3);

    //Submit normal job w.r.t reduces
    jConf = new JobConf();
    jConf.setMemoryForMapTask(1 * 1024L);
    jConf.setMemoryForReduceTask(1 * 1024L);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    jConf.setMapSpeculativeExecution(false);
    jConf.setReduceSpeculativeExecution(false);
    FakeJobInProgress job4 = submitJob(JobStatus.PREP, jConf);
    
    controlledInitializationPoller.selectJobsToInitialize();
    raiseStatusChangeEvents(scheduler.jobQueuesManager);

    // Finish up the map scheduler
    checkAssignment("tt1", "attempt_test_0003_m_000001_0 on tt1");
    // Total 2 map slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 1, 2, 33.3f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 0L);

    checkAssignment("tt2", "attempt_test_0004_m_000001_0 on tt2");
    // Total 3 map slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.MAP, 1, 3, 50.0f);
    checkMemReservedForTasksOnTT("tt2", 1 * 1024L, 0L);

    // first, a reduce from j3 will run
    // at this point, there is a speculative task for the same job to be
    //scheduled. This task would be scheduled. Till the tasks from job3 gets
    //complete none of the tasks from other jobs would be scheduled.
    checkAssignment("tt1", "attempt_test_0003_r_000001_0 on tt1");
    assertEquals("pending reduces greater than zero ", job3.pendingReduces(),
        0);
    // Total 2 reduce slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.REDUCE, 1, 2,
        33.3f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 2*1024L);

    //make same tracker get back, check if you are blocking. Your job
    //has speculative reduce task so tracker should be blocked even tho' it
    //can run job4's reduce.
    assertNull(scheduler.assignTasks(tracker("tt1")));
    // Total 2 reduce slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.REDUCE, 1, 2,
        33.3f);

    //TT2 now gets speculative reduce of the job3
    checkAssignment("tt2", "attempt_test_0003_r_000001_1 on tt2");
    // Total 4 reduce slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.REDUCE, 1, 4,
        66.7f);
    checkMemReservedForTasksOnTT("tt2", 1 * 1024L, 2 * 1024L);

    // Now since j3 has no more speculative reduces, it can schedule
    // the j4.
    checkAssignment("tt1", "attempt_test_0004_r_000001_0 on tt1");
    // Total 5 reduce slots should be accounted for.
    checkOccupiedSlots("default", CapacityTaskScheduler.TYPE.REDUCE, 1, 5,
        83.3f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 3 * 1024L);
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
        .getOrderedQueues(CapacityTaskScheduler.TYPE.MAP));

    // Reduce 1 of high memory job
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    checkQueuesOrder(qs, scheduler
        .getOrderedQueues(CapacityTaskScheduler.TYPE.REDUCE));

    // Map 1 of normal job
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(CapacityTaskScheduler.TYPE.MAP));

    // Reduce 1 of normal job
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(CapacityTaskScheduler.TYPE.REDUCE));

    // Map 2 of normal job
    checkAssignment("tt1", "attempt_test_0002_m_000002_0 on tt1");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(CapacityTaskScheduler.TYPE.MAP));

    // Reduce 2 of normal job
    checkAssignment("tt1", "attempt_test_0002_r_000002_0 on tt1");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(CapacityTaskScheduler.TYPE.REDUCE));

    // Now both the queues are equally served. But the comparator doesn't change
    // the order if queues are equally served.

    // Map 3 of normal job
    checkAssignment("tt2", "attempt_test_0002_m_000003_0 on tt2");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(CapacityTaskScheduler.TYPE.MAP));

    // Reduce 3 of normal job
    checkAssignment("tt2", "attempt_test_0002_r_000003_0 on tt2");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(CapacityTaskScheduler.TYPE.REDUCE));

    // Map 2 of high memory job
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkQueuesOrder(qs, scheduler
        .getOrderedQueues(CapacityTaskScheduler.TYPE.MAP));

    // Reduce 2 of high memory job
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkQueuesOrder(qs, scheduler
        .getOrderedQueues(CapacityTaskScheduler.TYPE.REDUCE));

    // Map 4 of normal job
    checkAssignment("tt2", "attempt_test_0002_m_000004_0 on tt2");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(CapacityTaskScheduler.TYPE.MAP));

    // Reduce 4 of normal job
    checkAssignment("tt2", "attempt_test_0002_r_000004_0 on tt2");
    checkQueuesOrder(reversedQs, scheduler
        .getOrderedQueues(CapacityTaskScheduler.TYPE.REDUCE));
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
        scheduler.memoryMatcher.getMemReservedForTasks(tracker(taskTracker),
            CapacityTaskScheduler.TYPE.MAP);
    Long observedMemForReducesOnTT =
        scheduler.memoryMatcher.getMemReservedForTasks(tracker(taskTracker),
            CapacityTaskScheduler.TYPE.REDUCE);
    if (expectedMemForMapsOnTT == null) {
      assertTrue(observedMemForMapsOnTT == null);
    } else {
      assertTrue(observedMemForMapsOnTT.equals(expectedMemForMapsOnTT));
    }
    if (expectedMemForReducesOnTT == null) {
      assertTrue(observedMemForReducesOnTT == null);
    } else {
      assertTrue(observedMemForReducesOnTT.equals(expectedMemForReducesOnTT));
    }
  }

  /**
   * Verify the number of slots of type 'type' from the queue 'queue'.
   * 
   * @param queue
   * @param type
   * @param numActiveUsers in the queue at present.
   * @param expectedOccupiedSlots
   * @param expectedOccupiedSlotsPercent
   * @return
   */
  private void checkOccupiedSlots(String queue,
      CapacityTaskScheduler.TYPE type, int numActiveUsers,
      int expectedOccupiedSlots, float expectedOccupiedSlotsPercent) {
    scheduler.updateQSIInfoForTests();
    QueueManager queueManager = scheduler.taskTrackerManager.getQueueManager();
    String schedulingInfo =
        queueManager.getJobQueueInfo(queue).getSchedulingInfo();
    String[] infoStrings = schedulingInfo.split("\n");
    int index = -1;
    if (type.equals(CapacityTaskScheduler.TYPE.MAP)) {
      index = 7;
    } else if (type.equals(CapacityTaskScheduler.TYPE.REDUCE)) {
      index = (numActiveUsers == 0 ? 12 : 13 + numActiveUsers);
    }
    LOG.info(infoStrings[index]);
    assertEquals(String.format("Used capacity: %d (%.1f%% of Capacity)",
        expectedOccupiedSlots, expectedOccupiedSlotsPercent),
        infoStrings[index]);
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
}
