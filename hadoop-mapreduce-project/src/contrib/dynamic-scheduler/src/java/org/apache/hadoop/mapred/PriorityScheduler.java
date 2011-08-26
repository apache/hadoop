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
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 * A {@link TaskScheduler} that 
 * provides the following features: 
 * (1) allows continuous enforcement of user controlled dynamic queue shares,
 * (2) preempts tasks exceeding their queue shares instantaneously when new 
 * jobs arrive,
 * (3) is work conserving,
 * (4) tracks queue usage to only charge when jobs are pending or running,
 * (5) authorizes queue submissions based on symmetric private key HMAC/SHA1 
 * signatures.
 */
class PriorityScheduler extends QueueTaskScheduler {

  private class InitThread extends Thread {
    JobInProgress job;

    InitThread(JobInProgress job) {
      this.job = job;
    }

    @Override
    public void run() {
      taskTrackerManager.initJob(job);
    }
  }

  private class JobListener extends JobInProgressListener {
    @Override
    public void jobAdded(JobInProgress job) {
      new InitThread(job).start();
      synchronized (PriorityScheduler.this) {
        String queue = authorize(job);
        if (queue.equals("")) {
            job.kill(); 
            return;
        }
        jobQueue.add(job);
        QueueJobs jobs = queueJobs.get(queue);
        if (jobs == null) {
          jobs = new QueueJobs(queue);
          queueJobs.put(queue,jobs);
        }
        jobs.jobs.add(job);
        if (debug) {
          LOG.debug("Add job " + job.getProfile().getJobID());
        }
      }
    }
    @Override
    public void jobRemoved(JobInProgress job) {
      synchronized (PriorityScheduler.this) {
        jobQueue.remove(job);
        String queue = getQueue(job);
        queueJobs.get(queue).jobs.remove(job);
      }
    }
    @Override
    public void jobUpdated(JobChangeEvent event) {
    }
  }


  static final Comparator<TaskInProgress> TASK_COMPARATOR
    = new Comparator<TaskInProgress>() {
    public int compare(TaskInProgress o1, TaskInProgress o2) {
      int res = 0;
      if (o1.getProgress() < o2.getProgress()) {
        res = -1;
      } else {
        res = (o1.getProgress() == o2.getProgress() ? 0 : 1);
      }
      if (res == 0) {
        if (o1.getExecStartTime() > o2.getExecStartTime()) {
          res = -1;
        } else {
          res = (o1.getExecStartTime() == o2.getExecStartTime() ? 0 : 1);
        }
      }
      return res;
    }
  };

  static final Comparator<KillQueue> QUEUE_COMPARATOR
    = new Comparator<KillQueue>() {
    public int compare(KillQueue o1, KillQueue o2) {
      if (o1.startTime < o2.startTime) {
        return 1;
      }
      if (o1.startTime > o2.startTime) {
        return -1;
      }
      return 0;
    }
  };

  class QueueJobs {
    String name;
    LinkedList<JobInProgress> jobs = new LinkedList<JobInProgress>();
    QueueJobs(String name) {
      this.name = name;
    }
  }

  class QueueQuota {
    int quota;
    int map_used;
    int reduce_used;
    int map_pending;
    int reduce_pending;
    int mappers;
    int reducers;
    String name;
    QueueQuota(String name) {
      this.name = name;
    }
  }

  private QueueAllocator allocator;

  private static final Log LOG = 
    LogFactory.getLog(PriorityScheduler.class);

  static final boolean MAP = true;
  static final boolean REDUCE = false;
  private static final boolean FILL = true;
  private static final boolean NO_FILL = false;

  private JobListener jobListener = new JobListener();
  private static final boolean debug = LOG.isDebugEnabled();
  private boolean sortTasks = true;
  private long lastKill = 0;
  private long killInterval = 0;
  private PriorityAuthorization auth = new PriorityAuthorization();

  private LinkedList<JobInProgress> jobQueue = 
    new LinkedList<JobInProgress>();
  private HashMap<String,QueueJobs> queueJobs = 
    new HashMap<String,QueueJobs>();

  @Override
  public void start() throws IOException {
    taskTrackerManager.addJobInProgressListener(jobListener);
    sortTasks = conf.getBoolean("mapred.priority-scheduler.sort-tasks", true);
    killInterval = conf.getLong("mapred.priority-scheduler.kill-interval", 0);
    auth.init(conf);
  }

  @Override
  public void terminate() throws IOException {
  }

  @Override
  public void setAllocator(QueueAllocator allocator) {
    this.allocator = allocator;
  }

  private boolean assignMapRedTask(JobInProgress job, 
      TaskTrackerStatus taskTracker, int numTrackers, List<Task> assignedTasks,
      Map<String,QueueQuota> queueQuota, boolean fill, boolean map) 
      throws IOException {
    String queue = getQueue(job);
    QueueQuota quota = queueQuota.get(queue);
    if (quota == null) {
        LOG.error("Queue " + queue + " not configured properly");
        return false;
    }
    if (quota.quota < 1 && !fill) {
      return false;
    }
    Task t = null;
    if (map) {
      t = job.obtainNewLocalMapTask(taskTracker, numTrackers,
                                 taskTrackerManager.getNumberOfUniqueHosts());
      if (t != null) {
        if (debug) {
          LOG.debug("assigned local task for job " + job.getProfile().getJobID() + 
              " " + taskType(map) );
        }
        assignedTasks.add(t);
        if (map) {
          quota.map_used++;
        } else {
          quota.reduce_used++;
        }
        quota.quota--;
        return true;
      }
      t = job.obtainNewNonLocalMapTask(taskTracker, numTrackers,
          taskTrackerManager.getNumberOfUniqueHosts());
    } else {
      t = job.obtainNewReduceTask(taskTracker, numTrackers,
          taskTrackerManager.getNumberOfUniqueHosts());
    }
    if (t != null) {
      if (debug) {
        LOG.debug("assigned remote task for job " + job.getProfile().getJobID() + 
            " " + taskType(map));
      }
      assignedTasks.add(t);
      if (map) {
        quota.map_used++;
      } else {
        quota.reduce_used++;
      }
      quota.quota--;
      return true;
    }
    return false;
  }

  Map<String,QueueQuota> getQueueQuota(int maxMapTasks, int maxReduceTasks, 
      boolean map) {
    if (debug) {
      LOG.debug("max map tasks " + Integer.toString(maxMapTasks) + " "  + 
          taskType(map));
      LOG.debug("max reduce tasks " + Integer.toString(maxReduceTasks) + " "  + 
          taskType(map));
    }
    int maxTasks = (map) ? maxMapTasks : maxReduceTasks;
    Map<String,QueueAllocation> shares = allocator.getAllocation();
    Map<String,QueueQuota> quotaMap = new HashMap<String,QueueQuota>();
    for (QueueAllocation share: shares.values()) {
      QueueQuota quota = new QueueQuota(share.getName());
      quota.mappers = Math.round(share.getShare() * maxMapTasks);
      quota.reducers = Math.round(share.getShare() * maxReduceTasks);
      quota.quota = (map) ? quota.mappers : quota.reducers;

      if (debug) {
        LOG.debug("queue " + quota.name + " initial quota " + 
            Integer.toString(quota.quota) + " "  + taskType(map));
      }
      quota.map_used = 0;
      quota.reduce_used = 0;
      quota.map_pending = 0;
      quota.reduce_pending = 0;
      Collection<JobInProgress> jobs = getJobs(quota.name);
      for (JobInProgress job : jobs) {
        quota.map_pending += job.pendingMaps();
        quota.reduce_pending += job.pendingReduces();
        int running = (map) ? job.runningMapTasks : job.runningReduceTasks;
        quota.quota -= running;
        quota.map_used += job.runningMapTasks ;
        quota.reduce_used += job.runningReduceTasks;
      }
      if (debug) {
        LOG.debug("queue " + quota.name + " quota " + 
            Integer.toString(quota.quota) + " "  + taskType(map));
      }
      quotaMap.put(quota.name,quota);
    } 
   return quotaMap;
  }

  private void scheduleJobs(int availableSlots, boolean map, boolean fill, 
      TaskTrackerStatus taskTracker, int numTrackers, List<Task> assignedTasks, 
      Map<String,QueueQuota> queueQuota) throws IOException {
    for (int i = 0; i < availableSlots; i++) {
      for (JobInProgress job : jobQueue) {
        if ((job.getStatus().getRunState() != JobStatus.RUNNING) ||
            (!map && job.numReduceTasks == 0)) {
          continue;
        }
        if (assignMapRedTask(job, taskTracker, numTrackers, assignedTasks, 
            queueQuota, fill, map)) {
          break;
        }
      }
    }
  } 

  private int countTasksToKill(Map<String,QueueQuota> queueQuota, boolean map) {
    int killTasks = 0;
    for (QueueQuota quota : queueQuota.values()) {
      killTasks += Math.min((map) ? quota.map_pending : quota.reduce_pending,
          Math.max(quota.quota,0));
    } 
    return killTasks;
  }
   
  protected void markIdle(Map<String, QueueQuota> queueQuota) {
    for (QueueQuota quota: queueQuota.values()) {
      allocator.setUsage(quota.name, Math.min(quota.map_used, quota.mappers) + 
          Math.min(quota.reduce_used, quota.reducers), 
          (quota.map_pending + quota.reduce_pending));
    }  
  }

  private synchronized void assignMapRedTasks(List<Task> assignedTasks, 
      TaskTrackerStatus taskTracker, int numTrackers, boolean map)
      throws IOException {
    int taskOffset = assignedTasks.size();
    int maxTasks = (map) ? taskTracker.getMaxMapSlots() : 
        taskTracker.getMaxReduceSlots();
    int countTasks =  (map) ? taskTracker.countMapTasks() : 
        taskTracker.countReduceTasks();
    int availableSlots = maxTasks - countTasks;
    int map_capacity = 0;
    int reduce_capacity = 0;
    ClusterStatus status = taskTrackerManager.getClusterStatus();
    if (status != null) {
      map_capacity = status.getMaxMapTasks();
      reduce_capacity = status.getMaxReduceTasks();
    }
    Map<String,QueueQuota> queueQuota = getQueueQuota(map_capacity, 
        reduce_capacity,map);
    if (debug) {
      LOG.debug("available slots " + Integer.toString(availableSlots) + " " + 
          taskType(map));
      LOG.debug("queue size " + Integer.toString(jobQueue.size()));
      LOG.debug("map capacity " + Integer.toString(map_capacity) + " " + 
           taskType(map));
      LOG.debug("reduce capacity " + Integer.toString(reduce_capacity) + " " + 
           taskType(map));
    }
    scheduleJobs(availableSlots, map, NO_FILL, taskTracker, numTrackers, 
        assignedTasks, queueQuota);
    availableSlots -= assignedTasks.size() + taskOffset;
    scheduleJobs(availableSlots, map, FILL, taskTracker, numTrackers, 
        assignedTasks, queueQuota);
    if (map) {
      markIdle(queueQuota);
    }

    long currentTime = System.currentTimeMillis()/1000;
    if ((killInterval > 0) && (currentTime - lastKill > killInterval)) {
      lastKill = currentTime;
    } else {
      return;
    }
 
    int killTasks = countTasksToKill(queueQuota, map);
    if (debug) {
      LOG.debug("trying to kill " + Integer.toString(killTasks) + " tasks " + 
          taskType(map));
    }
    killMapRedTasks(killTasks, queueQuota, map);
  }

  class KillQueue {
    String name;
    long startTime;
    QueueQuota quota;
  }

  private Collection<KillQueue> getKillQueues(Map<String, 
      QueueQuota> queueQuota) {
    TreeMap killQueues = new TreeMap(QUEUE_COMPARATOR);
    for (QueueJobs queueJob : queueJobs.values()) {
      QueueQuota quota = queueQuota.get(queueJob.name); 
      if (quota.quota >= 0) {
        continue;
      }
      for (JobInProgress job : queueJob.jobs) {
        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
          KillQueue killQueue = new KillQueue();
          killQueue.name = queueJob.name;
          killQueue.startTime = job.getStartTime();
          killQueue.quota = quota;
          killQueues.put(killQueue, killQueue);
        }
      }
    }
    return killQueues.values();
  }
  private void killMapRedTasks(int killTasks, Map<String,QueueQuota> queueQuota, 
      boolean map) {
    int killed = 0;
    // sort queues  exceeding quota in reverse order of time since starting 
    // a running job
    Collection<KillQueue> killQueues = getKillQueues(queueQuota);
    for (KillQueue killQueue : killQueues) {
      if (killed == killTasks) {
        return;
      }
      QueueQuota quota = killQueue.quota;
      // don't kill more than needed and not more than quota exceeded
      int toKill = Math.min(killTasks-killed,-quota.quota);
      killQueueTasks(quota.name, toKill, map);
      killed += toKill;
    }  
  }

  private String taskType(boolean map) {
    return (map) ? "MAP" : "REDUCE";
  }
  private void killQueueTasks(String queue, int killTasks, boolean map) {
    if (killTasks == 0) {
      return;
    }
    if (debug) {
      LOG.debug("trying to kill " + Integer.toString(killTasks) + 
          " tasks from queue " + queue + " " + taskType(map));
    }
    int killed = 0;
    Collection<JobInProgress> jobs = getJobs(queue); 
    if (debug) {
      LOG.debug("total jobs to kill from " + Integer.toString(jobs.size()) + 
          " " + taskType(map));
    }
    for (JobInProgress job : jobs) {
      TaskInProgress tasks[] = (map) ? job.maps.clone() : 
                                       job.reduces.clone();
      if (sortTasks) {
        Arrays.sort(tasks, TASK_COMPARATOR);
      }
      if (debug) {
        LOG.debug("total tasks to kill from " + 
            Integer.toString(tasks.length) + " " + taskType(map));
      }
      for (int i=0; i < tasks.length; i++) {
        if (debug) {
          LOG.debug("total active tasks to kill from " + 
              Integer.toString(tasks[i].getActiveTasks().keySet().size()) + 
              " " + taskType(map));
        }
        for (TaskAttemptID id: tasks[i].getActiveTasks().keySet()) {
          if (tasks[i].isCommitPending(id)) {
            continue;
          }
          tasks[i].killTask(id, false);
          if (debug) {
            LOG.debug("killed task " + id + " progress " + 
                Double.toString(tasks[i].getProgress()) +
                " start time " + Long.toString(tasks[i].getExecStartTime()) + 
                " " +  taskType(map));
          }
          killed += 1;
          if (killed == killTasks) {
            return;
          }
        }
      }
    }
  }

  @Override
  public List<Task> assignTasks(TaskTracker taskTracker)
    throws IOException {
    long millis = 0;
    if (debug) {
      millis = System.currentTimeMillis();
    } 
    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    int numTrackers = clusterStatus.getTaskTrackers();
 
    List<Task> assignedTasks = new ArrayList<Task>();

    assignMapRedTasks(assignedTasks, taskTracker.getStatus(), numTrackers, MAP);
    assignMapRedTasks(assignedTasks, taskTracker.getStatus(), numTrackers, REDUCE);
    if (debug) {
      long elapsed = System.currentTimeMillis() - millis;
      LOG.debug("assigned total tasks: " + 
          Integer.toString(assignedTasks.size()) + " in " + 
          Long.toString(elapsed) + " ms");
    }
    return assignedTasks;
  }

  @Override
  public Collection<JobInProgress> getJobs(String queueName) {
    QueueJobs jobs = queueJobs.get(queueName);
    if (jobs == null) {
      return new ArrayList<JobInProgress>();
    }
    return jobs.jobs;
  }

  private String getQueue(JobInProgress job) {
    JobConf conf = job.getJobConf();
    return conf.getQueueName();
  }
  private String getUser(JobInProgress job) {
    JobConf conf = job.getJobConf();
    return conf.getUser();
  }
  private String authorize(JobInProgress job) {
    JobConf conf = job.getJobConf();
    String user = conf.getUser();
    String queue = conf.getQueueName();
    if (!user.equals(queue)) {
      return "";
    }
    String timestamp = conf.get("mapred.job.timestamp");
    String signature = conf.get("mapred.job.signature");
    int role = auth.authorize("&user=" + user + "&timestamp=" + timestamp, 
        signature, user, timestamp);
    if (role != PriorityAuthorization.NO_ACCESS) {
      return queue;
    }
    return "";
  }
}
