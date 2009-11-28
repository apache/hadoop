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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 * A {@link TaskScheduler} that 
 * provides the following features: 
 * The purpose of this scheduler is to allow users to increase and decrease
 * their queue priorities continuosly to meet the requirements of their
 * current workloads. The scheduler is aware of the current demand and makes
 * it more expensive to boost the priority under peak usage times. Thus
 * users who move their workload to low usage times are rewarded with
 * discounts. Priorities can only be boosted within a limited quota.
 * All users are given a quota or a budget which is deducted periodically
 * in configurable accounting intervals. How much of the budget is
 * deducted is determined by a per-user spending rate, which may
 * be modified at any time directly by the user. The cluster slots
 * share allocated to a particular user is computed as that users
 * spending rate over the sum of all spending rates in the same accounting
 * period.
 *
 * This scheduler has been designed as a meta-scheduler on top of 
 * existing MapReduce schedulers, which are responsible for enforcing
 * shares computed by the dynamic scheduler in the cluster. 
 */
class DynamicPriorityScheduler extends TaskScheduler {
  /**
   * This class periodically checks spending rates for queues and
   * updates queue capacity shares and budgets
   */
  static class Allocations extends TimerTask implements QueueAllocator {
    Map<String,QueueAllocation> allocation = 
        new HashMap<String,QueueAllocation>();
    Configuration conf;
    HashMap<String,String> queueInfo = new HashMap<String,String>();
    float totalSpending;
    Set<String> infoQueues;
    QueueManager queueManager;
    AllocationStore store;
    Allocations(Configuration conf, QueueManager queueManager) {
      this.conf = conf;
      this.queueManager = queueManager;
      this.infoQueues = queueManager.getLeafQueueNames();
      
      this.store = ReflectionUtils.newInstance(
          conf.getClass(PrioritySchedulerOptions.DYNAMIC_SCHEDULER_STORE,
              FileAllocationStore.class, AllocationStore.class), conf);
      this.store.init(this.conf);
      this.store.load();
    }
    void addBudget(String queue, float budget) {
      store.addBudget(queue, budget);
    }
    void addQueue(String queue) {
      store.addQueue(queue);
    }
    synchronized float getPrice() {
      return totalSpending;
    }
    void removeQueue(String queue) {
      store.removeQueue(queue);
      queueManager.setSchedulerInfo(queue, "");
    }
    void setSpending(String queue, float spending) {
      store.setSpending(queue, spending);
    }
    String getInfo(String queue) {
      return store.getQueueInfo(queue);
    }
    String getQueueInfos() {
      String info = "<price>" + Float.toString(totalSpending) + "</price>\n";
      for (BudgetQueue queue: store.getQueues()) {
        info += "<queue name=\"" + queue.name + "\">" + 
            queueInfo.get(queue.name) + "</queue>\n";
      }
      return info;
    }
    private synchronized void updateAllocation() {
      String queueList = "";
      totalSpending = 0.0f;
      for (BudgetQueue queue: store.getQueues()) {
        if (!infoQueues.contains(queue.name)) {
          infoQueues.add(queue.name);
          QueueInfo newQueueInfo = new QueueInfo(queue.name, null, this); 
          queueManager.setSchedulerInfo(queue.name, newQueueInfo);
        }
        if (!queueList.equals("")) {
          queueList += ",";
        }
        queueList += queue.name;
        // What to include in the published price in spending per slot
        if (queue.spending <= queue.budget && 
            (queue.used != 0 || queue.pending != 0)) {
          totalSpending += queue.spending;
        } 
      } 
      conf.set(PrioritySchedulerOptions.MAPRED_QUEUE_NAMES, queueList);
      setShares();
    }
    // Calculates shares in proportion to spending rates
    // and sets the appropriate configuration parameter
    // for schedulers to read
    private synchronized void setShares() {
      Map<String,QueueAllocation> shares = new HashMap<String,QueueAllocation>();
      for (BudgetQueue queue: store.getQueues()) {
        float spending = queue.spending;
        if (queue.budget < (queue.spending * queue.used) || 
            (queue.used == 0 && queue.pending == 0)) {
          spending = 0.0f;
        } else {
          queue.addBudget(-(queue.spending*queue.used));
        }
        float queueShare = 0.0f;
        if (totalSpending > 0.0f) {
          queueShare = (spending/totalSpending);
        }
        queueInfo.put(queue.name, "<budget>" + Float.toString(queue.budget) + 
            "</budget>\n<spending>" + Float.toString(spending) + "</spending>\n<share>" + 
            Float.toString(queueShare) + "</share>\n<used>" + 
            Integer.toString(queue.used) + "</used>\n<pending>" +
            Integer.toString(queue.pending) + "</pending>\n"); 
        shares.put(queue.name,new QueueAllocation(queue.name,queueShare));
      }
      setAllocation(shares);
    }
    private synchronized void setAllocation(Map<String,QueueAllocation> shares) {
      allocation = shares;
    }
    /** {@inheritDoc} */
    public synchronized Map<String,QueueAllocation> getAllocation() {
      return allocation;
    }
    /** {@inheritDoc} */
    public synchronized void setUsage(String queue, int used, int pending) {
      store.setUsage(queue, used, pending);
    }
    // Used to expose the QueueInfo in the JobTracker web UI 
    synchronized String getQueueInfo(String queue) {
      return queueInfo.get(queue);
    }
    // run once in each allocation interval to 
    // calculate new shares based on updated
    // budgets and spending rates
    @Override
    public void run() {
      store.load();
      updateAllocation();
      store.save();
    }
  }
  /**
   * this class merges the queue info from the underlying
   * MapReduce scheduler and the dynamic scheduler
   * to be displayed in the JobTracker web UI
   */
  private static class QueueInfo {
    String queue;
    Object info; 
    Allocations allocations;
    QueueInfo(String queue, Object info, Allocations allocations) {
      this.queue = queue;
      this.info = info;  
      this.allocations = allocations;
    }
    public String toString() {
      String buffer = "";
      if (info != null) {
        buffer += info.toString();
      }
      String queueInfo = allocations.getQueueInfo(queue);
      buffer += queueInfo;
      return buffer;
    }
  } 

  // this is the actual scheduler that picks
  // the jobs to run, e.g. PriorityScheduler
  protected QueueTaskScheduler scheduler;
  private Timer timer = new Timer(true);
  protected Allocations allocations;
  private static final Log LOG = LogFactory.getLog(DynamicPriorityScheduler.class);

  // Used for testing in discrete time
  void setTimer(Timer timer) {
    this.timer = timer;
  }

  @Override
  public void start() throws IOException {
    Configuration conf = getConf();
    QueueManager queueManager = taskTrackerManager.getQueueManager();
    allocations = new Allocations(conf,queueManager);
    scheduler = ReflectionUtils.newInstance(
        conf.getClass(PrioritySchedulerOptions.DYNAMIC_SCHEDULER_SCHEDULER,
        PriorityScheduler.class, QueueTaskScheduler.class), conf);
    scheduler.setAllocator(allocations);
    scheduler.setConf(conf);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    scheduler.start();
    long interval = conf.getLong(PrioritySchedulerOptions.DYNAMIC_SCHEDULER_ALLOC_INTERVAL,20)*1000;
     
    timer.scheduleAtFixedRate(allocations, interval, interval);   
    for (String queue: queueManager.getLeafQueueNames()) {
      Object info = queueManager.getSchedulerInfo(queue);
      QueueInfo queueInfo = new QueueInfo(queue, info, allocations); 
      queueManager.setSchedulerInfo(queue, queueInfo);
    }
    if (taskTrackerManager instanceof JobTracker) {
      JobTracker jobTracker = (JobTracker) taskTrackerManager;
      HttpServer infoServer = jobTracker.infoServer;
      infoServer.setAttribute("scheduler", this);
      infoServer.addServlet("scheduler", "/scheduler",
          DynamicPriorityServlet.class);
    }
  }

  @Override
  public void terminate() throws IOException {
    scheduler.terminate();
  }

  @Override
  public List<Task> assignTasks(TaskTracker taskTracker)
      throws IOException {
    return scheduler.assignTasks(taskTracker);  
  }

  @Override
  public Collection<JobInProgress> getJobs(String queueName) {
    return scheduler.getJobs(queueName);
  }
}
