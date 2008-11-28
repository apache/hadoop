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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobQueueJobInProgressListener.JobSchedulingInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * This class asynchronously initializes jobs submitted to the
 * Map/Reduce cluster running with the {@link CapacityTaskScheduler}.
 *
 * <p>
 * The class comprises of a main poller thread, and a set of worker
 * threads that together initialize the jobs. The poller thread periodically
 * looks at jobs submitted to the scheduler, and selects a set of them
 * to be initialized. It passes these to the worker threads for initializing.
 * Each worker thread is configured to look at jobs submitted to a fixed
 * set of queues. It initializes jobs in a round robin manner - selecting
 * the first job in order from each queue ready to be initialized.
 * </p>
 * 
 * <p>
 * An initialized job occupies memory resources on the Job Tracker. Hence,
 * the poller limits the number of jobs initialized at any given time to
 * a configured limit. The limit is specified per user per queue.
 * </p>
 * 
 * <p>
 * However, since a job needs to be initialized before the scheduler can
 * select tasks from it to run, it tries to keep a backlog of jobs 
 * initialized so the scheduler does not need to wait and let empty slots
 * go waste. The core logic of the poller is to pick up the right jobs,
 * which have a good potential to be run next by the scheduler. To do this,
 * it picks up jobs submitted across users and across queues to account
 * both for guaranteed capacities and user limits. It also always initializes
 * high priority jobs, whenever they need to be initialized, even if this
 * means going over the limit for initialized jobs.
 * </p>
 */
public class JobInitializationPoller extends Thread {

  private static final Log LOG = LogFactory
      .getLog(JobInitializationPoller.class.getName());

  /*
   * The poller picks up jobs across users to initialize based on user limits.
   * Suppose the user limit for a queue is 25%, it means atmost 4 users' jobs
   * can run together. However, in order to account for jobs from a user that
   * might complete faster than others, it initializes jobs from an additional
   * number of users as a backlog. This variable defines the additional
   * number of users whose jobs can be considered for initializing. 
   */
  private static final int MAX_ADDITIONAL_USERS_TO_INIT = 2;

  private JobQueuesManager jobQueueManager;
  private long sleepInterval;
  private int poolSize;

  /**
   * A worker thread that initializes jobs in one or more queues assigned to
   * it.
   *
   * Jobs are initialized in a round robin fashion one from each queue at a
   * time.
   */
  class JobInitializationThread extends Thread {

    private JobInProgress initializingJob;

    private volatile boolean startIniting;
    private AtomicInteger currentJobCount = new AtomicInteger(0); // number of jobs to initialize

    /**
     * The hash map which maintains relationship between queue to jobs to
     * initialize per queue.
     */
    private HashMap<String, TreeMap<JobSchedulingInfo, JobInProgress>> jobsPerQueue;

    public JobInitializationThread() {
      startIniting = true;
      jobsPerQueue = new HashMap<String, TreeMap<JobSchedulingInfo, JobInProgress>>();
    }

    @Override
    public void run() {
      while (startIniting) {
        initializeJobs();  
        try {
          if (startIniting) {
            Thread.sleep(sleepInterval);
          } else {
            break;
          }
        } catch (Throwable t) {
        }
      }
    }

    // The key method that initializes jobs from queues
    // This method is package-private to allow test cases to call it
    // synchronously in a controlled manner.
    void initializeJobs() {
      // while there are more jobs to initialize...
      while (currentJobCount.get() > 0) {
        Set<String> queues = jobsPerQueue.keySet();
        for (String queue : queues) {
          JobInProgress job = getFirstJobInQueue(queue);
          if (job == null) {
            continue;
          }
          LOG.info("Initializing job : " + job.getJobID() + " in Queue "
              + job.getProfile().getQueueName() + " For user : "
              + job.getProfile().getUser());
          try {
            if (startIniting) {
              setInitializingJob(job);
              job.initTasks();
              setInitializingJob(null);
            } else {
              break;
            }
          } catch (Throwable t) {
            LOG.info("Job initialization failed:\n"
                + StringUtils.stringifyException(t));
            if (job != null)
              job.fail();
          }
        }
      }
    }

    /**
     * This method returns the first job in the queue and removes the same.
     * 
     * @param queue
     *          queue name
     * @return First job in the queue and removes it.
     */
    private JobInProgress getFirstJobInQueue(String queue) {
      TreeMap<JobSchedulingInfo, JobInProgress> jobsList = jobsPerQueue
          .get(queue);
      synchronized (jobsList) {
        if (jobsList.isEmpty()) {
          return null;
        }
        Iterator<JobInProgress> jobIterator = jobsList.values().iterator();
        JobInProgress job = jobIterator.next();
        jobIterator.remove();
        currentJobCount.getAndDecrement();
        return job;
      }
    }

    /*
     * Test method to check if the thread is currently initialising the job
     */
    synchronized JobInProgress getInitializingJob() {
      return this.initializingJob;
    }
    
    synchronized void setInitializingJob(JobInProgress job) {
      this.initializingJob  = job;
    }

    void terminate() {
      startIniting = false;
    }

    void addJobsToQueue(String queue, JobInProgress job) {
      TreeMap<JobSchedulingInfo, JobInProgress> jobs = jobsPerQueue
          .get(queue);
      if (jobs == null) {
        LOG.error("Invalid queue passed to the thread : " + queue
            + " For job :: " + job.getJobID());
      }
      synchronized (jobs) {
        JobSchedulingInfo schedInfo = new JobSchedulingInfo(job);
        jobs.put(schedInfo, job);
        currentJobCount.getAndIncrement();
      }
    }

    void addQueue(String queue) {
      TreeMap<JobSchedulingInfo, JobInProgress> jobs = new TreeMap<JobSchedulingInfo, JobInProgress>(
          jobQueueManager.getComparator(queue));
      jobsPerQueue.put(queue, jobs);
    }
  }

  /**
   * The queue information class maintains following information per queue:
   * Maximum users allowed to initialize job in the particular queue. Maximum
   * jobs allowed to be initialize per user in the queue.
   * 
   */
  private class QueueInfo {
    String queue;
    int maxUsersAllowedToInitialize;
    int maxJobsPerUserToInitialize;

    public QueueInfo(String queue, int maxUsersAllowedToInitialize,
        int maxJobsPerUserToInitialize) {
      this.queue = queue;
      this.maxJobsPerUserToInitialize = maxJobsPerUserToInitialize;
      this.maxUsersAllowedToInitialize = maxUsersAllowedToInitialize;
    }
  }

  /**
   * Map which contains the configuration used for initializing jobs
   * in that associated to a particular job queue.
   */
  private HashMap<String, QueueInfo> jobQueues;

  /**
   * Set of jobs which have been passed to Initialization threads.
   * This is maintained so that we dont call initTasks() for same job twice.
   */
  private HashSet<JobID> initializedJobs;

  private volatile boolean running;

  /**
   * The map which provides information which thread should be used to
   * initialize jobs for a given job queue.
   */
  private HashMap<String, JobInitializationThread> threadsToQueueMap;

  public JobInitializationPoller(JobQueuesManager mgr,
      CapacitySchedulerConf rmConf, Set<String> queue) {
    initializedJobs = new HashSet<JobID>();
    jobQueues = new HashMap<String, QueueInfo>();
    this.jobQueueManager = mgr;
    threadsToQueueMap = new HashMap<String, JobInitializationThread>();
    super.setName("JobInitializationPollerThread");
    running = true;
  }

  /*
   * method to read all configuration values required by the initialisation
   * poller
   */

  void init(Set<String> queues, CapacitySchedulerConf capacityConf) {
    for (String queue : queues) {
      int userlimit = capacityConf.getMinimumUserLimitPercent(queue);
      int maxUsersToInitialize = ((100 / userlimit) + MAX_ADDITIONAL_USERS_TO_INIT);
      int maxJobsPerUserToInitialize = capacityConf
          .getMaxJobsPerUserToInitialize(queue);
      QueueInfo qi = new QueueInfo(queue, maxUsersToInitialize,
          maxJobsPerUserToInitialize);
      jobQueues.put(queue, qi);
    }
    sleepInterval = capacityConf.getSleepInterval();
    poolSize = capacityConf.getMaxWorkerThreads();
    if (poolSize > queues.size()) {
      poolSize = queues.size();
    }
    assignThreadsToQueues();
    Collection<JobInitializationThread> threads = threadsToQueueMap.values();
    for (JobInitializationThread t : threads) {
      if (!t.isAlive()) {
        t.setDaemon(true);
        t.start();
      }
    }
  }

  public void run() {
    while (running) {
      try {
        selectJobsToInitialize();
        if (!this.isInterrupted()) {
          Thread.sleep(sleepInterval);
        }
      } catch (InterruptedException e) {
        LOG.error("Job Initialization poller interrupted"
            + StringUtils.stringifyException(e));
      }
    }
  }

  // The key method that picks up jobs to initialize for each queue.
  // The jobs picked up are added to the worker thread that is handling
  // initialization for that queue.
  // The method is package private to allow tests to call it synchronously
  // in a controlled manner.
  void selectJobsToInitialize() {
    for (String queue : jobQueues.keySet()) {
      ArrayList<JobInProgress> jobsToInitialize = getJobsToInitialize(queue);
      //if (LOG.isDebugEnabled()) {
        printJobs(jobsToInitialize);
      //}
      JobInitializationThread t = threadsToQueueMap.get(queue);
      for (JobInProgress job : jobsToInitialize) {
        t.addJobsToQueue(queue, job);
      }
    }
  }

  private void printJobs(ArrayList<JobInProgress> jobsToInitialize) {
    for (JobInProgress job : jobsToInitialize) {
      LOG.info("Passing to Initializer Job Id :" + job.getJobID()
          + " User: " + job.getProfile().getUser() + " Queue : "
          + job.getProfile().getQueueName());
    }
  }

  // This method exists to be overridden by test cases that wish to
  // create a test-friendly worker thread which can be controlled
  // synchronously.
  JobInitializationThread createJobInitializationThread() {
    return new JobInitializationThread();
  }
  
  private void assignThreadsToQueues() {
    int countOfQueues = jobQueues.size();
    String[] queues = (String[]) jobQueues.keySet().toArray(
        new String[countOfQueues]);
    int numberOfQueuesPerThread = countOfQueues / poolSize;
    int numberOfQueuesAssigned = 0;
    for (int i = 0; i < poolSize; i++) {
      JobInitializationThread initializer = createJobInitializationThread();
      int batch = (i * numberOfQueuesPerThread);
      for (int j = batch; j < (batch + numberOfQueuesPerThread); j++) {
        initializer.addQueue(queues[j]);
        threadsToQueueMap.put(queues[j], initializer);
        numberOfQueuesAssigned++;
      }
    }

    if (numberOfQueuesAssigned < countOfQueues) {
      // Assign remaining queues in round robin fashion to other queues
      int startIndex = 0;
      for (int i = numberOfQueuesAssigned; i < countOfQueues; i++) {
        JobInitializationThread t = threadsToQueueMap
            .get(queues[startIndex]);
        t.addQueue(queues[i]);
        threadsToQueueMap.put(queues[i], t);
        startIndex++;
      }
    }
  }

  /*
   * Select jobs to be initialized for a given queue.
   * 
   * The jobs are selected such that they are within the limits
   * for number of users and number of jobs per user in the queue.
   * The only exception is if high priority jobs are waiting to be
   * initialized. In that case, we could exceed the configured limits.
   * However, we try to restrict the excess to a minimum.
   */
  ArrayList<JobInProgress> getJobsToInitialize(String queue) {
    QueueInfo qi = jobQueues.get(queue);
    ArrayList<JobInProgress> jobsToInitialize = new ArrayList<JobInProgress>();
    // use the configuration parameter which is configured for the particular
    // queue.
    int maximumUsersAllowedToInitialize = qi.maxUsersAllowedToInitialize;
    int maxJobsPerUserAllowedToInitialize = qi.maxJobsPerUserToInitialize;
    // calculate maximum number of jobs which can be allowed to initialize
    // for this queue.
    // This value is used when a user submits a high priority job after we
    // have initialized jobs for that queue and none of them is scheduled.
    // This would prevent us from initializing extra jobs for that particular
    // user. Explanation given at end of method.
    int maxJobsPerQueueToInitialize = maximumUsersAllowedToInitialize
        * maxJobsPerUserAllowedToInitialize;
    Collection<JobInProgress> jobs = jobQueueManager.getJobs(queue);
    int countOfJobsInitialized = 0;
    HashMap<String, Integer> userJobsInitialized = new HashMap<String, Integer>();
    for (JobInProgress job : jobs) {
      /*
       * First check if job has been scheduled or completed or killed. If so
       * then remove from uninitialised jobs. Remove from Job queue
       */
      if ((job.getStatus().getRunState() == JobStatus.RUNNING)
          && (job.runningMaps() > 0 || job.runningReduces() > 0
              || job.finishedMaps() > 0 || job.finishedReduces() > 0)) {
        LOG.debug("Removing from the queue " + job.getJobID());
        initializedJobs.remove(job.getJobID());
        jobQueueManager.removeJobFromQueue(job);
        continue;
      } else if (job.isComplete()) {
        LOG.debug("Removing from completed job from " + "the queue "
            + job.getJobID());
        initializedJobs.remove(job.getJobID());
        jobQueueManager.removeJobFromQueue(job);
        continue;
      }
      String user = job.getProfile().getUser();
      int numberOfJobs = userJobsInitialized.get(user) == null ? 0
          : userJobsInitialized.get(user);
      // If the job is already initialized then add the count against user
      // then continue.
      if (initializedJobs.contains(job.getJobID())) {
        userJobsInitialized.put(user, Integer.valueOf(numberOfJobs + 1));
        countOfJobsInitialized++;
        continue;
      }
      boolean isUserPresent = userJobsInitialized.containsKey(user);
      /*
       * If the user is present in user list and size of user list is less
       * maximum allowed users initialize then initialize this job and add this
       * user to the global list.
       * 
       * Else if he is present we check if his number of jobs has not crossed
       * his quota and global quota.
       * 
       * The logic behind using a global per queue job can be understood by example
       * below: Consider 3 users submitting normal priority job in a job queue with
       * user limit as 100. (Max jobs per user = 2)
       * 
       * U1J1,U1J2,U1J3....,U3J3.
       * 
       * Jobs initialized would be
       * 
       * U1J1,U1J2,U2J1,U2J2,U3J1,U3J2
       * 
       * Now consider a case where U4 comes in and submits a high priority job.
       * 
       * U4J1 --- High Priority JOb, U4J2---- Normal priority job.
       * 
       * So, if we dont use global per queue value we would end up initializing both
       * U4 jobs which is not correct.
       * 
       * By using a global value we ensure that we dont initialize any extra jobs
       * for a user.
       */
      if (!isUserPresent
          && userJobsInitialized.size() < maximumUsersAllowedToInitialize) {
        // this is a new user being considered and the number of users
        // is within limits.
        userJobsInitialized.put(user, Integer.valueOf(numberOfJobs + 1));
        jobsToInitialize.add(job);
        initializedJobs.add(job.getJobID());
        countOfJobsInitialized++;
      } else if (isUserPresent
          && numberOfJobs < maxJobsPerUserAllowedToInitialize
          && countOfJobsInitialized < maxJobsPerQueueToInitialize) {
        /*
         * this is an existing user and the number of jobs per user
         * is within limits, as also the number of jobs per queue.
         * We need the check on number of jobs per queue to restrict
         * the number of jobs we initialize over the limit due to high
         * priority jobs.
         * 
         * For e.g Consider 3 users submitting normal priority job in 
         * a job queue with user limit as 100 and max jobs per user as 2
         * Say the jobs are U1J1,U1J2,U1J3....,U3J3.
         * 
         * Jobs initialized would be U1J1,U1J2,U2J1,U2J2,U3J1,U3J2
         * 
         * Now consider a case where U4 comes in and submits a high priority job
         * and a normal priority job. Say U4J1 and U4J2
         * 
         * If we dont consider the number of jobs per queue we would end up 
         * initializing both jobs from U4. Initializing the second job is
         * unnecessary.
         */
        userJobsInitialized.put(user, Integer.valueOf(numberOfJobs + 1));
        jobsToInitialize.add(job);
        initializedJobs.add(job.getJobID());
        countOfJobsInitialized++;
      }
    }
    return jobsToInitialize;
  }


  void terminate() {
    running = false;
    for (Entry<String, JobInitializationThread> entry : threadsToQueueMap
        .entrySet()) {
      JobInitializationThread t = entry.getValue();
      if (t.isAlive()) {
        t.terminate();
        t.interrupt();
      }
    }
  }

  /*
   * Test method used only for testing purposes.
   */
  JobInProgress getInitializingJob(String queue) {
    JobInitializationThread t = threadsToQueueMap.get(queue);
    if (t == null) {
      return null;
    } else {
      return t.getInitializingJob();
    }
  }

  HashSet<JobID> getInitializedJobList() {
    return initializedJobs;
  }
}
