/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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


import org.apache.commons.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;

import java.io.*;
import java.net.*;
import java.text.NumberFormat;
import java.util.*;

/*******************************************************
 * JobTracker is the central location for submitting and 
 * tracking MR jobs in a network environment.
 *
 * @author Mike Cafarella
 *******************************************************/
public class JobTracker implements MRConstants, InterTrackerProtocol, JobSubmissionProtocol {
    static long JOBINIT_SLEEP_INTERVAL = 2000;
    static long RETIRE_JOB_INTERVAL;
    static long RETIRE_JOB_CHECK_INTERVAL;
    static float TASK_ALLOC_EPSILON;
    static float PAD_FRACTION;
    static final int MIN_CLUSTER_SIZE_FOR_PADDING = 3;

    /**
     * Used for formatting the id numbers
     */
    private static NumberFormat idFormat = NumberFormat.getInstance();
    static {
      idFormat.setMinimumIntegerDigits(4);
      idFormat.setGroupingUsed(false);
    }

    private int nextJobId = 1;

    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.mapred.JobTracker");

    private static JobTracker tracker = null;
    private static boolean runTracker = true;
    public static void startTracker(Configuration conf) throws IOException {
      if (tracker != null)
        throw new IOException("JobTracker already running.");
      runTracker = true;
      while (runTracker) {
        try {
          tracker = new JobTracker(conf);
          break;
        } catch (IOException e) {
          LOG.warn("Starting tracker", e);
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
      if (runTracker) { tracker.offerService(); }
    }

    public static JobTracker getTracker() {
        return tracker;
    }

    public static void stopTracker() throws IOException {
      if (tracker == null)
        throw new IOException("Trying to stop JobTracker that is not running.");
      runTracker = false;
      tracker.close();
      tracker = null;
    }
    
    public long getProtocolVersion(String protocol, long clientVersion) {
      if (protocol.equals(InterTrackerProtocol.class.getName())) {
        return InterTrackerProtocol.versionID;
      } else {
        return JobSubmissionProtocol.versionID;
      }
    }
    /**
     * A thread to timeout tasks that have been assigned to task trackers,
     * but that haven't reported back yet.
     * Note that I included a stop() method, even though there is no place
     * where JobTrackers are cleaned up.
     * @author Owen O'Malley
     */
    private class ExpireLaunchingTasks implements Runnable {
      private volatile boolean shouldRun = true;
      /**
       * This is a map of the tasks that have been assigned to task trackers,
       * but that have not yet been seen in a status report.
       * map: task-id (String) -> time-assigned (Long)
       */
      private Map launchingTasks = new LinkedHashMap();
      
      public void run() {
        try {
          while (shouldRun) {
            // Every 3 minutes check for any tasks that are overdue
            Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL/3);
            long now = System.currentTimeMillis();
            LOG.debug("Starting launching task sweep");
            synchronized (JobTracker.this) {
              synchronized (launchingTasks) {
                Iterator itr = launchingTasks.entrySet().iterator();
                while (itr.hasNext()) {
                  Map.Entry pair = (Map.Entry) itr.next();
                  String taskId = (String) pair.getKey();
                  long age = now - ((Long) pair.getValue()).longValue();
                  LOG.info(taskId + " is " + age + " ms debug.");
                  if (age > TASKTRACKER_EXPIRY_INTERVAL) {
                    LOG.info("Launching task " + taskId + " timed out.");
                    TaskInProgress tip = null;
                    tip = (TaskInProgress) taskidToTIPMap.get(taskId);
                    if (tip != null) {
                      JobInProgress job = tip.getJob();
                      String trackerName = getAssignedTracker(taskId);
                      TaskTrackerStatus trackerStatus = 
                        getTaskTracker(trackerName);
                      job.failedTask(tip, taskId, "Error launching task", 
                                     trackerStatus.getHost(), trackerName);
                    }
                    itr.remove();
                  } else {
                    // the tasks are sorted by start time, so once we find
                    // one that we want to keep, we are done for this cycle.
                    break;
                  }
                }
              }
            }
          }
        } catch (InterruptedException ie) {
          // all done
        }
      }
      
      public void addNewTask(String taskName) {
        synchronized (launchingTasks) {
          launchingTasks.put(taskName, 
                             new Long(System.currentTimeMillis()));
        }
      }
      
      public void removeTask(String taskName) {
        synchronized (launchingTasks) {
          launchingTasks.remove(taskName);
        }
      }
      
      public void stop() {
        shouldRun = false;
      }
    }
    
    ///////////////////////////////////////////////////////
    // Used to expire TaskTrackers that have gone down
    ///////////////////////////////////////////////////////
    class ExpireTrackers implements Runnable {
        boolean shouldRun = true;
        public ExpireTrackers() {
        }
        /**
         * The run method lives for the life of the JobTracker, and removes TaskTrackers
         * that have not checked in for some time.
         */
        public void run() {
            while (shouldRun) {
                //
                // Thread runs periodically to check whether trackers should be expired.
                // The sleep interval must be no more than half the maximum expiry time
                // for a task tracker.
                //
                try {
                    Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL / 3);
                } catch (InterruptedException ie) {
                }

                //
                // Loop through all expired items in the queue
                //
                synchronized (taskTrackers) {
                    synchronized (trackerExpiryQueue) {
                        long now = System.currentTimeMillis();
                        TaskTrackerStatus leastRecent = null;
                        while ((trackerExpiryQueue.size() > 0) &&
                               ((leastRecent = (TaskTrackerStatus) trackerExpiryQueue.first()) != null) &&
                               (now - leastRecent.getLastSeen() > TASKTRACKER_EXPIRY_INTERVAL)) {

                            // Remove profile from head of queue
                            trackerExpiryQueue.remove(leastRecent);
                            String trackerName = leastRecent.getTrackerName();

                            // Figure out if last-seen time should be updated, or if tracker is dead
                            TaskTrackerStatus newProfile = (TaskTrackerStatus) taskTrackers.get(leastRecent.getTrackerName());
                            // Items might leave the taskTracker set through other means; the
                            // status stored in 'taskTrackers' might be null, which means the
                            // tracker has already been destroyed.
                            if (newProfile != null) {
                                if (now - newProfile.getLastSeen() > TASKTRACKER_EXPIRY_INTERVAL) {
                                    // Remove completely
                                    updateTaskTrackerStatus(trackerName, null);
                                    lostTaskTracker(leastRecent.getTrackerName(),
                                                    leastRecent.getHost());
                                } else {
                                    // Update time by inserting latest profile
                                    trackerExpiryQueue.add(newProfile);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        /**
         * Stop the tracker on next iteration
         */
        public void stopTracker() {
            shouldRun = false;
        }
    }

    ///////////////////////////////////////////////////////
    // Used to remove old finished Jobs that have been around for too long
    ///////////////////////////////////////////////////////
    class RetireJobs implements Runnable {
        boolean shouldRun = true;
        public RetireJobs() {
        }

        /**
         * The run method lives for the life of the JobTracker,
         * and removes Jobs that are not still running, but which
         * finished a long time ago.
         */
        public void run() {
            while (shouldRun) {
                try {
                    Thread.sleep(RETIRE_JOB_CHECK_INTERVAL);
                } catch (InterruptedException ie) {
                }
                
                synchronized (jobs) {
                    synchronized (jobsByArrival) {
                        synchronized (jobInitQueue) {
                            for (Iterator it = jobs.keySet().iterator(); it.hasNext(); ) {
                                String jobid = (String) it.next();
                                JobInProgress job = (JobInProgress) jobs.get(jobid);

                                if (job.getStatus().getRunState() != JobStatus.RUNNING &&
                                    job.getStatus().getRunState() != JobStatus.PREP &&
                                    (job.getFinishTime() + RETIRE_JOB_INTERVAL < System.currentTimeMillis())) {
                                    it.remove();
                            
                                    jobInitQueue.remove(job);
                                    jobsByArrival.remove(job);
                                }
                            }
                        }
                    }
                }
            }
        }
        public void stopRetirer() {
            shouldRun = false;
        }
    }

    /////////////////////////////////////////////////////////////////
    //  Used to init new jobs that have just been created
    /////////////////////////////////////////////////////////////////
    class JobInitThread implements Runnable {
        boolean shouldRun = true;
        public JobInitThread() {
        }
        public void run() {
            while (shouldRun) {
                JobInProgress job = null;
                synchronized (jobInitQueue) {
                    if (jobInitQueue.size() > 0) {
                        job = (JobInProgress) jobInitQueue.elementAt(0);
                        jobInitQueue.remove(job);
                    } else {
                        try {
                            jobInitQueue.wait(JOBINIT_SLEEP_INTERVAL);
                        } catch (InterruptedException iex) {
                        }
                    }
                }
                try {
                    if (job != null) {
                        job.initTasks();
                    }
                } catch (Exception e) {
                    LOG.warn("job init failed", e);
                    job.kill();
                }
            }
        }
        public void stopIniter() {
            shouldRun = false;
        }
    }


    /////////////////////////////////////////////////////////////////
    // The real JobTracker
    ////////////////////////////////////////////////////////////////
    int port;
    String localMachine;
    long startTime;
    int totalSubmissions = 0;
    Random r = new Random();

    private int maxCurrentTasks;

    //
    // Properties to maintain while running Jobs and Tasks:
    //
    // 1.  Each Task is always contained in a single Job.  A Job succeeds when all its 
    //     Tasks are complete.
    //
    // 2.  Every running or successful Task is assigned to a Tracker.  Idle Tasks are not.
    //
    // 3.  When a Tracker fails, all of its assigned Tasks are marked as failures.
    //
    // 4.  A Task might need to be reexecuted if it (or the machine it's hosted on) fails
    //     before the Job is 100% complete.  Sometimes an upstream Task can fail without
    //     reexecution if all downstream Tasks that require its output have already obtained
    //     the necessary files.
    //

    // All the known jobs.  (jobid->JobInProgress)
    TreeMap jobs = new TreeMap();
    Vector jobsByArrival = new Vector();

    // All the known TaskInProgress items, mapped to by taskids (taskid->TIP)
    TreeMap taskidToTIPMap = new TreeMap();

    // (taskid --> trackerID) 
    TreeMap taskidToTrackerMap = new TreeMap();

    // (trackerID->TreeSet of taskids running at that tracker)
    TreeMap trackerToTaskMap = new TreeMap();

    //
    // Watch and expire TaskTracker objects using these structures.
    // We can map from Name->TaskTrackerStatus, or we can expire by time.
    //
    int totalMaps = 0;
    int totalReduces = 0;
    private TreeMap taskTrackers = new TreeMap();
    Vector jobInitQueue = new Vector();
    ExpireTrackers expireTrackers = new ExpireTrackers();
    Thread expireTrackersThread = null;
    RetireJobs retireJobs = new RetireJobs();
    Thread retireJobsThread = null;
    JobInitThread initJobs = new JobInitThread();
    Thread initJobsThread = null;
    ExpireLaunchingTasks expireLaunchingTasks = new ExpireLaunchingTasks();
    Thread expireLaunchingTaskThread = new Thread(expireLaunchingTasks);
    
    /**
     * It might seem like a bug to maintain a TreeSet of status objects,
     * which can be updated at any time.  But that's not what happens!  We
     * only update status objects in the taskTrackers table.  Status objects
     * are never updated once they enter the expiry queue.  Instead, we wait
     * for them to expire and remove them from the expiry queue.  If a status
     * object has been updated in the taskTracker table, the latest status is 
     * reinserted.  Otherwise, we assume the tracker has expired.
     */
    TreeSet trackerExpiryQueue = new TreeSet(new Comparator() {
        public int compare(Object o1, Object o2) {
            TaskTrackerStatus p1 = (TaskTrackerStatus) o1;
            TaskTrackerStatus p2 = (TaskTrackerStatus) o2;
            if (p1.getLastSeen() < p2.getLastSeen()) {
                return -1;
            } else if (p1.getLastSeen() > p2.getLastSeen()) {
                return 1;
            } else {
                return (p1.getTrackerName().compareTo(p2.getTrackerName()));
            }
        }
    });

    // Used to provide an HTML view on Job, Task, and TaskTracker structures
    StatusHttpServer infoServer;
    int infoPort;

    Server interTrackerServer;

    // Some jobs are stored in a local system directory.  We can delete
    // the files when we're done with the job.
    static final String SUBDIR = "jobTracker";
    FileSystem fs;
    Path systemDir;
    private Configuration conf;

    /**
     * Start the JobTracker process, listen on the indicated port
     */
    JobTracker(Configuration conf) throws IOException {
        //
        // Grab some static constants
        //
        maxCurrentTasks = conf.getInt("mapred.tasktracker.tasks.maximum", 2);
        RETIRE_JOB_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.interval", 24 * 60 * 60 * 1000);
        RETIRE_JOB_CHECK_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.check", 60 * 1000);
        TASK_ALLOC_EPSILON = conf.getFloat("mapred.jobtracker.taskalloc.loadbalance.epsilon", 0.2f);
        PAD_FRACTION = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 
                                     0.01f);

        // This is a directory of temporary submission files.  We delete it
        // on startup, and can delete any files that we're done with
        this.conf = conf;
        JobConf jobConf = new JobConf(conf);
        this.systemDir = jobConf.getSystemDir();
        this.fs = FileSystem.get(conf);
        fs.delete(systemDir);
        fs.mkdirs(systemDir);

        // Same with 'localDir' except it's always on the local disk.
        jobConf.deleteLocalFiles(SUBDIR);

        // Set ports, start RPC servers, etc.
        InetSocketAddress addr = getAddress(conf);
        this.localMachine = addr.getHostName();
        this.port = addr.getPort();
        this.interTrackerServer = RPC.getServer(this, addr.getPort(), 10, false, conf);
        this.interTrackerServer.start();
        Properties p = System.getProperties();
        for (Iterator it = p.keySet().iterator(); it.hasNext(); ) {
          String key = (String) it.next();
          String val = (String) p.getProperty(key);
          LOG.info("Property '" + key + "' is " + val);
        }

        this.infoPort = conf.getInt("mapred.job.tracker.info.port", 50030);
        this.infoServer = new StatusHttpServer("job", infoPort, false);
        this.infoServer.start();

        this.startTime = System.currentTimeMillis();

        this.expireTrackersThread = new Thread(this.expireTrackers);
        this.expireTrackersThread.start();
        this.retireJobsThread = new Thread(this.retireJobs);
        this.retireJobsThread.start();
        this.initJobsThread = new Thread(this.initJobs);
        this.initJobsThread.start();
        expireLaunchingTaskThread.start();
    }

    public static InetSocketAddress getAddress(Configuration conf) {
      String jobTrackerStr =
        conf.get("mapred.job.tracker", "localhost:8012");
      int colon = jobTrackerStr.indexOf(":");
      if (colon < 0) {
        throw new RuntimeException("Bad mapred.job.tracker: "+jobTrackerStr);
      }
      String jobTrackerName = jobTrackerStr.substring(0, colon);
      int jobTrackerPort = Integer.parseInt(jobTrackerStr.substring(colon+1));
      return new InetSocketAddress(jobTrackerName, jobTrackerPort);
    }


    /**
     * Run forever
     */
    public void offerService() {
        try {
            this.interTrackerServer.join();
        } catch (InterruptedException ie) {
        }
        LOG.info("Stopped interTrackerServer");
    }

    void close() throws IOException {
        if (this.infoServer != null) {
            LOG.info("Stopping infoServer");
            try {
                this.infoServer.stop();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        if (this.interTrackerServer != null) {
            LOG.info("Stopping interTrackerServer");
            this.interTrackerServer.stop();
        }
        if (this.expireTrackers != null) {
            LOG.info("Stopping expireTrackers");
            this.expireTrackers.stopTracker();
            try {
                this.expireTrackersThread.interrupt();
                this.expireTrackersThread.join();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        if (this.retireJobs != null) {
            LOG.info("Stopping retirer");
            this.retireJobs.stopRetirer();
            try {
                this.retireJobsThread.interrupt();
                this.retireJobsThread.join();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        if (this.initJobs != null) {
            LOG.info("Stopping initer");
            this.initJobs.stopIniter();
            try {
                this.initJobsThread.interrupt();
                this.initJobsThread.join();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        if (this.expireLaunchingTaskThread != null) {
            LOG.info("Stopping expireLaunchingTasks");
            this.expireLaunchingTasks.stop();
            try {
                this.expireLaunchingTaskThread.interrupt();
                this.expireLaunchingTaskThread.join();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        LOG.info("stopped all jobtracker services");
        return;
    }
    
    ///////////////////////////////////////////////////////
    // Maintain lookup tables; called by JobInProgress
    // and TaskInProgress
    ///////////////////////////////////////////////////////
    void createTaskEntry(String taskid, String taskTracker, TaskInProgress tip) {
        LOG.info("Adding task '" + taskid + "' to tip " + tip.getTIPId() + ", for tracker '" + taskTracker + "'");

        // taskid --> tracker
        taskidToTrackerMap.put(taskid, taskTracker);

        // tracker --> taskid
        TreeSet taskset = (TreeSet) trackerToTaskMap.get(taskTracker);
        if (taskset == null) {
            taskset = new TreeSet();
            trackerToTaskMap.put(taskTracker, taskset);
        }
        taskset.add(taskid);

        // taskid --> TIP
        taskidToTIPMap.put(taskid, tip);
    }
    void removeTaskEntry(String taskid) {
        // taskid --> tracker
        String tracker = (String) taskidToTrackerMap.remove(taskid);

        // tracker --> taskid
        TreeSet trackerSet = (TreeSet) trackerToTaskMap.get(tracker);
        if (trackerSet != null) {
            trackerSet.remove(taskid);
        }

        // taskid --> TIP
        taskidToTIPMap.remove(taskid);
    }

    ///////////////////////////////////////////////////////
    // Accessors for objects that want info on jobs, tasks,
    // trackers, etc.
    ///////////////////////////////////////////////////////
    public int getTotalSubmissions() {
        return totalSubmissions;
    }
    public String getJobTrackerMachine() {
        return localMachine;
    }
    public int getTrackerPort() {
        return port;
    }
    public int getInfoPort() {
        return infoPort;
    }
    public long getStartTime() {
        return startTime;
    }
    public Vector runningJobs() {
        Vector v = new Vector();
        for (Iterator it = jobs.values().iterator(); it.hasNext(); ) {
            JobInProgress jip = (JobInProgress) it.next();
            JobStatus status = jip.getStatus();
            if (status.getRunState() == JobStatus.RUNNING) {
                v.add(jip);
            }
        }
        return v;
    }
    public Vector failedJobs() {
        Vector v = new Vector();
        for (Iterator it = jobs.values().iterator(); it.hasNext(); ) {
            JobInProgress jip = (JobInProgress) it.next();
            JobStatus status = jip.getStatus();
            if (status.getRunState() == JobStatus.FAILED) {
                v.add(jip);
            }
        }
        return v;
    }
    public Vector completedJobs() {
        Vector v = new Vector();
        for (Iterator it = jobs.values().iterator(); it.hasNext(); ) {
            JobInProgress jip = (JobInProgress) it.next();
            JobStatus status = jip.getStatus();
            if (status.getRunState() == JobStatus.SUCCEEDED) {
                v.add(jip);
            }
        }
        return v;
    }
    public Collection taskTrackers() {
      synchronized (taskTrackers) {
        return taskTrackers.values();
      }
    }
    public TaskTrackerStatus getTaskTracker(String trackerID) {
      synchronized (taskTrackers) {
        return (TaskTrackerStatus) taskTrackers.get(trackerID);
      }
    }

    ////////////////////////////////////////////////////
    // InterTrackerProtocol
    ////////////////////////////////////////////////////

    /**
     * Update the last recorded status for the given task tracker.
     * It assumes that the taskTrackers are locked on entry.
     * @author Owen O'Malley
     * @param trackerName The name of the tracker
     * @param status The new status for the task tracker
     * @return Was an old status found?
     */
    private boolean updateTaskTrackerStatus(String trackerName,
                                            TaskTrackerStatus status) {
      TaskTrackerStatus oldStatus = 
        (TaskTrackerStatus) taskTrackers.get(trackerName);
      if (oldStatus != null) {
        totalMaps -= oldStatus.countMapTasks();
        totalReduces -= oldStatus.countReduceTasks();
        if (status == null) {
          taskTrackers.remove(trackerName);
        }
      }
      if (status != null) {
        totalMaps += status.countMapTasks();
        totalReduces += status.countReduceTasks();
        taskTrackers.put(trackerName, status);
      }
      return oldStatus != null;
    }
    
    /**
     * Process incoming heartbeat messages from the task trackers.
     */
    public synchronized int emitHeartbeat(TaskTrackerStatus trackerStatus, boolean initialContact) {
        String trackerName = trackerStatus.getTrackerName();
        trackerStatus.setLastSeen(System.currentTimeMillis());

        synchronized (taskTrackers) {
            synchronized (trackerExpiryQueue) {
                boolean seenBefore = updateTaskTrackerStatus(trackerName,
                                                             trackerStatus);
                if (initialContact) {
                    // If it's first contact, then clear out any state hanging around
                    if (seenBefore) {
                        lostTaskTracker(trackerName, trackerStatus.getHost());
                    }
                } else {
                    // If not first contact, there should be some record of the tracker
                    if (!seenBefore) {
                        return InterTrackerProtocol.UNKNOWN_TASKTRACKER;
                    }
                }

                if (initialContact) {
                    trackerExpiryQueue.add(trackerStatus);
                }
            }
        }

        updateTaskStatuses(trackerStatus);
        //LOG.info("Got heartbeat from "+trackerName);
        return InterTrackerProtocol.TRACKERS_OK;
    }

    /**
     * A tracker wants to know if there's a Task to run.  Returns
     * a task we'd like the TaskTracker to execute right now.
     *
     * Eventually this function should compute load on the various TaskTrackers,
     * and incorporate knowledge of DFS file placement.  But for right now, it
     * just grabs a single item out of the pending task list and hands it back.
     */
    public synchronized Task pollForNewTask(String taskTracker) {
        //
        // Compute average map and reduce task numbers across pool
        //
        int remainingReduceLoad = 0;
        int remainingMapLoad = 0;
        int numTaskTrackers;
        TaskTrackerStatus tts;
        int avgMapLoad = 0;
        int avgReduceLoad = 0;
	
        synchronized (taskTrackers) {
          numTaskTrackers = taskTrackers.size();
          tts = (TaskTrackerStatus) taskTrackers.get(taskTracker);
        }
        synchronized(jobsByArrival){
            for (Iterator it = jobsByArrival.iterator(); it.hasNext(); ) {
                    JobInProgress job = (JobInProgress) it.next();
                    if (job.getStatus().getRunState() == JobStatus.RUNNING) {
                         int totalMapTasks = job.desiredMaps();
                         int totalReduceTasks = job.desiredReduces();
                         remainingMapLoad += (totalMapTasks - job.finishedMaps());
                         remainingReduceLoad += (totalReduceTasks - job.finishedReduces());
                    }
            }   
        }
        
        if (numTaskTrackers > 0) {
          avgMapLoad = remainingMapLoad / numTaskTrackers;
          avgReduceLoad = remainingReduceLoad / numTaskTrackers;
        }
        int totalCapacity = numTaskTrackers * maxCurrentTasks;
        //
        // Get map + reduce counts for the current tracker.
        //
        if (tts == null) {
          LOG.warn("Unknown task tracker polling; ignoring: " + taskTracker);
          return null;
        }

        int numMaps = tts.countMapTasks();
        int numReduces = tts.countReduceTasks();

        //
        // In the below steps, we allocate first a map task (if appropriate),
        // and then a reduce task if appropriate.  We go through all jobs
        // in order of job arrival; jobs only get serviced if their 
        // predecessors are serviced, too.
        //

        //
        // We hand a task to the current taskTracker if the given machine 
        // has a workload that's equal to or less than the pendingMaps average.
        // This way the maps are launched if the TaskTracker has running tasks 
        // less than the pending average 
        // +/- TASK_ALLOC_EPSILON.  (That epsilon is in place in case
        // there is an odd machine that is failing for some reason but 
        // has not yet been removed from the pool, making capacity seem
        // larger than it really is.)
        //
       
        synchronized (jobsByArrival) {
            if ((numMaps < maxCurrentTasks) &&
                (numMaps <= avgMapLoad + 1 + TASK_ALLOC_EPSILON)) {

                int totalNeededMaps = 0;
                for (Iterator it = jobsByArrival.iterator(); it.hasNext(); ) {
                    JobInProgress job = (JobInProgress) it.next();
                    if (job.getStatus().getRunState() != JobStatus.RUNNING) {
                        continue;
                    }

                    Task t = job.obtainNewMapTask(taskTracker, tts);
                    if (t != null) {
                      expireLaunchingTasks.addNewTask(t.getTaskId());
                      return t;
                    }

                    //
                    // Beyond the highest-priority task, reserve a little 
                    // room for failures and speculative executions; don't 
                    // schedule tasks to the hilt.
                    //
                    totalNeededMaps += job.desiredMaps();
                    int padding = 0;
                    if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
                      padding = Math.min(maxCurrentTasks,
                                         (int)(totalNeededMaps * PAD_FRACTION));
                    }
                    if (totalMaps + padding >= totalCapacity) {
                        break;
                    }
                }
            }

            //
            // Same thing, but for reduce tasks
            //
            if ((numReduces < maxCurrentTasks) &&
                (numReduces <= avgReduceLoad + 1 + TASK_ALLOC_EPSILON)) {

                int totalNeededReduces = 0;
                for (Iterator it = jobsByArrival.iterator(); it.hasNext(); ) {
                    JobInProgress job = (JobInProgress) it.next();
                    if (job.getStatus().getRunState() != JobStatus.RUNNING) {
                        continue;
                    }

                    Task t = job.obtainNewReduceTask(taskTracker, tts);
                    if (t != null) {
                      expireLaunchingTasks.addNewTask(t.getTaskId());
                      return t;
                    }

                    //
                    // Beyond the highest-priority task, reserve a little 
                    // room for failures and speculative executions; don't 
                    // schedule tasks to the hilt.
                    //
                    totalNeededReduces += job.desiredReduces();
                    int padding = 0;
                    if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
                        padding = 
                          Math.min(maxCurrentTasks,
                                   (int) (totalNeededReduces * PAD_FRACTION));
                    }
                    if (totalReduces + padding >= totalCapacity) {
                        break;
                    }
                }
            }
        }
        return null;
    }

    /**
     * A tracker wants to know if any of its Tasks have been
     * closed (because the job completed, whether successfully or not)
     */
    public synchronized String[] pollForTaskWithClosedJob(String taskTracker) {
        TreeSet taskIds = (TreeSet) trackerToTaskMap.get(taskTracker);
        if (taskIds != null) {
            ArrayList list = new ArrayList();
            for (Iterator it = taskIds.iterator(); it.hasNext(); ) {
                String taskId = (String) it.next();
                TaskInProgress tip = (TaskInProgress) taskidToTIPMap.get(taskId);
                if (tip.shouldCloseForClosedJob(taskId)) {
                    // 
                    // This is how the JobTracker ends a task at the TaskTracker.
                    // It may be successfully completed, or may be killed in
                    // mid-execution.
                    //
                   list.add(taskId);
                }
            }
            return (String[]) list.toArray(new String[list.size()]);
        }
        return null;
    }

    /**
     * A TaskTracker wants to know the physical locations of completed, but not
     * yet closed, tasks.  This exists so the reduce task thread can locate
     * map task outputs.
     */
    public synchronized MapOutputLocation[] 
             locateMapOutputs(String jobId, int[] mapTasksNeeded, int reduce) {
        ArrayList result = new ArrayList(mapTasksNeeded.length);
        JobInProgress job = getJob(jobId);
        for (int i = 0; i < mapTasksNeeded.length; i++) {
          TaskStatus status = job.findFinishedMap(mapTasksNeeded[i]);
          if (status != null) {
             String trackerId = 
               (String) taskidToTrackerMap.get(status.getTaskId());
             TaskTrackerStatus tracker;
             synchronized (taskTrackers) {
               tracker = (TaskTrackerStatus) taskTrackers.get(trackerId);
             }
             result.add(new MapOutputLocation(status.getTaskId(), 
                                              mapTasksNeeded[i],
                                              tracker.getHost(), 
                                              tracker.getHttpPort()));
          }
        }
        return (MapOutputLocation[]) 
               result.toArray(new MapOutputLocation[result.size()]);
    }

    /**
     * Grab the local fs name
     */
    public synchronized String getFilesystemName() throws IOException {
        return fs.getName();
    }


    public void reportTaskTrackerError(String taskTracker,
            String errorClass,
            String errorMessage) throws IOException {
        LOG.warn("Report from " + taskTracker + ": " + errorMessage);        
    }

    ////////////////////////////////////////////////////
    // JobSubmissionProtocol
    ////////////////////////////////////////////////////
    /**
     * JobTracker.submitJob() kicks off a new job.  
     *
     * Create a 'JobInProgress' object, which contains both JobProfile
     * and JobStatus.  Those two sub-objects are sometimes shipped outside
     * of the JobTracker.  But JobInProgress adds info that's useful for
     * the JobTracker alone.
     *
     * We add the JIP to the jobInitQueue, which is processed 
     * asynchronously to handle split-computation and build up
     * the right TaskTracker/Block mapping.
     */
    public synchronized JobStatus submitJob(String jobFile) throws IOException {
        totalSubmissions++;
        JobInProgress job = new JobInProgress(jobFile, this, this.conf);
        synchronized (jobs) {
            synchronized (jobsByArrival) {
                synchronized (jobInitQueue) {
                    jobs.put(job.getProfile().getJobId(), job);
                    jobsByArrival.add(job);
                    jobInitQueue.add(job);
                    jobInitQueue.notifyAll();
                }
            }
        }
        return job.getStatus();
    }

    public synchronized ClusterStatus getClusterStatus() {
        synchronized (taskTrackers) {
          return new ClusterStatus(taskTrackers.size(),
                                   totalMaps,
                                   totalReduces,
                                   maxCurrentTasks);          
        }
    }
    
    public synchronized void killJob(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        job.kill();
    }

    public synchronized JobProfile getJobProfile(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job != null) {
            return job.getProfile();
        } else {
            return null;
        }
    }
    public synchronized JobStatus getJobStatus(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job != null) {
            return job.getStatus();
        } else {
            return null;
        }
    }
    public synchronized TaskReport[] getMapTaskReports(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job == null) {
            return new TaskReport[0];
        } else {
            Vector reports = new Vector();
            Vector completeMapTasks = job.reportTasksInProgress(true, true);
            for (Iterator it = completeMapTasks.iterator(); it.hasNext(); ) {
                TaskInProgress tip = (TaskInProgress) it.next();
                reports.add(tip.generateSingleReport());
            }
            Vector incompleteMapTasks = job.reportTasksInProgress(true, false);
            for (Iterator it = incompleteMapTasks.iterator(); it.hasNext(); ) {
                TaskInProgress tip = (TaskInProgress) it.next();
                reports.add(tip.generateSingleReport());
            }
            return (TaskReport[]) reports.toArray(new TaskReport[reports.size()]);
        }
    }

    public synchronized TaskReport[] getReduceTaskReports(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job == null) {
            return new TaskReport[0];
        } else {
            Vector reports = new Vector();
            Vector completeReduceTasks = job.reportTasksInProgress(false, true);
            for (Iterator it = completeReduceTasks.iterator(); it.hasNext(); ) {
                TaskInProgress tip = (TaskInProgress) it.next();
                reports.add(tip.generateSingleReport());
            }
            Vector incompleteReduceTasks = job.reportTasksInProgress(false, false);
            for (Iterator it = incompleteReduceTasks.iterator(); it.hasNext(); ) {
                TaskInProgress tip = (TaskInProgress) it.next();
                reports.add(tip.generateSingleReport());
            }
            return (TaskReport[]) reports.toArray(new TaskReport[reports.size()]);
        }
    }

    /** Get all the TaskStatuses from the tipid. */
    TaskStatus[] getTaskStatuses(String jobid, String tipid){
	JobInProgress job = (JobInProgress) jobs.get(jobid);
	if (job == null){
	    return new TaskStatus[0];
	}
	TaskInProgress tip = (TaskInProgress) job.getTaskInProgress(tipid);
	if (tip == null){
	    return new TaskStatus[0];
	}
	return tip.getTaskStatuses();
    }

    /**
     * Get tracker name for a given task id.
     * @param taskId the name of the task
     * @return The name of the task tracker
     */
    public synchronized String getAssignedTracker(String taskId) {
      return (String) taskidToTrackerMap.get(taskId);
    }
    
    public JobStatus[] jobsToComplete() {
        Vector v = new Vector();
        for (Iterator it = jobs.values().iterator(); it.hasNext(); ) {
            JobInProgress jip = (JobInProgress) it.next();
            JobStatus status = jip.getStatus();
            if (status.getRunState() == JobStatus.RUNNING 
		|| status.getRunState() == JobStatus.PREP) {
		status.setStartTime(jip.getStartTime());
                v.add(status);
            }
        }
        return (JobStatus[]) v.toArray(new JobStatus[v.size()]);
    } 
    
    ///////////////////////////////////////////////////////////////
    // JobTracker methods
    ///////////////////////////////////////////////////////////////
    public JobInProgress getJob(String jobid) {
        return (JobInProgress) jobs.get(jobid);
    }
    /**
     * Grab random num for job id
     */
    String createUniqueId() {
        return idFormat.format(nextJobId++);
    }

    ////////////////////////////////////////////////////
    // Methods to track all the TaskTrackers
    ////////////////////////////////////////////////////
    /**
     * Accept and process a new TaskTracker profile.  We might
     * have known about the TaskTracker previously, or it might
     * be brand-new.  All task-tracker structures have already
     * been updated.  Just process the contained tasks and any
     * jobs that might be affected.
     */
    void updateTaskStatuses(TaskTrackerStatus status) {
        for (Iterator it = status.taskReports(); it.hasNext(); ) {
            TaskStatus report = (TaskStatus) it.next();
            report.setTaskTracker(status.getTrackerName());
            String taskId = report.getTaskId();
            TaskInProgress tip = (TaskInProgress) taskidToTIPMap.get(taskId);
            if (tip == null) {
                LOG.info("Serious problem.  While updating status, cannot find taskid " + report.getTaskId());
            } else {
                expireLaunchingTasks.removeTask(taskId);
                JobInProgress job = tip.getJob();

                if (report.getRunState() == TaskStatus.SUCCEEDED) {
                    job.completedTask(tip, report);
                } else if (report.getRunState() == TaskStatus.FAILED) {
                    // Tell the job to fail the relevant task
                    job.failedTask(tip, report.getTaskId(), report, 
                                   status.getTrackerName());
                } else {
                    job.updateTaskStatus(tip, report);
                }
            }
        }
    }

    /**
     * We lost the task tracker!  All task-tracker structures have 
     * already been updated.  Just process the contained tasks and any
     * jobs that might be affected.
     */
    void lostTaskTracker(String trackerName, String hostname) {
        LOG.info("Lost tracker '" + trackerName + "'");
        TreeSet lostTasks = (TreeSet) trackerToTaskMap.get(trackerName);
        trackerToTaskMap.remove(trackerName);

        if (lostTasks != null) {
            for (Iterator it = lostTasks.iterator(); it.hasNext(); ) {
                String taskId = (String) it.next();
                TaskInProgress tip = (TaskInProgress) taskidToTIPMap.get(taskId);

                // Completed reduce tasks never need to be failed, because 
                // their outputs go to dfs
                if (tip.isMapTask() || !tip.isComplete()) {
                  JobInProgress job = tip.getJob();
                  // if the job is done, we don't want to change anything
                  if (job.getStatus().getRunState() == JobStatus.RUNNING) {
                    job.failedTask(tip, taskId, "Lost task tracker", 
                                   hostname, trackerName);
                  }
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////
    // main()
    ////////////////////////////////////////////////////////////

    /**
     * Start the JobTracker process.  This is used only for debugging.  As a rule,
     * JobTracker should be run as part of the DFS Namenode process.
     */
    public static void main(String argv[]) throws IOException, InterruptedException {
        if (argv.length != 0) {
          System.out.println("usage: JobTracker");
          System.exit(-1);
        }

        Configuration conf=new Configuration();
        startTracker(conf);
    }
}
