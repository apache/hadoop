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
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

import java.io.*;
import java.net.*;
import java.util.*;

/*******************************************************
 * TaskTracker is a process that starts and tracks MR Tasks
 * in a networked environment.  It contacts the JobTracker
 * for Task assignments and reporting results.
 *
 * @author Mike Cafarella
 *******************************************************/
public class TaskTracker 
             implements MRConstants, TaskUmbilicalProtocol, Runnable {
    static final long WAIT_FOR_DONE = 3 * 1000;
    private long taskTimeout; 
    private int httpPort;

    static final int STALE_STATE = 1;

    // required for unknown reason to make WritableFactories work distributed
    static { new MapTask(); new ReduceTask(); new MapOutputLocation(); }

    public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.mapred.TaskTracker");

    private boolean running = true;

    String taskTrackerName;
    String localHostname;
    InetSocketAddress jobTrackAddr;

    int taskReportPort;
    int mapOutputPort;

    Server taskReportServer = null;
    Server mapOutputServer = null;
    InterTrackerProtocol jobClient;

    TreeMap tasks = null;
    TreeMap runningTasks = null;
    int mapTotal = 0;
    int reduceTotal = 0;
    boolean justStarted = true;

    static Random r = new Random();
    FileSystem fs = null;
    static final String SUBDIR = "taskTracker";

    private JobConf fConf;
    private MapOutputFile mapOutputFile;

    private int maxCurrentTasks;
    private int failures;
    
    /**
     * A list of tips that should be cleaned up.
     */
    private BlockingQueue tasksToCleanup = new BlockingQueue();
    
    /**
     * A daemon-thread that pulls tips off the list of things to cleanup.
     */
    private Thread taskCleanupThread = 
      new Thread(new Runnable() {
        public void run() {
          while (true) {
            try {
              TaskInProgress tip = (TaskInProgress) tasksToCleanup.take();
              tip.jobHasFinished();
            } catch (Throwable except) {
              LOG.warn(StringUtils.stringifyException(except));
            }
          }
        }
      });
    {
      taskCleanupThread.setDaemon(true);
      taskCleanupThread.start();
    }
    
    /**
     * Do the real constructor work here.  It's in a separate method
     * so we can call it again and "recycle" the object after calling
     * close().
     */
    synchronized void initialize() throws IOException {
        this.localHostname = InetAddress.getLocalHost().getHostName();

        //check local disk
        checkLocalDirs(this.fConf.getLocalDirs());
        fConf.deleteLocalFiles(SUBDIR);

        // Clear out state tables
        this.tasks = new TreeMap();
        this.runningTasks = new TreeMap();
        this.mapTotal = 0;
        this.reduceTotal = 0;

        // port numbers
        this.taskReportPort = this.fConf.getInt("mapred.task.tracker.report.port", 50050);
        this.mapOutputPort = this.fConf.getInt("mapred.task.tracker.output.port", 50040);

        // RPC initialization
        while (true) {
            try {
                this.taskReportServer = RPC.getServer(this, this.taskReportPort, maxCurrentTasks, false, this.fConf);
                this.taskReportServer.start();
                break;
            } catch (BindException e) {
                LOG.info("Could not open report server at " + this.taskReportPort + ", trying new port");
                this.taskReportPort++;
            }
        
        }
        this.taskTrackerName = "tracker_" + 
                               localHostname + ":" + taskReportPort;
        LOG.info("Starting tracker " + taskTrackerName);

        // Clear out temporary files that might be lying around
        this.mapOutputFile.cleanupStorage();
        this.justStarted = true;

        this.jobClient = (InterTrackerProtocol) RPC.getProxy(InterTrackerProtocol.class, jobTrackAddr, this.fConf);
    }

    /**
     * Close down the TaskTracker and all its components.  We must also shutdown
     * any running tasks or threads, and cleanup disk space.  A new TaskTracker
     * within the same process space might be restarted, so everything must be
     * clean.
     */
    public synchronized void close() throws IOException {
        //
        // Kill running tasks.  Do this in a 2nd vector, called 'tasksToClose',
        // because calling jobHasFinished() may result in an edit to 'tasks'.
        //
        TreeMap tasksToClose = new TreeMap();
        tasksToClose.putAll(tasks);
        for (Iterator it = tasksToClose.values().iterator(); it.hasNext(); ) {
            TaskInProgress tip = (TaskInProgress) it.next();
            tip.jobHasFinished();
        }

        // Wait for them to die and report in
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ie) {
        }

        //
        // Shutdown local RPC servers.  Do them
        // in parallel, as RPC servers can take a long
        // time to shutdown.  (They need to wait a full
        // RPC timeout, which might be 10-30 seconds.)
        //
        new Thread() {
            public void run() {
                if (taskReportServer != null) {
                    taskReportServer.stop();
                    taskReportServer = null;
                }
            }
        }.start();

        if (mapOutputServer != null) {
            mapOutputServer.stop();
            mapOutputServer = null;
        }

        // Clear local storage
        this.mapOutputFile.cleanupStorage();
    }

    /**
     * Start with the local machine name, and the default JobTracker
     */
    public TaskTracker(JobConf conf) throws IOException {
      maxCurrentTasks = conf.getInt("mapred.tasktracker.tasks.maximum", 2);
      this.fConf = conf;
      this.jobTrackAddr = JobTracker.getAddress(conf);
      this.taskTimeout = conf.getInt("mapred.task.timeout", 10* 60 * 1000);
      this.mapOutputFile = new MapOutputFile();
      this.mapOutputFile.setConf(conf);
      int httpPort = conf.getInt("tasktracker.http.port", 50060);
      StatusHttpServer server = new StatusHttpServer("task", httpPort, true);
      int workerThreads = conf.getInt("tasktracker.http.threads", 40);
      server.setThreads(1, workerThreads);
      server.start();
      this.httpPort = server.getPort();
      // let the jsp pages get to the task tracker, config, and other relevant
      // objects
      FileSystem local = FileSystem.getNamed("local", conf);
      server.setAttribute("task.tracker", this);
      server.setAttribute("local.file.system", local);
      server.setAttribute("conf", conf);
      server.setAttribute("log", LOG);
      initialize();
    }

    /**
     * The connection to the JobTracker, used by the TaskRunner 
     * for locating remote files.
     */
    public InterTrackerProtocol getJobClient() {
      return jobClient;
    }

    /**
     * Main service loop.  Will stay in this loop forever.
     */
    int offerService() throws Exception {
        long lastHeartbeat = 0;
        this.fs = FileSystem.getNamed(jobClient.getFilesystemName(), this.fConf);

        while (running) {
            long now = System.currentTimeMillis();

            long waitTime = HEARTBEAT_INTERVAL - (now - lastHeartbeat);
            if (waitTime > 0) {
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException ie) {
                }
                continue;
            }
            lastHeartbeat = now;

            //
            // Emit standard hearbeat message to check in with JobTracker
            //
            Vector taskReports = new Vector();
            synchronized (this) {
                for (Iterator it = runningTasks.values().iterator(); 
                     it.hasNext(); ) {
                    TaskInProgress tip = (TaskInProgress) it.next();
                    TaskStatus status = tip.createStatus();
                    taskReports.add(status);
                    if (status.getRunState() != TaskStatus.RUNNING) {
                        if (tip.getTask().isMapTask()) {
                            mapTotal--;
                        } else {
                            reduceTotal--;
                        }
                        it.remove();
                    }
                }
            }

            //
            // Xmit the heartbeat
            //
            
            TaskTrackerStatus status = 
              new TaskTrackerStatus(taskTrackerName, localHostname, 
                                    mapOutputPort, httpPort, taskReports, 
                                    failures); 
            int resultCode = jobClient.emitHeartbeat(status, justStarted);
            justStarted = false;
              
            if (resultCode == InterTrackerProtocol.UNKNOWN_TASKTRACKER) {
                return STALE_STATE;
            }

            //
            // Check if we should create a new Task
            //
            try {
              if (mapTotal < maxCurrentTasks || reduceTotal < maxCurrentTasks) {
                  checkLocalDirs(fConf.getLocalDirs());
                  Task t = jobClient.pollForNewTask(taskTrackerName);
                  if (t != null) {
                    startNewTask(t);
                  }
              }
            } catch (DiskErrorException de ) {
                LOG.warn("Exiting task tracker because "+de.getMessage());
                jobClient.reportTaskTrackerError(taskTrackerName, 
                        "DiskErrorException", de.getMessage());
                return STALE_STATE;
            } catch (IOException ie) {
              LOG.info("Problem launching task: " + 
                       StringUtils.stringifyException(ie));
            }

            //
            // Kill any tasks that have not reported progress in the last X seconds.
            //
            synchronized (this) {
                for (Iterator it = runningTasks.values().iterator(); it.hasNext(); ) {
                    TaskInProgress tip = (TaskInProgress) it.next();
                    long timeSinceLastReport = System.currentTimeMillis() - 
                                               tip.getLastProgressReport();
                    if ((tip.getRunState() == TaskStatus.RUNNING) &&
                        (timeSinceLastReport > this.taskTimeout) &&
                        !tip.wasKilled) {
                        String msg = "Task failed to report status for " +
                                     (timeSinceLastReport / 1000) + 
                                     " seconds. Killing.";
                        LOG.info(tip.getTask().getTaskId() + ": " + msg);
                        tip.reportDiagnosticInfo(msg);
                        try {
                          tip.killAndCleanup(true);
                        } catch (IOException ie) {
                          LOG.info("Problem cleaning task up: " +
                                   StringUtils.stringifyException(ie));
                        }
                    }
                }
            }

            //
            // Check for any Tasks that should be killed, even if
            // the containing Job is still ongoing.  (This happens
            // with speculative execution, when one version of the
            // task finished before another
            //

            //
            // Check for any Tasks whose job may have ended
            //
            try {
            String[] toCloseIds = jobClient.pollForTaskWithClosedJob(taskTrackerName);
            if (toCloseIds != null) {
              synchronized (this) {
                for (int i = 0; i < toCloseIds.length; i++) {
                  Object tip = tasks.get(toCloseIds[i]);
                  if (tip != null) {
                    tasksToCleanup.put(tip);
                  } else {
                    LOG.info("Attempt to cleanup unknown tip " + toCloseIds[i]);
                  }
                }
              }
            }
            } catch (IOException ie) {
              LOG.info("Problem getting closed tasks: " +
                       StringUtils.stringifyException(ie));
            }
        }

        return 0;
    }

    /**
     * Start a new task.
     * All exceptions are handled locally, so that we don't mess up the
     * task tracker.
     */
    private void startNewTask(Task t) {
      TaskInProgress tip = new TaskInProgress(t, this.fConf);
      synchronized (this) {
        tasks.put(t.getTaskId(), tip);
        runningTasks.put(t.getTaskId(), tip);
        boolean isMap = t.isMapTask();
        if (isMap) {
          mapTotal++;
        } else {
          reduceTotal++;
        }
      }
      synchronized (tip) {
        try {
          tip.launchTask();
        } catch (Throwable ie) {
          tip.runstate = TaskStatus.FAILED;
          try {
            tip.cleanup();
          } catch (Throwable ie2) {
            // Ignore it, we are just trying to cleanup.
          }
          String error = StringUtils.stringifyException(ie);
          tip.reportDiagnosticInfo(error);
          LOG.info(error);
        }
      }
    }
    
    /**
     * The server retry loop.  
     * This while-loop attempts to connect to the JobTracker.  It only 
     * loops when the old TaskTracker has gone bad (its state is
     * stale somehow) and we need to reinitialize everything.
     */
    public void run() {
        try {
            while (running) {
                boolean staleState = false;
                try {
                    // This while-loop attempts reconnects if we get network errors
                    while (running && ! staleState) {
                        try {
                            if (offerService() == STALE_STATE) {
                                staleState = true;
                            }
                        } catch (Exception ex) {
                            LOG.info("Lost connection to JobTracker [" + jobTrackAddr + "].  Retrying...", ex);
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException ie) {
                            }
                        }
                    }
                } finally {
                    close();
                }
                LOG.info("Reinitializing local state");
                initialize();
            }
        } catch (IOException iex) {
            LOG.info("Got fatal exception while reinitializing TaskTracker: " + iex.toString());
            return;
        }
    }

    /**
     * This class implements a queue that is put between producer and 
     * consumer threads. It will grow without bound.
     * @author Owen O'Malley
     */
    static private class BlockingQueue {
      private List queue;
      
      /**
       * Create an empty queue.
       */
      public BlockingQueue() {
        queue = new ArrayList();
      }
       
      /**
       * Put the given object at the back of the queue.
       * @param obj
       */
      public void put(Object obj) {
        synchronized (queue) {
          queue.add(obj);
          queue.notify();
        }
      }
      
      /**
       * Take the object at the front of the queue.
       * It blocks until there is an object available.
       * @return the head of the queue
       */
      public Object take() {
        synchronized (queue) {
          while (queue.isEmpty()) {
            try {
              queue.wait();
            } catch (InterruptedException ie) {}
          }
          Object result = queue.get(0);
          queue.remove(0);
          return result;
        }
      }
    }
    
    ///////////////////////////////////////////////////////
    // TaskInProgress maintains all the info for a Task that
    // lives at this TaskTracker.  It maintains the Task object,
    // its TaskStatus, and the TaskRunner.
    ///////////////////////////////////////////////////////
    class TaskInProgress {
        Task task;
        float progress;
        int runstate;
        String stateString = "";
        long lastProgressReport;
        StringBuffer diagnosticInfo = new StringBuffer();
        TaskRunner runner;
        boolean done = false;
        boolean wasKilled = false;
        private JobConf defaultJobConf;
        private JobConf localJobConf;

        /**
         */
        public TaskInProgress(Task task, JobConf conf) {
            this.task = task;
            this.progress = 0.0f;
            this.runstate = TaskStatus.UNASSIGNED;
            stateString = "initializing";
            this.lastProgressReport = System.currentTimeMillis();
            this.defaultJobConf = conf;
            localJobConf = null;
        }

        /**
         * Some fields in the Task object need to be made machine-specific.
         * So here, edit the Task's fields appropriately.
         */
        private void localizeTask(Task t) throws IOException {
            this.defaultJobConf.deleteLocalFiles(SUBDIR + "/" + 
                                                 task.getTaskId());
            Path localJobFile =
              this.defaultJobConf.getLocalPath(SUBDIR+"/"+t.getTaskId()+"/"+"job.xml");
            Path localJarFile =
              this.defaultJobConf.getLocalPath(SUBDIR+"/"+t.getTaskId()+"/"+"job.jar");

            String jobFile = t.getJobFile();
            fs.copyToLocalFile(new Path(jobFile), localJobFile);
            t.setJobFile(localJobFile.toString());

            localJobConf = new JobConf(localJobFile);
            localJobConf.set("mapred.task.id", task.getTaskId());
            String jarFile = localJobConf.getJar();
            if (jarFile != null) {
              fs.copyToLocalFile(new Path(jarFile), localJarFile);
              localJobConf.setJar(localJarFile.toString());

              FileSystem localFs = FileSystem.getNamed("local", fConf);
              OutputStream out = localFs.create(localJobFile);
              try {
                localJobConf.write(out);
              } finally {
                out.close();
              }
            }
            // set the task's configuration to the local job conf
            // rather than the default.
            t.setConf(localJobConf);
        }

        /**
         */
        public Task getTask() {
            return task;
        }

        /**
         */
        public synchronized TaskStatus createStatus() {
            TaskStatus status = new TaskStatus(task.getTaskId(), task.isMapTask(), progress, runstate, diagnosticInfo.toString(), (stateString == null) ? "" : stateString, "");
            if (diagnosticInfo.length() > 0) {
                diagnosticInfo = new StringBuffer();
            }
            return status;
        }

        /**
         * Kick off the task execution
         */
        public synchronized void launchTask() throws IOException {
            localizeTask(task);
            this.runstate = TaskStatus.RUNNING;
            this.runner = task.createRunner(TaskTracker.this);
            this.runner.start();
        }

        /**
         * The task is reporting its progress
         */
        public synchronized void reportProgress(float p, String state) {
            LOG.info(task.getTaskId()+" "+p+"% "+state);
            this.progress = p;
            this.runstate = TaskStatus.RUNNING;
            this.lastProgressReport = System.currentTimeMillis();
            this.stateString = state;
        }

        /**
         */
        public long getLastProgressReport() {
            return lastProgressReport;
        }

        /**
         */
        public int getRunState() {
            return runstate;
        }

        /**
         * The task has reported some diagnostic info about its status
         */
        public synchronized void reportDiagnosticInfo(String info) {
            this.diagnosticInfo.append(info);
        }

        /**
         * The task is reporting that it's done running
         */
        public synchronized void reportDone() {
            LOG.info("Task " + task.getTaskId() + " is done.");
            this.progress = 1.0f;
            this.done = true;
        }

        /**
         * The task has actually finished running.
         */
        public void taskFinished() {
            long start = System.currentTimeMillis();

            //
            // Wait until task reports as done.  If it hasn't reported in,
            // wait for a second and try again.
            //
            while (! done && (System.currentTimeMillis() - start < WAIT_FOR_DONE)) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }

            //
            // Change state to success or failure, depending on whether
            // task was 'done' before terminating
            //
            boolean needCleanup = false;
            synchronized (this) {
              if (done) {
                  runstate = TaskStatus.SUCCEEDED;
              } else {
                  if (!wasKilled) {
                    failures += 1;
                  }
                  runstate = TaskStatus.FAILED;
                  progress = 0.0f;
              }
              
              needCleanup = runstate == TaskStatus.FAILED;
            }

            //
            // If the task has failed, or if the task was killAndCleanup()'ed,
            // we should clean up right away.  We only wait to cleanup
            // if the task succeeded, and its results might be useful
            // later on to downstream job processing.
            //
            if (needCleanup) {
                try {
                    cleanup();
                } catch (IOException ie) {
                }
            }
        }

        /**
         * We no longer need anything from this task, as the job has
         * finished.  If the task is still running, kill it (and clean up
         */
        public synchronized void jobHasFinished() throws IOException {
            if (getRunState() == TaskStatus.RUNNING) {
                killAndCleanup(false);
            } else {
                cleanup();
            }
        }

        /**
         * This task has run on too long, and should be killed.
         */
        public synchronized void killAndCleanup(boolean wasFailure
                                                ) throws IOException {
            if (runstate == TaskStatus.RUNNING) {
                wasKilled = true;
                if (wasFailure) {
                  failures += 1;
                }
                runner.kill();
            }
        }

        /**
         * The map output has been lost.
         */
        public synchronized void mapOutputLost() throws IOException {
            if (runstate == TaskStatus.SUCCEEDED) {
              LOG.info("Reporting output lost:"+task.getTaskId());
              runstate = TaskStatus.FAILED;       // change status to failure
              progress = 0.0f;
              runningTasks.put(task.getTaskId(), this);
              mapTotal++;
            } else {
              LOG.warn("Output already reported lost:"+task.getTaskId());
            }
        }

        /**
         * We no longer need anything from this task.  Either the 
         * controlling job is all done and the files have been copied
         * away, or the task failed and we don't need the remains.
         */
        void cleanup() throws IOException {
            String taskId = task.getTaskId();
            LOG.debug("Cleaning up " + taskId);
            synchronized (TaskTracker.this) {
               tasks.remove(taskId);
               synchronized (this) {
                 try {
                    runner.close();
                 } catch (Throwable ie) {
                 }
               }
            }
            this.defaultJobConf.deleteLocalFiles(SUBDIR + "/" + taskId);
        }
    }

    // ///////////////////////////////////////////////////////////////
    // TaskUmbilicalProtocol
    /////////////////////////////////////////////////////////////////
    /**
     * Called upon startup by the child process, to fetch Task data.
     */
    public synchronized Task getTask(String taskid) throws IOException {
        TaskInProgress tip = (TaskInProgress) tasks.get(taskid);
        if (tip != null) {
            return (Task) tip.getTask();
        } else {
            return null;
        }
    }

    /**
     * Called periodically to report Task progress, from 0.0 to 1.0.
     */
    public synchronized void progress(String taskid, float progress, String state) throws IOException {
        TaskInProgress tip = (TaskInProgress) tasks.get(taskid);
        if (tip != null) {
          tip.reportProgress(progress, state);
        } else {
          LOG.warn("Progress from unknown child task: "+taskid+". Ignored.");
        }
    }

    /**
     * Called when the task dies before completion, and we want to report back
     * diagnostic info
     */
    public synchronized void reportDiagnosticInfo(String taskid, String info) throws IOException {
        TaskInProgress tip = (TaskInProgress) tasks.get(taskid);
        if (tip != null) {
          tip.reportDiagnosticInfo(info);
        } else {
          LOG.warn("Error from unknown child task: "+taskid+". Ignored.");
        }
    }

    /** Child checking to see if we're alive.  Normally does nothing.*/
    public synchronized boolean ping(String taskid) throws IOException {
      return tasks.get(taskid) != null;
    }

    /**
     * The task is done.
     */
    public synchronized void done(String taskid) throws IOException {
        TaskInProgress tip = (TaskInProgress) tasks.get(taskid);
        if (tip != null) {
          tip.reportDone();
        } else {
          LOG.warn("Unknown child task done: "+taskid+". Ignored.");
        }
    }

    /** A child task had a local filesystem error.  Exit, so that no future
     * jobs are accepted. */
    public synchronized void fsError(String message) throws IOException {
      LOG.fatal("FSError, exiting: "+ message);
      running = false;
    }

    /////////////////////////////////////////////////////
    //  Called by TaskTracker thread after task process ends
    /////////////////////////////////////////////////////
    /**
     * The task is no longer running.  It may not have completed successfully
     */
    void reportTaskFinished(String taskid) {
        TaskInProgress tip;
        synchronized (this) {
          tip = (TaskInProgress) tasks.get(taskid);
        }
        if (tip != null) {
          tip.taskFinished();
        } else {
          LOG.warn("Unknown child task finshed: "+taskid+". Ignored.");
        }
    }

    /**
     * A completed map task's output has been lost.
     */
    public synchronized void mapOutputLost(String taskid) throws IOException {
        TaskInProgress tip = (TaskInProgress) tasks.get(taskid);
        if (tip != null) {
          tip.mapOutputLost();
        } else {
          LOG.warn("Unknown child with bad map output: "+taskid+". Ignored.");
        }
    }

    /** 
     * The main() for child processes. 
     */
    public static class Child {
        public static void main(String[] args) throws Throwable {
          //LogFactory.showTime(false);
          LOG.info("Child starting");

          JobConf defaultConf = new JobConf();
          int port = Integer.parseInt(args[0]);
          String taskid = args[1];
          TaskUmbilicalProtocol umbilical =
            (TaskUmbilicalProtocol)RPC.getProxy(TaskUmbilicalProtocol.class,
                                                new InetSocketAddress(port), 
                                                defaultConf);
            
          Task task = umbilical.getTask(taskid);
          JobConf job = new JobConf(task.getJobFile());

          defaultConf.addFinalResource(new Path(task.getJobFile()));

          startPinging(umbilical, taskid);        // start pinging parent

          try {
            // use job-specified working directory
            FileSystem.get(job).setWorkingDirectory(job.getWorkingDirectory());
            task.run(job, umbilical);             // run the task
          } catch (FSError e) {
            LOG.fatal("FSError from child", e);
            umbilical.fsError(e.getMessage());
          } catch (Throwable throwable) {
              LOG.warn("Error running child", throwable);
              // Report back any failures, for diagnostic purposes
              ByteArrayOutputStream baos = new ByteArrayOutputStream();
              throwable.printStackTrace(new PrintStream(baos));
              umbilical.reportDiagnosticInfo(taskid, baos.toString());
          }
        }

        /** Periodically ping parent and exit when this fails.*/
        private static void startPinging(final TaskUmbilicalProtocol umbilical,
                                         final String taskid) {
          Thread thread = new Thread(new Runnable() {
              public void run() {
                final int MAX_RETRIES = 3;
                int remainingRetries = MAX_RETRIES;
                while (true) {
                  try {
                    if (!umbilical.ping(taskid)) {
                      LOG.warn("Parent died.  Exiting "+taskid);
                      System.exit(66);
                    }
                    remainingRetries = MAX_RETRIES;
                  } catch (Throwable t) {
                    String msg = StringUtils.stringifyException(t);
                    LOG.info("Ping exception: " + msg);
                    remainingRetries -=1;
                    if (remainingRetries == 0) {
                      LOG.warn("Last retry, killing "+taskid);
                      System.exit(65);
                    }
                  }
                  try {
                    Thread.sleep(1000);
                  } catch (InterruptedException e) {
                  }
                }
              }
            }, "Pinger for "+taskid);
          thread.setDaemon(true);
          thread.start();
        }
    }

    /**
     * Get the name for this task tracker.
     * @return the string like "tracker_mymachine:50010"
     */
    String getName() {
      return taskTrackerName;
    }
    
    /**
     * Get the list of tasks that will be reported back to the 
     * job tracker in the next heartbeat cycle.
     * @return a copy of the list of TaskStatus objects
     */
    synchronized List getRunningTaskStatuses() {
      List result = new ArrayList(runningTasks.size());
      Iterator itr = runningTasks.values().iterator();
      while (itr.hasNext()) {
        TaskInProgress tip = (TaskInProgress) itr.next();
        result.add(tip.createStatus());
      }
      return result;
    }
    
    /**
     * Get the default job conf for this tracker.
     */
    JobConf getJobConf() {
      return fConf;
    }
    
    /**
     * Check if the given local directories
     * (and parent directories, if necessary) can be created.
     * @param localDirs where the new TaskTracker should keep its local files.
     * @throws DiskErrorException if all local directories are not writable
     * @author hairong
     */
    private static void checkLocalDirs( String[] localDirs ) 
            throws DiskErrorException {
        boolean writable = false;
        
        if( localDirs != null ) {
            for (int i = 0; i < localDirs.length; i++) {
                try {
                    DiskChecker.checkDir( new File(localDirs[i]) );
                    writable = true;
                } catch( DiskErrorException e ) {
                    LOG.warn("Task Tracker local " + e.getMessage() );
                }
            }
        }

        if( !writable )
            throw new DiskErrorException( 
                    "all local directories are not writable" );
    }
    
    /**
     * Start the TaskTracker, point toward the indicated JobTracker
     */
    public static void main(String argv[]) throws Exception {
        if (argv.length != 0) {
            System.out.println("usage: TaskTracker");
            System.exit(-1);
        }

        try {
          JobConf conf=new JobConf();
          new TaskTracker(conf).run();
        } catch (IOException e) {
            LOG.warn( "Can not start task tracker because "+e.getMessage());
            System.exit(-1);
        }
    }
}
