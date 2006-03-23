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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

/*******************************************************
 * TaskTracker is a process that starts and tracks MR Tasks
 * in a networked environment.  It contacts the JobTracker
 * for Task assignments and reporting results.
 *
 * @author Mike Cafarella
 *******************************************************/
public class TaskTracker implements MRConstants, TaskUmbilicalProtocol, MapOutputProtocol, Runnable {
    static final long WAIT_FOR_DONE = 3 * 1000;
    private long taskTimeout; 

    static final int STALE_STATE = 1;

    public static final Logger LOG =
    LogFormatter.getLogger("org.apache.hadoop.mapred.TaskTracker");

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

    private Configuration fConf;
    private MapOutputFile mapOutputFile;

    private int maxCurrentTasks;

    class MapOutputServer extends RPC.Server {
      private MapOutputServer(int port, int threads) {
        super(TaskTracker.this, fConf, port, threads, false);
      }
      public TaskTracker getTaskTracker() {
        return TaskTracker.this;
      }
    }

    /**
     * Start with the local machine name, and the default JobTracker
     */
    public TaskTracker(Configuration conf) throws IOException {
      this(JobTracker.getAddress(conf), conf);
    }

    /**
     * Start with the local machine name, and the addr of the target JobTracker
     */
    public TaskTracker(InetSocketAddress jobTrackAddr, Configuration conf) throws IOException {
        maxCurrentTasks = conf.getInt("mapred.tasktracker.tasks.maximum", 2);

        this.fConf = conf;
        this.jobTrackAddr = jobTrackAddr;
        this.taskTimeout = conf.getInt("mapred.task.timeout", 10* 60 * 1000);
        this.mapOutputFile = new MapOutputFile();
        this.mapOutputFile.setConf(conf);
        initialize();
    }

    /**
     * Do the real constructor work here.  It's in a separate method
     * so we can call it again and "recycle" the object after calling
     * close().
     */
    void initialize() throws IOException {
        this.taskTrackerName = "tracker_" + (Math.abs(r.nextInt()) % 100000);
        LOG.info("Starting tracker " + taskTrackerName);
        this.localHostname = InetAddress.getLocalHost().getHostName();

        new JobConf(this.fConf).deleteLocalFiles(SUBDIR);

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
        while (true) {
            try {
                this.mapOutputServer = new MapOutputServer(mapOutputPort, maxCurrentTasks);
                this.mapOutputServer.start();
                break;
            } catch (BindException e) {
                LOG.info("Could not open mapoutput server at " + this.mapOutputPort + ", trying new port");
                this.mapOutputPort++;
            }
        }

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

            //
            // Emit standard hearbeat message to check in with JobTracker
            //
            Vector taskReports = new Vector();
            synchronized (this) {
                for (Iterator it = runningTasks.keySet().iterator(); it.hasNext(); ) {
                    String taskid = (String) it.next();
                    TaskInProgress tip = (TaskInProgress) runningTasks.get(taskid);
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
            if (justStarted) {
                this.fs = FileSystem.getNamed(jobClient.getFilesystemName(), this.fConf);
            }
            
            int resultCode = jobClient.emitHeartbeat(new TaskTrackerStatus(taskTrackerName, localHostname, mapOutputPort, taskReports), justStarted);
            justStarted = false;
              
            if (resultCode == InterTrackerProtocol.UNKNOWN_TASKTRACKER) {
                return STALE_STATE;
            }

            //
            // Check if we should create a new Task
            //
            if (mapTotal < maxCurrentTasks || reduceTotal < maxCurrentTasks) {
                Task t = jobClient.pollForNewTask(taskTrackerName);
                if (t != null) {
                    TaskInProgress tip = new TaskInProgress(t, this.fConf);
                    synchronized (this) {
                      tasks.put(t.getTaskId(), tip);
                      if (t.isMapTask()) {
                          mapTotal++;
                      } else {
                          reduceTotal++;
                      }
                      runningTasks.put(t.getTaskId(), tip);
                    }
                    tip.launchTask();
                }
            }

            //
            // Kill any tasks that have not reported progress in the last X seconds.
            //
            synchronized (this) {
                for (Iterator it = runningTasks.values().iterator(); it.hasNext(); ) {
                    TaskInProgress tip = (TaskInProgress) it.next();
                    if ((tip.getRunState() == TaskStatus.RUNNING) &&
                        (System.currentTimeMillis() - tip.getLastProgressReport() > this.taskTimeout)) {
                        LOG.info("Task " + tip.getTask().getTaskId() + " timed out.  Killing.");
                        tip.reportDiagnosticInfo("Timed out.");
                        tip.killAndCleanup();
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
            String toCloseId = jobClient.pollForTaskWithClosedJob(taskTrackerName);
            if (toCloseId != null) {
              synchronized (this) {
                TaskInProgress tip = (TaskInProgress) tasks.get(toCloseId);
                tip.jobHasFinished();
              }
            }
            lastHeartbeat = now;
        }

        return 0;
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
                            LOG.log(Level.INFO, "Lost connection to JobTracker [" + jobTrackAddr + "].  Retrying...", ex);
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
        private JobConf jobConf;

        /**
         */
        public TaskInProgress(Task task, Configuration conf) throws IOException {
            this.task = task;
            this.lastProgressReport = System.currentTimeMillis();
            this.jobConf = new JobConf(conf);
            this.jobConf.deleteLocalFiles(SUBDIR + File.separator + task.getTaskId());
            localizeTask(task);
        }

        /**
         * Some fields in the Task object need to be made machine-specific.
         * So here, edit the Task's fields appropriately.
         */
        void localizeTask(Task t) throws IOException {
            File localJobFile =
              this.jobConf.getLocalFile(SUBDIR+File.separator+t.getTaskId(), "job.xml");
            File localJarFile =
              this.jobConf.getLocalFile(SUBDIR+File.separator+t.getTaskId(), "job.jar");

            String jobFile = t.getJobFile();
            fs.copyToLocalFile(new File(jobFile), localJobFile);
            t.setJobFile(localJobFile.getCanonicalPath());

            JobConf jc = new JobConf(localJobFile);
            String jarFile = jc.getJar();
            if (jarFile != null) {
              fs.copyToLocalFile(new File(jarFile), localJarFile);
              jc.setJar(localJarFile.getCanonicalPath());

              BufferedOutputStream out =
                new BufferedOutputStream(new FileOutputStream(localJobFile));
              try {
                jc.write(out);
              } finally {
                out.close();
              }
            }
        }

        /**
         */
        public Task getTask() {
            return task;
        }

        /**
         */
        public TaskStatus createStatus() {
            TaskStatus status = new TaskStatus(task.getTaskId(), task.isMapTask(), progress, runstate, diagnosticInfo.toString(), (stateString == null) ? "" : stateString);
            if (diagnosticInfo.length() > 0) {
                diagnosticInfo = new StringBuffer();
            }
            return status;
        }

        /**
         * Kick off the task execution
         */
        public synchronized void launchTask() throws IOException {
            this.progress = 0.0f;
            this.runstate = TaskStatus.RUNNING;
            this.diagnosticInfo = new StringBuffer();
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
        public synchronized void taskFinished() {
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
            if (done) {
                runstate = TaskStatus.SUCCEEDED;
            } else {
                runstate = TaskStatus.FAILED;
            }

            //
            // If the task has failed, or if the task was killAndCleanup()'ed,
            // we should clean up right away.  We only wait to cleanup
            // if the task succeeded, and its results might be useful
            // later on to downstream job processing.
            //
            if (wasKilled || runstate == TaskStatus.FAILED) {
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
                killAndCleanup();
            } else {
                cleanup();
            }
        }

        /**
         * This task has run on too long, and should be killed.
         */
        public synchronized void killAndCleanup() throws IOException {
            if (runstate == TaskStatus.RUNNING) {
                wasKilled = true;
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
              synchronized (TaskTracker.this) {   // force into next heartbeat
                runningTasks.put(task.getTaskId(), this);
                mapTotal++;
              }
            } else {
              LOG.warning("Output already reported lost:"+task.getTaskId());
            }
        }

        /**
         * We no longer need anything from this task.  Either the 
         * controlling job is all done and the files have been copied
         * away, or the task failed and we don't need the remains.
         */
        synchronized void cleanup() throws IOException {
            tasks.remove(task.getTaskId());
            try {
                runner.close();
            } catch (IOException ie) {
            }
            this.jobConf.deleteLocalFiles(SUBDIR + File.separator + task.getTaskId());
        }
    }

    /////////////////////////////////////////////////////////////////
    // MapOutputProtocol
    /////////////////////////////////////////////////////////////////
    public MapOutputFile getFile(String mapTaskId, String reduceTaskId,
      IntWritable partition) {
    MapOutputFile mapOutputFile = new MapOutputFile(mapTaskId, reduceTaskId,
        partition.get());
    mapOutputFile.setConf(this.fConf);
    return mapOutputFile;
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
          LOG.warning("Progress from unknown child task: "+taskid+". Ignored.");
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
          LOG.warning("Error from unknown child task: "+taskid+". Ignored.");
        }
    }

    /** Child checking to see if we're alive.  Normally does nothing.*/
    public synchronized void ping(String taskid) throws IOException {
      if (tasks.get(taskid) == null) {
        throw new IOException("No such task id."); // force child exit
      }
    }

    /**
     * The task is done.
     */
    public synchronized void done(String taskid) throws IOException {
        TaskInProgress tip = (TaskInProgress) tasks.get(taskid);
        if (tip != null) {
          tip.reportDone();
        } else {
          LOG.warning("Unknown child task done: "+taskid+". Ignored.");
        }
    }

    /** A child task had a local filesystem error.  Exit, so that no future
     * jobs are accepted. */
    public synchronized void fsError(String message) throws IOException {
      LOG.severe("FSError, exiting: "+ message);
      running = false;
    }

    /////////////////////////////////////////////////////
    //  Called by TaskTracker thread after task process ends
    /////////////////////////////////////////////////////
    /**
     * The task is no longer running.  It may not have completed successfully
     */
    synchronized void reportTaskFinished(String taskid) {
        TaskInProgress tip = (TaskInProgress) tasks.get(taskid);
        if (tip != null) {
          tip.taskFinished();
        } else {
          LOG.warning("Unknown child task finshed: "+taskid+". Ignored.");
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
          LOG.warning("Unknown child with bad map output: "+taskid+". Ignored.");
        }
    }

    /** 
     * The main() for child processes. 
     */
    public static class Child {
        public static void main(String[] args) throws Throwable {
          LogFormatter.showTime(false);
          LOG.info("Child starting");

          Configuration conf = new Configuration();
          int port = Integer.parseInt(args[0]);
          String taskid = args[1];
          TaskUmbilicalProtocol umbilical =
            (TaskUmbilicalProtocol)RPC.getProxy(TaskUmbilicalProtocol.class,
                                                new InetSocketAddress(port), conf);
            
          Task task = umbilical.getTask(taskid);
          JobConf job = new JobConf(task.getJobFile());

          conf.addFinalResource(new File(task.getJobFile()));

          startPinging(umbilical, taskid);        // start pinging parent

          try {
              // If the user set a working directory, use it
              String workDir = job.getWorkingDirectory();
              if (workDir != null) {
                FileSystem file_sys = FileSystem.get(job);
                file_sys.setWorkingDirectory(new File(workDir));
              }
              task.run(job, umbilical);           // run the task
          } catch (FSError e) {
            LOG.log(Level.SEVERE, "FSError from child", e);
            umbilical.fsError(e.getMessage());
          } catch (Throwable throwable) {
              LOG.log(Level.WARNING, "Error running child", throwable);
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
                while (true) {
                  try {
                    umbilical.ping(taskid);
                  } catch (Throwable t) {
                    LOG.log(Level.WARNING, "Parent died.  Exiting "+taskid, t);
                    System.exit(1);
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
     * Start the TaskTracker, point toward the indicated JobTracker
     */
    public static void main(String argv[]) throws IOException {
        if (argv.length != 0) {
            System.out.println("usage: TaskTracker");
            System.exit(-1);
        }

        TaskTracker tt = new TaskTracker(new Configuration());
        tt.run();
    }
}
