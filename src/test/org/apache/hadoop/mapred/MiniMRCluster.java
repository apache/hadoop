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

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class creates a single-process Map-Reduce cluster for junit testing.
 * One thread is created for each server.
 */
public class MiniMRCluster {
  private static final Log LOG = LogFactory.getLog(MiniMRCluster.class);
    
  private Thread jobTrackerThread;
  private JobTrackerRunner jobTracker;
    
  private int jobTrackerPort = 0;
  private int taskTrackerPort = 0;
  private int jobTrackerInfoPort = 0;
  private int numTaskTrackers;
    
  private List<TaskTrackerRunner> taskTrackerList = new ArrayList<TaskTrackerRunner>();
  private List<Thread> taskTrackerThreadList = new ArrayList<Thread>();
    
  private String namenode;
    
  /**
   * An inner class that runs a job tracker.
   */
  class JobTrackerRunner implements Runnable {
    private JobTracker tracker = null;

    JobConf jc = null;
        
    public boolean isUp() {
      return (tracker != null);
    }
        
    public int getJobTrackerPort() {
      return tracker.getTrackerPort();
    }

    public int getJobTrackerInfoPort() {
      return tracker.getInfoPort();
    }
        
    /**
     * Create the job tracker and run it.
     */
    public void run() {
      try {
        jc = createJobConf();
        jc.set("mapred.local.dir","build/test/mapred/local");
        tracker = JobTracker.startTracker(jc);
        tracker.offerService();
      } catch (Throwable e) {
        LOG.error("Job tracker crashed", e);
      }
    }
        
    /**
     * Shutdown the job tracker and wait for it to finish.
     */
    public void shutdown() {
      try {
        if (tracker != null) {
          tracker.stopTracker();
        }
      } catch (Throwable e) {
        LOG.error("Problem shutting down job tracker", e);
      }
    }
  }
    
  /**
   * An inner class to run the task tracker.
   */
  class TaskTrackerRunner implements Runnable {
    volatile TaskTracker tt;
    int trackerId;
    JobConf conf = createJobConf();
    // the localDirs for this taskTracker
    String[] localDirs;
    volatile boolean isInitialized = false;
    volatile boolean isDead = false;
    int numDir;

    TaskTrackerRunner(int trackerId, int numDir) throws IOException {
      this.trackerId = trackerId;
      this.numDir = numDir;
      localDirs = new String[numDir];
      conf = createJobConf();
      conf.set("mapred.task.tracker.http.address", "0.0.0.0:0");
      conf.set("mapred.task.tracker.report.address", 
                "127.0.0.1:" + taskTrackerPort);
      File localDirBase = 
        new File(conf.get("mapred.local.dir")).getAbsoluteFile();
      localDirBase.mkdirs();
      StringBuffer localPath = new StringBuffer();
      for(int i=0; i < numDir; ++i) {
        File ttDir = new File(localDirBase, 
                              Integer.toString(trackerId) + "_" + 0);
        if (!ttDir.mkdirs()) {
          if (!ttDir.isDirectory()) {
            throw new IOException("Mkdirs failed to create " + ttDir);
          }
        }
        localDirs[i] = ttDir.toString();
        if (i != 0) {
          localPath.append(",");
        }
        localPath.append(localDirs[i]);
      }
      conf.set("mapred.local.dir", localPath.toString());
      LOG.info("mapred.local.dir is " +  localPath);
    }
        
    /**
     * Create and run the task tracker.
     */
    public void run() {
      try {
        tt = new TaskTracker(conf);
        isInitialized = true;
        tt.run();
      } catch (Throwable e) {
        isDead = true;
        tt = null;
        LOG.error("task tracker " + trackerId + " crashed", e);
      }
    }
        
    /**
     * Get the local dir for this TaskTracker.
     * This is there so that we do not break
     * previous tests. 
     * @return the absolute pathname
     */
    public String getLocalDir() {
      return localDirs[0];
    }
       
    public String[] getLocalDirs(){
      return localDirs;
    } 
    /**
     * Shut down the server and wait for it to finish.
     */
    public void shutdown() {
      if (tt != null) {
        try {
          tt.shutdown();
        } catch (Throwable e) {
          LOG.error("task tracker " + trackerId + " could not shut down",
                    e);
        }
      }
    }
  }
    
  /**
   * Get the local directory for the Nth task tracker
   * @param taskTracker the index of the task tracker to check
   * @return the absolute pathname of the local dir
   */
  public String getTaskTrackerLocalDir(int taskTracker) {
    return ((TaskTrackerRunner) 
            taskTrackerList.get(taskTracker)).getLocalDir();
  }

  /**
   * Get the number of task trackers in the cluster
   */
  public int getNumTaskTrackers() {
    return taskTrackerList.size();
  }
    
  /**
   * Wait until the system is idle.
   */
  public void waitUntilIdle() {
    for(Iterator itr= taskTrackerList.iterator(); itr.hasNext();) {
      TaskTrackerRunner runner = (TaskTrackerRunner) itr.next();
      while (!runner.isDead && (!runner.isInitialized || !runner.tt.isIdle())) {
        if (!runner.isInitialized) {
          LOG.info("Waiting for task tracker to start.");
        } else {
          LOG.info("Waiting for task tracker " + runner.tt.getName() +
                   " to be idle.");
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }
  }

  /** 
   * Get the actual rpc port used.
   */
  public int getJobTrackerPort() {
    return jobTrackerPort;
  }

  public JobConf createJobConf() {
    JobConf result = new JobConf();
    result.set("fs.default.name", namenode);
    result.set("mapred.job.tracker", "localhost:"+jobTrackerPort);
    result.set("mapred.job.tracker.http.address", 
                        "0.0.0.0:" + jobTrackerInfoPort);
    // for debugging have all task output sent to the test output
    JobClient.setTaskOutputFilter(result, JobClient.TaskStatusFilter.ALL);
    return result;
  }

  /**
   * Create the config and the cluster.
   * @param numTaskTrackers no. of tasktrackers in the cluster
   * @param namenode the namenode
   * @param numDir no. of directories
   * @throws IOException
   */
  public MiniMRCluster(int numTaskTrackers, String namenode, int numDir) 
    throws IOException {
    this(0, 0, numTaskTrackers, namenode, false, numDir);
  }
    
  /**
   * Create the config and start up the servers.  The ports supplied by the user are
   * just used as suggestions.  If those ports are already in use, new ports
   * are tried.  The caller should call getJobTrackerPort to get the actual rpc port used.
   * @deprecated use {@link #MiniMRCluster(int, String, int)}
   */
  public MiniMRCluster(int jobTrackerPort,
                       int taskTrackerPort,
                       int numTaskTrackers,
                       String namenode,
                       boolean taskTrackerFirst) throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, 
         taskTrackerFirst, 1);
  } 

  public MiniMRCluster(int jobTrackerPort,
                       int taskTrackerPort,
                       int numTaskTrackers,
                       String namenode,
                       boolean taskTrackerFirst, int numDir) throws IOException {

    this.jobTrackerPort = jobTrackerPort;
    this.taskTrackerPort = taskTrackerPort;
    this.jobTrackerInfoPort = 0;
    this.numTaskTrackers = numTaskTrackers;
    this.namenode = namenode;

    // Create the JobTracker
    jobTracker = new JobTrackerRunner();
    jobTrackerThread = new Thread(jobTracker);
        
    jobTrackerThread.start();
    while (!jobTracker.isUp()) {
      try {                                     // let daemons get started
        LOG.info("Waiting for JobTracker to start...");
        Thread.sleep(1000);
      } catch(InterruptedException e) {
      }
    }
        
    // Set the configuration for the task-trackers
    this.jobTrackerPort = jobTracker.getJobTrackerPort();
    this.jobTrackerInfoPort = jobTracker.getJobTrackerInfoPort();

    // Create the TaskTrackers
    for (int idx = 0; idx < numTaskTrackers; idx++) {
      TaskTrackerRunner taskTracker = new TaskTrackerRunner(idx, numDir);
      Thread taskTrackerThread = new Thread(taskTracker);
      taskTrackerList.add(taskTracker);
      taskTrackerThreadList.add(taskTrackerThread);
    }

    // Start the MiniMRCluster
        
    if (taskTrackerFirst) {
      for (Thread taskTrackerThread : taskTrackerThreadList){
        taskTrackerThread.start();
      }
    }
        
    if (!taskTrackerFirst) {
      for (Thread taskTrackerThread : taskTrackerThreadList){
        taskTrackerThread.start();
      }
    }

    // Wait till the MR cluster stabilizes
    waitUntilIdle();
  }
    
  /**
   * Shut down the servers.
   */
  public void shutdown() {
    try {
      waitUntilIdle();
      for (int idx = 0; idx < numTaskTrackers; idx++) {
        TaskTrackerRunner taskTracker = (TaskTrackerRunner) taskTrackerList.get(idx);
        Thread taskTrackerThread = (Thread) taskTrackerThreadList.get(idx);
        taskTracker.shutdown();
        taskTrackerThread.interrupt();
        try {
          taskTrackerThread.join();
        } catch (InterruptedException ex) {
          LOG.error("Problem shutting down task tracker", ex);
        }
      }
      jobTracker.shutdown();
      jobTrackerThread.interrupt();
      try {
        jobTrackerThread.join();
      } catch (InterruptedException ex) {
        LOG.error("Problem waiting for job tracker to finish", ex);
      }
    } finally {
      File configDir = new File("build", "minimr");
      File siteFile = new File(configDir, "hadoop-site.xml");
      siteFile.delete();
    }
  }
    
  public static void main(String[] args) throws IOException {
    LOG.info("Bringing up Jobtracker and tasktrackers.");
    MiniMRCluster mr = new MiniMRCluster(4, "local", 1);
    LOG.info("JobTracker and TaskTrackers are up.");
    mr.shutdown();
    LOG.info("JobTracker and TaskTrackers brought down.");
  }
}

