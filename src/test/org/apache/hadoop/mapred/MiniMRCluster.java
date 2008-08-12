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
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.fs.FileSystem;

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
  private UnixUserGroupInformation ugi = null;
    
  /**
   * An inner class that runs a job tracker.
   */
  class JobTrackerRunner implements Runnable {
    private JobTracker tracker = null;
    
    JobConf jc = null;
        
    public JobTrackerRunner(JobConf conf) {
      jc = conf;
    }

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
        jc = (jc == null) ? createJobConf() : createJobConf(jc);
        jc.set("mapred.local.dir","build/test/mapred/local");
        jc.setClass("topology.node.switch.mapping.impl", 
            StaticMapping.class, DNSToSwitchMapping.class);
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
    // the localDirs for this taskTracker
    String[] localDirs;
    volatile boolean isInitialized = false;
    volatile boolean isDead = false;
    int numDir;

    TaskTrackerRunner(int trackerId, int numDir, String hostname) 
    throws IOException {
      this.trackerId = trackerId;
      this.numDir = numDir;
      localDirs = new String[numDir];
      JobConf conf = createJobConf();
      if (hostname != null) {
        conf.set("slave.host.name", hostname);
      }
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
      try {
        tt = new TaskTracker(conf);
        isInitialized = true;
      } catch (Throwable e) {
        isDead = true;
        tt = null;
        LOG.error("task tracker " + trackerId + " crashed", e);
      }
    }
        
    /**
     * Create and run the task tracker.
     */
    public void run() {
      try {
        if (tt != null) {
          tt.run();
        }
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
    
    public TaskTracker getTaskTracker() {
      return tt;
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
    return createJobConf(new JobConf());
  }

  public JobConf createJobConf(JobConf conf) {
    JobConf result = new JobConf(conf);
    FileSystem.setDefaultUri(result, namenode);
    result.set("mapred.job.tracker", "localhost:"+jobTrackerPort);
    result.set("mapred.job.tracker.http.address", 
                        "127.0.0.1:" + jobTrackerInfoPort);
    if (ugi != null) {
      result.set("mapred.system.dir", "/mapred/system");
      UnixUserGroupInformation.saveToConf(result,
          UnixUserGroupInformation.UGI_PROPERTY_NAME, ugi);
    }
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
  public MiniMRCluster(int numTaskTrackers, String namenode, int numDir, 
      String[] racks, String[] hosts) throws IOException {
    this(0, 0, numTaskTrackers, namenode, numDir, racks, hosts);
  }
  
  /**
   * Create the config and the cluster.
   * @param numTaskTrackers no. of tasktrackers in the cluster
   * @param namenode the namenode
   * @param numDir no. of directories
   * @param racks Array of racks
   * @param hosts Array of hosts in the corresponding racks
   * @param conf Default conf for the jobtracker
   * @throws IOException
   */
  public MiniMRCluster(int numTaskTrackers, String namenode, int numDir, 
                       String[] racks, String[] hosts, JobConf conf) 
  throws IOException {
    this(0, 0, numTaskTrackers, namenode, numDir, racks, hosts, null, conf);
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
    this(0, 0, numTaskTrackers, namenode, numDir);
  }
    
  public MiniMRCluster(int jobTrackerPort,
      int taskTrackerPort,
      int numTaskTrackers,
      String namenode,
      int numDir)
  throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, 
         numDir, null);
  }
  
  public MiniMRCluster(int jobTrackerPort,
      int taskTrackerPort,
      int numTaskTrackers,
      String namenode,
      int numDir,
      String[] racks) throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, 
         numDir, racks, null);
  }
  
  public MiniMRCluster(int jobTrackerPort,
                       int taskTrackerPort,
                       int numTaskTrackers,
                       String namenode,
                       int numDir,
                       String[] racks, String[] hosts) throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, 
         numDir, racks, hosts, null);
  }

  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort,
      int numTaskTrackers, String namenode, 
      int numDir, String[] racks, String[] hosts, UnixUserGroupInformation ugi
      ) throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, 
         numDir, racks, hosts, ugi, null);
  }

  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort,
      int numTaskTrackers, String namenode, 
      int numDir, String[] racks, String[] hosts, UnixUserGroupInformation ugi,
      JobConf conf) throws IOException {
    if (racks != null && racks.length < numTaskTrackers) {
      LOG.error("Invalid number of racks specified. It should be at least " +
          "equal to the number of tasktrackers");
      shutdown();
    }
    if (hosts != null && numTaskTrackers > hosts.length ) {
      throw new IllegalArgumentException( "The length of hosts [" + hosts.length
          + "] is less than the number of tasktrackers [" + numTaskTrackers + "].");
    }
     
     //Generate rack names if required
     if (racks == null) {
       System.out.println("Generating rack names for tasktrackers");
       racks = new String[numTaskTrackers];
       for (int i=0; i < racks.length; ++i) {
         racks[i] = NetworkTopology.DEFAULT_RACK;
       }
     }
     
    //Generate some hostnames if required
    if (hosts == null) {
      System.out.println("Generating host names for tasktrackers");
      hosts = new String[numTaskTrackers];
      for (int i = 0; i < numTaskTrackers; i++) {
        hosts[i] = "host" + i + ".foo.com";
      }
    }
    this.jobTrackerPort = jobTrackerPort;
    this.taskTrackerPort = taskTrackerPort;
    this.jobTrackerInfoPort = 0;
    this.numTaskTrackers = numTaskTrackers;
    this.namenode = namenode;
    this.ugi = ugi;

    // Create the JobTracker
    jobTracker = new JobTrackerRunner(conf);
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
      if (racks != null) {
        StaticMapping.addNodeToRack(hosts[idx],racks[idx]);
      }
      if (hosts != null) {
        NetUtils.addStaticResolution(hosts[idx], "localhost");
      }
      TaskTrackerRunner taskTracker;
      taskTracker = new TaskTrackerRunner(idx, numDir, 
          hosts == null ? null : hosts[idx]);
      
      Thread taskTrackerThread = new Thread(taskTracker);
      taskTrackerList.add(taskTracker);
      taskTrackerThreadList.add(taskTrackerThread);
    }

    // Start the MiniMRCluster
        
    for (Thread taskTrackerThread : taskTrackerThreadList){
      taskTrackerThread.start();
    }

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
    MiniMRCluster mr = new MiniMRCluster(4, "file:///", 1);
    LOG.info("JobTracker and TaskTrackers are up.");
    mr.shutdown();
    LOG.info("JobTracker and TaskTrackers brought down.");
  }
}

