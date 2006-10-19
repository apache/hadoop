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

import java.io.*;
import java.util.*;

/**
 * This class creates a single-process Map-Reduce cluster for junit testing.
 * One thread is created for each server.
 * @author Milind Bhandarkar
 */
public class MiniMRCluster {
    
    private Thread jobTrackerThread;
    private JobTrackerRunner jobTracker;
    
    private int jobTrackerPort = 0;
    private int taskTrackerPort = 0;
    
    private int numTaskTrackers;
    
    private List taskTrackerList = new ArrayList();
    private List taskTrackerThreadList = new ArrayList();
    
    private String namenode;
    
    /**
     * An inner class that runs a job tracker.
     */
    class JobTrackerRunner implements Runnable {
        /**
         * Create the job tracker and run it.
         */
        public void run() {
            try {
                JobConf jc = new JobConf();
                jc.set("fs.name.node", namenode);
                jc.set("mapred.job.tracker", "localhost:"+jobTrackerPort);
                // this timeout seems to control the minimum time for the test, so
                // set it down at 2 seconds.
                jc.setInt("ipc.client.timeout", 1000);
                jc.set("mapred.local.dir","build/test/mapred/local");
                JobTracker.startTracker(jc);
            } catch (Throwable e) {
                System.err.println("Job tracker crashed:");
                e.printStackTrace();
            }
        }
        
        /**
         * Shutdown the job tracker and wait for it to finish.
         */
        public void shutdown() {
            try {
                JobTracker.stopTracker();
            } catch (Throwable e) {
                System.err.println("Unable to shut down job tracker:");
                e.printStackTrace();
            }
        }
    }
    
    /**
     * An inner class to run the task tracker.
     */
    class TaskTrackerRunner implements Runnable {
        TaskTracker tt;
        // the localDirs for this taskTracker
        String[] localDir;
        boolean isInitialized = false;
        boolean isDead = false;
        int numDir;       
        TaskTrackerRunner(int numDir) {
          this.numDir = numDir;
          // a maximum of 10 local dirs can be specified in MinMRCluster
          localDir = new String[10];
        }
        
        /**
         * Create and run the task tracker.
         */
        public void run() {
            try {
                JobConf jc = new JobConf();
                jc.set("fs.name.node", namenode);
                jc.set("mapred.job.tracker", "localhost:"+jobTrackerPort);
                // this timeout seems to control the minimum time for the test, so
                // set it down at 2 seconds.
                jc.setInt("ipc.client.timeout", 1000);
                jc.setInt("mapred.task.tracker.info.port", taskTrackerPort++);
                jc.setInt("mapred.task.tracker.report.port", taskTrackerPort++);
                File localDir = new File(jc.get("mapred.local.dir"));
                String mapredDir = "";
                File ttDir = new File(localDir, Integer.toString(taskTrackerPort) + "_" + 0);
                ttDir.mkdirs();
                this.localDir[0] = ttDir.getAbsolutePath();
                mapredDir = ttDir.getAbsolutePath();
                for (int i = 1; i < numDir; i++){
                  ttDir = new File(localDir, Integer.toString(taskTrackerPort) + "_" + i);
                  ttDir.mkdirs();
                  this.localDir[i] = ttDir.getAbsolutePath();
                  mapredDir = mapredDir + "," + ttDir.getAbsolutePath();
                }
                jc.set("mapred.local.dir", mapredDir);
                System.out.println("mapred.local.dir is " +  mapredDir);
                tt = new TaskTracker(jc);
                isInitialized = true;
                tt.run();
            } catch (Throwable e) {
                isDead = true;
                tt = null;
                System.err.println("Task tracker crashed:");
                e.printStackTrace();
            }
        }
        
        /**
         * Get the local dir for this TaskTracker.
         * This is there so that we do not break
         * previous tests. 
         * @return the absolute pathname
         */
        public String getLocalDir() {
          return localDir[0];
        }
       
        public String[] getLocalDirs(){
         return localDir;
        } 
        /**
         * Shut down the server and wait for it to finish.
         */
        public void shutdown() {
            if (tt != null) {
                try {
                    tt.shutdown();
                } catch (Throwable e) {
                    System.err.println("Unable to shut down task tracker:");
                    e.printStackTrace();
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
      for(Iterator itr= taskTrackerList.iterator(); itr.hasNext(); ) {
        TaskTrackerRunner runner = (TaskTrackerRunner) itr.next();
        while (!runner.isDead && (!runner.isInitialized || !runner.tt.isIdle())) {
          if (!runner.isInitialized) {
            System.out.println("Waiting for task tracker to start.");
          } else {
            System.out.println("Waiting for task tracker " + runner.tt.getName() +
                               " to be idle.");
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {}
        }
      }
    }
    
    /**
     * Create the config and start up the servers.
     */
    public MiniMRCluster(int jobTrackerPort,
                         int taskTrackerPort,
                         int numTaskTrackers,
                         String namenode,
                         boolean taskTrackerFirst) throws IOException {
        this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, taskTrackerFirst, 1);
    } 
  
    public MiniMRCluster(int jobTrackerPort,
            int taskTrackerPort,
            int numTaskTrackers,
            String namenode,
            boolean taskTrackerFirst, int numDir) throws IOException {
        this.jobTrackerPort = jobTrackerPort;
        this.taskTrackerPort = taskTrackerPort;
        this.numTaskTrackers = numTaskTrackers;
        this.namenode = namenode;
        
        File configDir = new File("build", "minimr");
        configDir.mkdirs();
        File siteFile = new File(configDir, "hadoop-site.xml");
        PrintWriter pw = new PrintWriter(siteFile);
        pw.print("<?xml version=\"1.0\"?>\n"+
                "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"+
                "<configuration>\n"+
                " <property>\n"+
                "   <name>mapred.system.dir</name>\n"+
                "   <value>build/test/mapred/system</value>\n"+
                " </property>\n"+
                "</configuration>\n");
        pw.close();
        jobTracker = new JobTrackerRunner();
        jobTrackerThread = new Thread(jobTracker);
        if (!taskTrackerFirst) {
          jobTrackerThread.start();
        }
        for (int idx = 0; idx < numTaskTrackers; idx++) {
            TaskTrackerRunner taskTracker = new TaskTrackerRunner(numDir);
            Thread taskTrackerThread = new Thread(taskTracker);
            taskTrackerThread.start();
            taskTrackerList.add(taskTracker);
            taskTrackerThreadList.add(taskTrackerThread);
        }
        if (taskTrackerFirst) {
          jobTrackerThread.start();
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
                ex.printStackTrace();
            }
        }
        jobTracker.shutdown();
        jobTrackerThread.interrupt();
        try {
            jobTrackerThread.join();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        } finally {
            File configDir = new File("build", "minimr");
            File siteFile = new File(configDir, "hadoop-site.xml");
            siteFile.delete();
        }
    }
    
    public static void main(String[] args) throws IOException {
        System.out.println("Bringing up Jobtracker and tasktrackers.");
        MiniMRCluster mr = new MiniMRCluster(50000, 50002, 4, "local", false);
        System.out.println("JobTracker and TaskTrackers are up.");
        mr.shutdown();
        System.out.println("JobTracker and TaskTrackers brought down.");
    }
}

