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
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;

/**
 * This class creates a single-process Map-Reduce cluster for junit testing.
 * One thread is created for each server.
 * @author Milind Bhandarkar
 */
public class MiniMRCluster {
    
    private Thread jobTrackerThread;
    private JobTrackerRunner jobTracker;
    private TaskTrackerRunner taskTracker;
    
    private int jobTrackerPort = 0;
    private int taskTrackerPort = 0;
    
    private int numTaskTrackers;
    
    private ArrayList taskTrackerList = new ArrayList();
    private ArrayList taskTrackerThreadList = new ArrayList();
    
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
                jc.setInt("mapred.task.tracker.output.port", taskTrackerPort++);
                jc.setInt("mapred.task.tracker.report.port", taskTrackerPort++);
                File localDir = new File(jc.get("mapred.local.dir"));
                File ttDir = new File(localDir, Integer.toString(taskTrackerPort));
                ttDir.mkdirs();
                jc.set("mapred.local.dir", ttDir.getAbsolutePath());
                tt = new TaskTracker(jc);
                tt.run();
            } catch (Throwable e) {
                tt = null;
                System.err.println("Task tracker crashed:");
                e.printStackTrace();
            }
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
     * Create the config and start up the servers.
     */
    public MiniMRCluster(int jobTrackerPort,
            int taskTrackerPort,
            int numTaskTrackers,
            String namenode) throws IOException {
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
        jobTrackerThread.start();
        try {                                     // let jobTracker get started
            Thread.sleep(2000);
        } catch(InterruptedException e) {
        }
        for (int idx = 0; idx < numTaskTrackers; idx++) {
            TaskTrackerRunner taskTracker = new TaskTrackerRunner();
            Thread taskTrackerThread = new Thread(taskTracker);
            taskTrackerThread.start();
            taskTrackerList.add(taskTracker);
            taskTrackerThreadList.add(taskTrackerThread);
        }
        try {                                     // let taskTrackers get started
            Thread.sleep(2000);
        } catch(InterruptedException e) {
        }
    }
    
    /**
     * Shut down the servers.
     */
    public void shutdown() {
        try {
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
        MiniMRCluster mr = new MiniMRCluster(50000, 50002, 4, "local");
        System.out.println("JobTracker and TaskTrackers are up.");
        mr.shutdown();
        System.out.println("JobTracker and TaskTrackers brought down.");
    }
}

