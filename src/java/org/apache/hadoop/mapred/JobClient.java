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
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

/*******************************************************
 * JobClient interacts with the JobTracker network interface.
 * This object implements the job-control interface, and
 * should be the primary method by which user programs interact
 * with the networked job system.
 *
 * @author Mike Cafarella
 *******************************************************/
public class JobClient implements MRConstants {
    private static final Logger LOG = LogFormatter.getLogger("org.apache.hadoop.mapred.JobClient");

    static long MAX_JOBPROFILE_AGE = 1000 * 2;

    /**
     * A NetworkedJob is an implementation of RunningJob.  It holds
     * a JobProfile object to provide some info, and interacts with the
     * remote service to provide certain functionality.
     */
    class NetworkedJob implements RunningJob {
        JobProfile profile;
        JobStatus status;
        long statustime;

        /**
         * We store a JobProfile and a timestamp for when we last
         * acquired the job profile.  If the job is null, then we cannot
         * perform any of the tasks.  The job might be null if the JobTracker
         * has completely forgotten about the job.  (eg, 24 hours after the
         * job completes.)
         */
        public NetworkedJob(JobStatus job) throws IOException {
            this.status = job;
            this.profile = jobSubmitClient.getJobProfile(job.getJobId());
            this.statustime = System.currentTimeMillis();
        }

        /**
         * Some methods rely on having a recent job profile object.  Refresh
         * it, if necessary
         */
        synchronized void ensureFreshStatus() throws IOException {
            if (System.currentTimeMillis() - statustime > MAX_JOBPROFILE_AGE) {
                this.status = jobSubmitClient.getJobStatus(profile.getJobId());
                this.statustime = System.currentTimeMillis();
            }
        }

        /**
         * An identifier for the job
         */
        public String getJobID() {
            return profile.getJobId();
        }

        /**
         * The name of the job file
         */
        public String getJobFile() {
            return profile.getJobFile();
        }

        /**
         * A URL where the job's status can be seen
         */
        public String getTrackingURL() {
            return profile.getURL().toString();
        }

        /**
         * A float between 0.0 and 1.0, indicating the % of map work
         * completed.
         */
        public float mapProgress() throws IOException {
            ensureFreshStatus();
            return status.mapProgress();
        }

        /**
         * A float between 0.0 and 1.0, indicating the % of reduce work
         * completed.
         */
        public float reduceProgress() throws IOException {
            ensureFreshStatus();
            return status.reduceProgress();
        }

        /**
         * Returns immediately whether the whole job is done yet or not.
         */
        public synchronized boolean isComplete() throws IOException {
            ensureFreshStatus();
            return (status.getRunState() == JobStatus.SUCCEEDED ||
                    status.getRunState() == JobStatus.FAILED);
        }

        /**
         * True iff job completed successfully.
         */
        public synchronized boolean isSuccessful() throws IOException {
            ensureFreshStatus();
            return status.getRunState() == JobStatus.SUCCEEDED;
        }

        /**
         * Blocks until the job is finished
         */
        public synchronized void waitForCompletion() throws IOException {
            while (! isComplete()) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                }
            }
        }

        /**
         * Tells the service to terminate the current job.
         */
        public synchronized void killJob() throws IOException {
            jobSubmitClient.killJob(getJobID());
        }

        /**
         * Dump stats to screen
         */
        public String toString() {
            try {
                ensureFreshStatus();
            } catch (IOException e) {
            }
            return "Job: " + profile.getJobId() + "\n" + 
                "file: " + profile.getJobFile() + "\n" + 
                "tracking URL: " + profile.getURL() + "\n" + 
                "map() completion: " + status.mapProgress() + "\n" + 
                "reduce() completion: " + status.reduceProgress();
        }
    }

    JobSubmissionProtocol jobSubmitClient;
    FileSystem fs = null;

    private Configuration conf;
    static Random r = new Random();

    /**
     * Build a job client, connect to the default job tracker
     */
    public JobClient(Configuration conf) throws IOException {
      this.conf = conf;
      String tracker = conf.get("mapred.job.tracker", "local");
      if ("local".equals(tracker)) {
        this.jobSubmitClient = new LocalJobRunner(conf);
      } else {
        this.jobSubmitClient = (JobSubmissionProtocol) 
          RPC.getProxy(JobSubmissionProtocol.class,
                       JobTracker.getAddress(conf), conf);
      }
    }
  
    /**
     * Build a job client, connect to the indicated job tracker.
     */
    public JobClient(InetSocketAddress jobTrackAddr, Configuration conf) throws IOException {
        this.jobSubmitClient = (JobSubmissionProtocol) 
            RPC.getProxy(JobSubmissionProtocol.class, jobTrackAddr, conf);
    }


    /**
     */
    public synchronized void close() throws IOException {
        if (fs != null) {
            fs.close();
            fs = null;
        }
    }

    /**
     * Get a filesystem handle.  We need this to prepare jobs
     * for submission to the MapReduce system.
     */
    public synchronized FileSystem getFs() throws IOException {
      if (this.fs == null) {
        String fsName = jobSubmitClient.getFilesystemName();
        this.fs = FileSystem.getNamed(fsName, this.conf);
      }
      return fs;
    }

    /**
     * Submit a job to the MR system
     */
    public RunningJob submitJob(String jobFile) throws IOException {
        // Load in the submitted job details
        JobConf job = new JobConf(jobFile);
        return submitJob(job);
    }

    /**
     * Submit a job to the MR system
     */
    public RunningJob submitJob(JobConf job) throws IOException {
        //
        // First figure out what fs the JobTracker is using.  Copy the
        // job to it, under a temporary name.  This allows DFS to work,
        // and under the local fs also provides UNIX-like object loading 
        // semantics.  (that is, if the job file is deleted right after
        // submission, we can still run the submission to completion)
        //

        // Create a number of filenames in the JobTracker's fs namespace
        File submitJobDir = new File(job.getSystemDir(), "submit_" + Integer.toString(Math.abs(r.nextInt()), 36));
        File submitJobFile = new File(submitJobDir, "job.xml");
        File submitJarFile = new File(submitJobDir, "job.jar");

        String originalJarPath = job.getJar();

        if (originalJarPath != null) {           // Copy jar to JobTracker's fs
          job.setJar(submitJarFile.toString());
          getFs().copyFromLocalFile(new File(originalJarPath), submitJarFile);
        }

        FileSystem fileSys = getFs();

        // Set the user's name and working directory
        String user = System.getProperty("user.name");
        job.setUser(user != null ? user : "Dr Who");
        if (job.getWorkingDirectory() == null) {
          job.setWorkingDirectory(fileSys.getWorkingDirectory().toString());          
        }

        // Check the output specification
        job.getOutputFormat().checkOutputSpecs(fs, job);

        // Write job file to JobTracker's fs        
        FSDataOutputStream out = fileSys.create(submitJobFile);
        try {
          job.write(out);
        } finally {
          out.close();
        }

        //
        // Now, actually submit the job (using the submit name)
        //
        JobStatus status = jobSubmitClient.submitJob(submitJobFile.getPath());
        if (status != null) {
            return new NetworkedJob(status);
        } else {
            throw new IOException("Could not launch job");
        }
    }

    /**
     * Get an RunningJob object to track an ongoing job.  Returns
     * null if the id does not correspond to any known job.
     */
    public RunningJob getJob(String jobid) throws IOException {
        JobStatus status = jobSubmitClient.getJobStatus(jobid);
        if (status != null) {
            return new NetworkedJob(status);
        } else {
            return null;
        }
    }

    public ClusterStatus getClusterStatus() throws IOException {
      return jobSubmitClient.getClusterStatus();
    }
    
    /** Utility that submits a job, then polls for progress until the job is
     * complete. */
    public static void runJob(JobConf job) throws IOException {
      JobClient jc = new JobClient(job);
      boolean error = true;
      RunningJob running = null;
      String lastReport = null;
      try {
        running = jc.submitJob(job);
        String jobId = running.getJobID();
        LOG.info("Running job: " + jobId);
        while (!running.isComplete()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {}
          running = jc.getJob(jobId);
          String report = null;
          report = " map "+Math.round(running.mapProgress()*100)+"%  reduce " + Math.round(running.reduceProgress()*100)+"%";
          if (!report.equals(lastReport)) {
            LOG.info(report);
            lastReport = report;
          }
        }
        if (!running.isSuccessful()) {
          throw new IOException("Job failed!");
        }
        LOG.info("Job complete: " + jobId);
        error = false;
      } finally {
        if (error && (running != null)) {
          running.killJob();
        }
        jc.close();
      }
    }

    static Configuration getConfiguration(String jobTrackerSpec)
    {
      Configuration conf = new Configuration();
      if(jobTrackerSpec != null) {        
        if(jobTrackerSpec.indexOf(":") >= 0) {
          conf.set("mapred.job.tracker", jobTrackerSpec);
        } else {
          String classpathFile = "hadoop-" + jobTrackerSpec + ".xml";
          URL validate = conf.getResource(classpathFile);
          if(validate == null) {
            throw new RuntimeException(classpathFile + " not found on CLASSPATH");
          }
          conf.addFinalResource(classpathFile);
        }
      }
      return conf;
    }
        

    /**
     */
    public static void main(String argv[]) throws IOException {
        if (argv.length < 2) {
            System.out.println("JobClient -submit <job> | -status <id> | -kill <id> [-jt <jobtracker:port>|<config>]");
            System.exit(-1);
        }

        // Process args
        String jobTrackerSpec = null;
        String submitJobFile = null;
        String jobid = null;
        boolean getStatus = false;
        boolean killJob = false;

        for (int i = 0; i < argv.length; i++) {
            if ("-jt".equals(argv[i])) {
                jobTrackerSpec = argv[i+1];
                i++;
            } else if ("-submit".equals(argv[i])) {
                submitJobFile = argv[i+1];
                i++;
            } else if ("-status".equals(argv[i])) {
                jobid = argv[i+1];
                getStatus = true;
                i++;
            } else if ("-kill".equals(argv[i])) {
                jobid = argv[i+1];
                killJob = true;
                i++;
            }
        }

        // Submit the request
        JobClient jc = new JobClient(getConfiguration(jobTrackerSpec));
        try {
            if (submitJobFile != null) {
                RunningJob job = jc.submitJob(submitJobFile);
                System.out.println("Created job " + job.getJobID());
            } else if (getStatus) {
                RunningJob job = jc.getJob(jobid);
                if (job == null) {
                    System.out.println("Could not find job " + jobid);
                } else {
                    System.out.println();
                    System.out.println(job);
                }
            } else if (killJob) {
                RunningJob job = jc.getJob(jobid);
                if (job == null) {
                    System.out.println("Could not find job " + jobid);
                } else {
                    job.killJob();
                    System.out.println("Killed job " + jobid);
                }
            }
        } finally {
            jc.close();
        }
    }
}

