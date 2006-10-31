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

import org.apache.hadoop.ipc.VersionedProtocol;

/** 
 * Protocol that a JobClient and the central JobTracker use to communicate.  The
 * JobClient can use these methods to submit a Job for execution, and learn about
 * the current system status.
 */ 
interface JobSubmissionProtocol extends VersionedProtocol {
    public static final long versionID = 1L;
    /**
     * Submit a Job for execution.  Returns the latest profile for
     * that job.
     */
    public JobStatus submitJob(String jobFile) throws IOException;

    /**
     * Get the current status of the cluster
     * @return summary of the state of the cluster
     */
    public ClusterStatus getClusterStatus() throws IOException;
    
    /**
     * Kill the indicated job
     */
    public void killJob(String jobid) throws IOException;

    /**
     * Grab a handle to a job that is already known to the JobTracker
     */
    public JobProfile getJobProfile(String jobid) throws IOException;

    /**
     * Grab a handle to a job that is already known to the JobTracker
     */
    public JobStatus getJobStatus(String jobid) throws IOException;

    /**
     * Grab a bunch of info on the tasks that make up the job
     */
    public TaskReport[] getMapTaskReports(String jobid) throws IOException;
    public TaskReport[] getReduceTaskReports(String jobid) throws IOException;

    /**
     * A MapReduce system always operates on a single filesystem.  This 
     * function returns the fs name.  ('local' if the localfs; 'addr:port' 
     * if dfs).  The client can then copy files into the right locations 
     * prior to submitting the job.
     */
    public String getFilesystemName() throws IOException;

    /** 
     * Get the jobs that are not completed and not failed
     * @return array of JobStatus for the running/to-be-run
     * jobs.
     */
    public JobStatus[] jobsToComplete() throws IOException;
}
