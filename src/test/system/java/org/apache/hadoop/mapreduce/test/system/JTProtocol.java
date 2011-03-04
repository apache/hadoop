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

package org.apache.hadoop.mapreduce.test.system;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.test.system.DaemonProtocol;

/**
 * Client side API's exposed from JobTracker.
 */
public interface JTProtocol extends DaemonProtocol {
  long versionID = 1L;

  /**
   * Get the information pertaining to given job.<br/>
   * 
   * @param id
   *          of the job for which information is required.
   * @return information of regarding job.
   * @throws IOException
   */
  public JobInfo getJobInfo(JobID jobID) throws IOException;

  /**
   * Gets the information pertaining to a task. <br/>
   * 
   * @param id
   *          of the task for which information is required.
   * @return information of regarding the task.
   * @throws IOException
   */
  public TaskInfo getTaskInfo(TaskID taskID) throws IOException;

  /**
   * Gets the information pertaining to a given TaskTracker. <br/>
   * 
   * @param name
   *          of the tracker.
   * @return information regarding the tracker.
   * @throws IOException
   */
  public TTInfo getTTInfo(String trackerName) throws IOException;

  /**
   * Gets a list of all available jobs with JobTracker.<br/>
   * 
   * @return list of all jobs.
   * @throws IOException
   */
  public JobInfo[] getAllJobInfo() throws IOException;

  /**
   * Gets a list of tasks pertaining to a job. <br/>
   * 
   * @param id
   *          of the job.
   * 
   * @return list of all tasks for the job.
   * @throws IOException
   */
  public TaskInfo[] getTaskInfo(JobID jobID) throws IOException;

  /**
   * Gets a list of TaskTrackers which have reported to the JobTracker. <br/>
   * 
   * @return list of all TaskTracker.
   * @throws IOException
   */
  public TTInfo[] getAllTTInfo() throws IOException;

  /**
   * Checks if a given job is retired from the JobTrackers Memory. <br/>
   * 
   * @param id
   *          of the job
   * @return true if job is retired.
   * @throws IOException
   */
  boolean isJobRetired(JobID jobID) throws IOException;

  /**
   * Gets the location of the history file for a retired job. <br/>
   * 
   * @param id
   *          of the job
   * @return location of history file
   * @throws IOException
   */
  String getJobHistoryLocationForRetiredJob(JobID jobID) throws IOException;
}
