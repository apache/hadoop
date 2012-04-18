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

package org.apache.hadoop.mapreduce.v2.hs;

import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Provides an API to query jobs that have finished.
 * 
 * For those implementing this API be aware that there is no feedback when
 * files are removed from HDFS.  You may rely on HistoryFileManager to help
 * you know when that has happened if you have not made a complete backup of
 * the data stored on HDFS.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface HistoryStorage {
  
  /**
   * Give the Storage a reference to a class that can be used to interact with
   * history files.
   * @param hsManager the class that is used to interact with history files.
   */
  void setHistoryFileManager(HistoryFileManager hsManager);
  
  /**
   * Look for a set of partial jobs.
   * @param offset the offset into the list of jobs.
   * @param count the maximum number of jobs to return.
   * @param user only return jobs for the given user.
   * @param queue only return jobs for in the given queue.
   * @param sBegin only return Jobs that started on or after the given time.
   * @param sEnd only return Jobs that started on or before the given time.
   * @param fBegin only return Jobs that ended on or after the given time.
   * @param fEnd only return Jobs that ended on or before the given time.
   * @param jobState only return Jobs that are in the given job state.
   * @return The list of filtered jobs.
   */
  JobsInfo getPartialJobs(Long offset, Long count, String user, 
      String queue, Long sBegin, Long sEnd, Long fBegin, Long fEnd, 
      JobState jobState);
  
  /**
   * Get all of the cached jobs.  This only returns partial jobs and is here for
   * legacy reasons.
   * @return all of the cached jobs
   */
  Map<JobId, Job> getAllPartialJobs();
  
  /**
   * Get a fully parsed job.
   * @param jobId the id of the job
   * @return the job, or null if it is not found.
   */
  Job getFullJob(JobId jobId);
}
