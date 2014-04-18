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

package org.apache.hadoop.mapreduce.v2.jobhistory;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

/**
 * Maintains information which may be used by the jobHistory indexing
 * system.
 */
public class JobIndexInfo {
  private long submitTime;
  private long finishTime;
  private String user;
  private String queueName;
  private String jobName;
  private JobId jobId;
  private int numMaps;
  private int numReduces;
  private String jobStatus;
  private long jobStartTime;
  
  public JobIndexInfo() {
  }
  
  public JobIndexInfo(long submitTime, long finishTime, String user,
      String jobName, JobId jobId, int numMaps, int numReduces, String jobStatus) {
    this(submitTime, finishTime, user, jobName, jobId, numMaps, numReduces,
         jobStatus, JobConf.DEFAULT_QUEUE_NAME);
  }

  public JobIndexInfo(long submitTime, long finishTime, String user,
                      String jobName, JobId jobId, int numMaps, int numReduces,
                      String jobStatus, String queueName) {
    this.submitTime = submitTime;
    this.finishTime = finishTime;
    this.user = user;
    this.jobName = jobName;
    this.jobId = jobId;
    this.numMaps = numMaps;
    this.numReduces = numReduces;
    this.jobStatus = jobStatus;
    this.jobStartTime = -1;
    this.queueName = queueName;
  }

  public long getSubmitTime() {
    return submitTime;
  }
  public void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }
  public long getFinishTime() {
    return finishTime;
  }
  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }
  public String getUser() {
    return user;
  }
  public void setUser(String user) {
    this.user = user;
  }
  public String getQueueName() {
    return queueName;
  }
  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }
  public String getJobName() {
    return jobName;
  }
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }
  public JobId getJobId() {
    return jobId;
  }
  public void setJobId(JobId jobId) {
    this.jobId = jobId;
  }
  public int getNumMaps() {
    return numMaps;
  }
  public void setNumMaps(int numMaps) {
    this.numMaps = numMaps;
  }
  public int getNumReduces() {
    return numReduces;
  }
  public void setNumReduces(int numReduces) {
    this.numReduces = numReduces;
  }
  public String getJobStatus() {
    return jobStatus;
  }
  public void setJobStatus(String jobStatus) {
    this.jobStatus = jobStatus;
  }
  public long getJobStartTime() {
      return jobStartTime;
  }
  public void setJobStartTime(long lTime) {
      this.jobStartTime = lTime;
  }

  @Override
  public String toString() {
    return "JobIndexInfo [submitTime=" + submitTime + ", finishTime="
        + finishTime + ", user=" + user + ", jobName=" + jobName + ", jobId="
        + jobId + ", numMaps=" + numMaps + ", numReduces=" + numReduces
        + ", jobStatus=" + jobStatus + "]";
  }
  
  
}
