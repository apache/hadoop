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
package org.apache.hadoop.mapreduce.v2.hs.webapp.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfEntryInfo;
import org.apache.hadoop.mapreduce.v2.hs.CompletedJob;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.security.authorize.AccessControlList;

@XmlRootElement(name = "job")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobInfo {

  protected long submitTime;
  protected long startTime;
  protected long finishTime;
  protected String id;
  protected String name;
  protected String queue;
  protected String user;
  protected String state;
  protected int mapsTotal;
  protected int mapsCompleted;
  protected int reducesTotal;
  protected int reducesCompleted;
  protected Boolean uberized;
  protected String diagnostics;
  protected Long avgMapTime;
  protected Long avgReduceTime;
  protected Long avgShuffleTime;
  protected Long avgMergeTime;
  protected Integer failedReduceAttempts;
  protected Integer killedReduceAttempts;
  protected Integer successfulReduceAttempts;
  protected Integer failedMapAttempts;
  protected Integer killedMapAttempts;
  protected Integer successfulMapAttempts;
  protected ArrayList<ConfEntryInfo> acls;

  @XmlTransient
  protected int numMaps;
  @XmlTransient
  protected int numReduces;

  public JobInfo() {
  }

  public JobInfo(Job job) {
    this.id = MRApps.toString(job.getID());
    JobReport report = job.getReport();
    
    this.mapsTotal = job.getTotalMaps();
    this.mapsCompleted = job.getCompletedMaps();
    this.reducesTotal = job.getTotalReduces();
    this.reducesCompleted = job.getCompletedReduces();
    this.submitTime = report.getSubmitTime();
    this.startTime = report.getStartTime();
    this.finishTime = report.getFinishTime();
    this.name = job.getName().toString();
    this.queue = job.getQueueName();
    this.user = job.getUserName();
    this.state = job.getState().toString();

    this.acls = new ArrayList<ConfEntryInfo>();
    
    if (job instanceof CompletedJob) {
      avgMapTime = 0l;
      avgReduceTime = 0l;
      avgShuffleTime = 0l;
      avgMergeTime = 0l;
      failedReduceAttempts = 0;
      killedReduceAttempts = 0;
      successfulReduceAttempts = 0;
      failedMapAttempts = 0;
      killedMapAttempts = 0;
      successfulMapAttempts = 0;
      countTasksAndAttempts(job);
      this.uberized = job.isUber();
      this.diagnostics = "";
      List<String> diagnostics = job.getDiagnostics();
      if (diagnostics != null && !diagnostics.isEmpty()) {
        StringBuffer b = new StringBuffer();
        for (String diag : diagnostics) {
          b.append(diag);
        }
        this.diagnostics = b.toString();
      }


      Map<JobACL, AccessControlList> allacls = job.getJobACLs();
      if (allacls != null) {
        for (Map.Entry<JobACL, AccessControlList> entry : allacls.entrySet()) {
          this.acls.add(new ConfEntryInfo(entry.getKey().getAclName(), entry
              .getValue().getAclString()));
        }
      }
    }
  }

  public long getNumMaps() {
    return numMaps;
  }

  public long getNumReduces() {
    return numReduces;
  }

  public Long getAvgMapTime() {
    return avgMapTime;
  }

  public Long getAvgReduceTime() {
    return avgReduceTime;
  }

  public Long getAvgShuffleTime() {
    return avgShuffleTime;
  }

  public Long getAvgMergeTime() {
    return avgMergeTime;
  }

  public Integer getFailedReduceAttempts() {
    return failedReduceAttempts;
  }

  public Integer getKilledReduceAttempts() {
    return killedReduceAttempts;
  }

  public Integer getSuccessfulReduceAttempts() {
    return successfulReduceAttempts;
  }

  public Integer getFailedMapAttempts() {
    return failedMapAttempts;
  }

  public Integer getKilledMapAttempts() {
    return killedMapAttempts;
  }

  public Integer getSuccessfulMapAttempts() {
    return successfulMapAttempts;
  }

  public ArrayList<ConfEntryInfo> getAcls() {
    return acls;
  }

  public int getReducesCompleted() {
    return this.reducesCompleted;
  }

  public int getReducesTotal() {
    return this.reducesTotal;
  }

  public int getMapsCompleted() {
    return this.mapsCompleted;
  }

  public int getMapsTotal() {
    return this.mapsTotal;
  }

  public String getState() {
    return this.state;
  }

  public String getUserName() {
    return this.user;
  }

  public String getName() {
    return this.name;
  }

  public String getQueueName() {
    return this.queue;
  }

  public String getId() {
    return this.id;
  }

  public long getSubmitTime() {
      return this.submitTime;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getFinishTime() {
    return this.finishTime;
  }

  public Boolean isUber() {
    return this.uberized;
  }

  public String getDiagnostics() {
    return this.diagnostics;
  }

  /**
   * Go through a job and update the member variables with counts for
   * information to output in the page.
   *
   * @param job
   *          the job to get counts for.
   */
  private void countTasksAndAttempts(Job job) {
    numReduces = 0;
    numMaps = 0;
    final Map<TaskId, Task> tasks = job.getTasks();
    if (tasks == null) {
      return;
    }
    for (Task task : tasks.values()) {
      // Attempts counts
      Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
      int successful, failed, killed;
      for (TaskAttempt attempt : attempts.values()) {

        successful = 0;
        failed = 0;
        killed = 0;
        if (TaskAttemptStateUI.NEW.correspondsTo(attempt.getState())) {
          // Do Nothing
        } else if (TaskAttemptStateUI.RUNNING.correspondsTo(attempt.getState())) {
          // Do Nothing
        } else if (TaskAttemptStateUI.SUCCESSFUL.correspondsTo(attempt
            .getState())) {
          ++successful;
        } else if (TaskAttemptStateUI.FAILED.correspondsTo(attempt.getState())) {
          ++failed;
        } else if (TaskAttemptStateUI.KILLED.correspondsTo(attempt.getState())) {
          ++killed;
        }

        switch (task.getType()) {
        case MAP:
          successfulMapAttempts += successful;
          failedMapAttempts += failed;
          killedMapAttempts += killed;
          if (attempt.getState() == TaskAttemptState.SUCCEEDED) {
            numMaps++;
            avgMapTime += (attempt.getFinishTime() - attempt.getLaunchTime());
          }
          break;
        case REDUCE:
          successfulReduceAttempts += successful;
          failedReduceAttempts += failed;
          killedReduceAttempts += killed;
          if (attempt.getState() == TaskAttemptState.SUCCEEDED) {
            numReduces++;
            avgShuffleTime += (attempt.getShuffleFinishTime() - attempt
                .getLaunchTime());
            avgMergeTime += attempt.getSortFinishTime()
                - attempt.getShuffleFinishTime();
            avgReduceTime += (attempt.getFinishTime() - attempt
                .getSortFinishTime());
          }
          break;
        }
      }
    }

    if (numMaps > 0) {
      avgMapTime = avgMapTime / numMaps;
    }

    if (numReduces > 0) {
      avgReduceTime = avgReduceTime / numReduces;
      avgShuffleTime = avgShuffleTime / numReduces;
      avgMergeTime = avgMergeTime / numReduces;
    }
  }

}
