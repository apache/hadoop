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
package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import static org.apache.hadoop.yarn.util.StringHelper.percent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.util.Times;

@XmlRootElement(name = "job")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobInfo {

  // ok for any user to see
  protected long startTime;
  protected long finishTime;
  protected long elapsedTime;
  protected String id;
  protected String name;
  protected String user;
  protected JobState state;
  protected int mapsTotal;
  protected int mapsCompleted;
  protected int reducesTotal;
  protected int reducesCompleted;
  protected float mapProgress;
  protected float reduceProgress;

  @XmlTransient
  protected String mapProgressPercent;
  @XmlTransient
  protected String reduceProgressPercent;

  // these should only be seen if acls allow
  protected int mapsPending;
  protected int mapsRunning;
  protected int reducesPending;
  protected int reducesRunning;
  protected boolean uberized;
  protected String diagnostics;
  protected int newReduceAttempts = 0;
  protected int runningReduceAttempts = 0;
  protected int failedReduceAttempts = 0;
  protected int killedReduceAttempts = 0;
  protected int successfulReduceAttempts = 0;
  protected int newMapAttempts = 0;
  protected int runningMapAttempts = 0;
  protected int failedMapAttempts = 0;
  protected int killedMapAttempts = 0;
  protected int successfulMapAttempts = 0;
  protected ArrayList<ConfEntryInfo> acls;

  public JobInfo() {
  }

  public JobInfo(Job job, Boolean hasAccess) {
    this.id = MRApps.toString(job.getID());
    JobReport report = job.getReport();
    this.startTime = report.getStartTime();
    this.finishTime = report.getFinishTime();
    this.elapsedTime = Times.elapsed(this.startTime, this.finishTime);
    if (this.elapsedTime == -1) {
      this.elapsedTime = 0;
    }
    this.name = job.getName().toString();
    this.user = job.getUserName();
    this.state = job.getState();
    this.mapsTotal = job.getTotalMaps();
    this.mapsCompleted = job.getCompletedMaps();
    this.mapProgress = report.getMapProgress() * 100;
    this.mapProgressPercent = percent(report.getMapProgress());
    this.reducesTotal = job.getTotalReduces();
    this.reducesCompleted = job.getCompletedReduces();
    this.reduceProgress = report.getReduceProgress() * 100;
    this.reduceProgressPercent = percent(report.getReduceProgress());

    this.acls = new ArrayList<ConfEntryInfo>();
    if (hasAccess) {
      this.diagnostics = "";
      countTasksAndAttempts(job);

      this.uberized = job.isUber();

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

  public int getNewReduceAttempts() {
    return this.newReduceAttempts;
  }

  public int getKilledReduceAttempts() {
    return this.killedReduceAttempts;
  }

  public int getFailedReduceAttempts() {
    return this.failedReduceAttempts;
  }

  public int getRunningReduceAttempts() {
    return this.runningReduceAttempts;
  }

  public int getSuccessfulReduceAttempts() {
    return this.successfulReduceAttempts;
  }

  public int getNewMapAttempts() {
    return this.newMapAttempts;
  }

  public int getKilledMapAttempts() {
    return this.killedMapAttempts;
  }

  public ArrayList<ConfEntryInfo> getAcls() {
    return acls;
  }

  public int getFailedMapAttempts() {
    return this.failedMapAttempts;
  }

  public int getRunningMapAttempts() {
    return this.runningMapAttempts;
  }

  public int getSuccessfulMapAttempts() {
    return this.successfulMapAttempts;
  }

  public int getReducesCompleted() {
    return this.reducesCompleted;
  }

  public int getReducesTotal() {
    return this.reducesTotal;
  }

  public int getReducesPending() {
    return this.reducesPending;
  }

  public int getReducesRunning() {
    return this.reducesRunning;
  }

  public int getMapsCompleted() {
    return this.mapsCompleted;
  }

  public int getMapsTotal() {
    return this.mapsTotal;
  }

  public int getMapsPending() {
    return this.mapsPending;
  }

  public int getMapsRunning() {
    return this.mapsRunning;
  }

  public String getState() {
    return this.state.toString();
  }

  public String getUserName() {
    return this.user;
  }

  public String getName() {
    return this.name;
  }

  public String getId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getElapsedTime() {
    return this.elapsedTime;
  }

  public long getFinishTime() {
    return this.finishTime;
  }

  public boolean isUberized() {
    return this.uberized;
  }

  public String getdiagnostics() {
    return this.diagnostics;
  }

  public float getMapProgress() {
    return this.mapProgress;
  }

  public String getMapProgressPercent() {
    return this.mapProgressPercent;
  }

  public float getReduceProgress() {
    return this.reduceProgress;
  }

  public String getReduceProgressPercent() {
    return this.reduceProgressPercent;
  }

  /**
   * Go through a job and update the member variables with counts for
   * information to output in the page.
   *
   * @param job
   *          the job to get counts for.
   */
  private void countTasksAndAttempts(Job job) {
    final Map<TaskId, Task> tasks = job.getTasks();
    if (tasks == null) {
      return;
    }
    for (Task task : tasks.values()) {
      switch (task.getType()) {
      case MAP:
        // Task counts
        switch (task.getState()) {
        case RUNNING:
          ++this.mapsRunning;
          break;
        case SCHEDULED:
          ++this.mapsPending;
          break;
        default:
          break;
        }
        break;
      case REDUCE:
        // Task counts
        switch (task.getState()) {
        case RUNNING:
          ++this.reducesRunning;
          break;
        case SCHEDULED:
          ++this.reducesPending;
          break;
        default:
          break;
        }
        break;
      default:
        throw new IllegalStateException(
            "Task type is neither map nor reduce: " + task.getType());
      }
      // Attempts counts
      Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
      int newAttempts, running, successful, failed, killed;
      for (TaskAttempt attempt : attempts.values()) {

        newAttempts = 0;
        running = 0;
        successful = 0;
        failed = 0;
        killed = 0;
        if (TaskAttemptStateUI.NEW.correspondsTo(attempt.getState())) {
          ++newAttempts;
        } else if (TaskAttemptStateUI.RUNNING.correspondsTo(attempt.getState())) {
          ++running;
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
          this.newMapAttempts += newAttempts;
          this.runningMapAttempts += running;
          this.successfulMapAttempts += successful;
          this.failedMapAttempts += failed;
          this.killedMapAttempts += killed;
          break;
        case REDUCE:
          this.newReduceAttempts += newAttempts;
          this.runningReduceAttempts += running;
          this.successfulReduceAttempts += successful;
          this.failedReduceAttempts += failed;
          this.killedReduceAttempts += killed;
          break;
        default:
          throw new IllegalStateException("Task type neither map nor reduce: " + 
              task.getType());
        }
      }
    }
  }

}
