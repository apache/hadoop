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

package org.apache.hadoop.mapreduce.v2.api.records;

public interface JobReport {
  public abstract JobId getJobId();
  public abstract JobState getJobState();
  public abstract float getMapProgress();
  public abstract float getReduceProgress();
  public abstract float getCleanupProgress();
  public abstract float getSetupProgress();
  public abstract long getStartTime();
  public abstract long getFinishTime();
  public abstract String getUser();
  public abstract String getJobName();

  public abstract void setJobId(JobId jobId);
  public abstract void setJobState(JobState jobState);
  public abstract void setMapProgress(float progress);
  public abstract void setReduceProgress(float progress);
  public abstract void setCleanupProgress(float progress);
  public abstract void setSetupProgress(float progress);
  public abstract void setStartTime(long startTime);
  public abstract void setFinishTime(long finishTime);
  public abstract void setUser(String user);
  public abstract void setJobName(String jobName);
}
