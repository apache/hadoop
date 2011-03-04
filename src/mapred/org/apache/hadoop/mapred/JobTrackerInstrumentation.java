/*
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

class JobTrackerInstrumentation {

  protected final JobTracker tracker;
  
  public JobTrackerInstrumentation(JobTracker jt, JobConf conf) {
    tracker = jt;
  }

  public void launchMap(TaskAttemptID taskAttemptID)
  { }

  public void completeMap(TaskAttemptID taskAttemptID)
  { }

  public void failedMap(TaskAttemptID taskAttemptID)
  { }

  public void launchReduce(TaskAttemptID taskAttemptID)
  { }

  public void completeReduce(TaskAttemptID taskAttemptID)
  { }
  
  public void failedReduce(TaskAttemptID taskAttemptID)
  { }

  public void submitJob(JobConf conf, JobID id) 
  { }
    
  public void completeJob(JobConf conf, JobID id) 
  { }

  public void terminateJob(JobConf conf, JobID id) 
  { }
  
  public void finalizeJob(JobConf conf, JobID id) 
  { }
  
  public void addWaitingMaps(JobID id, int task)
  { }
  
  public void decWaitingMaps(JobID id, int task) 
  { }
  
  public void addWaitingReduces(JobID id, int task)
  { }
  
  public void decWaitingReduces(JobID id, int task)
  { }

  public void setMapSlots(int slots)
  { }

  public void setReduceSlots(int slots)
  { }

  public void addBlackListedMapSlots(int slots)
  { }

  public void decBlackListedMapSlots(int slots)
  { }

  public void addBlackListedReduceSlots(int slots)
  { }

  public void decBlackListedReduceSlots(int slots)
  { }
}
