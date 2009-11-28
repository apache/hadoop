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

/**
 * {@link JobCompleteEvent} is created by {@link SimulatorJobTracker} when a job
 * is completed. {@link SimulatorJobClient} picks up the event, and mark the job
 * as completed. When all jobs are completed, the simulation is terminated.
 */
public class JobCompleteEvent extends SimulatorEvent {

  private SimulatorEngine engine;
  private JobStatus jobStatus;

  public JobCompleteEvent(SimulatorJobClient jc, long timestamp, 
                          JobStatus jobStatus, SimulatorEngine engine) {
    super(jc, timestamp);
    this.engine = engine;
    this.jobStatus = jobStatus;
  }

  public SimulatorEngine getEngine() {
    return engine;
  }

  public JobStatus getJobStatus() {
    return jobStatus;
  }

  @Override
  protected String realToString() {
    return super.realToString()+", status=("+jobStatus.toString()+")";
  }
}
