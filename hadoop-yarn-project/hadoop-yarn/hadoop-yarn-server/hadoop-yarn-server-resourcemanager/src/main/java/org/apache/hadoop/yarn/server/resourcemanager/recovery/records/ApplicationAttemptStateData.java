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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records;

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;

/*
 * Contains the state data that needs to be persisted for an ApplicationAttempt
 */
@Public
@Unstable
public interface ApplicationAttemptStateData {
  
  /**
   * The ApplicationAttemptId for the application attempt
   * @return ApplicationAttemptId for the application attempt
   */
  @Public
  @Unstable
  public ApplicationAttemptId getAttemptId();
  
  public void setAttemptId(ApplicationAttemptId attemptId);
  
  /*
   * The master container running the application attempt
   * @return Container that hosts the attempt
   */
  @Public
  @Unstable
  public Container getMasterContainer();
  
  public void setMasterContainer(Container container);

  /**
   * The application attempt tokens that belong to this attempt
   * @return The application attempt tokens that belong to this attempt
   */
  @Public
  @Unstable
  public ByteBuffer getAppAttemptTokens();

  public void setAppAttemptTokens(ByteBuffer attemptTokens);

  /**
   * Get the final state of the application attempt.
   * @return the final state of the application attempt.
   */
  public RMAppAttemptState getState();

  public void setState(RMAppAttemptState state);

  /**
   * Get the original not-proxied <em>final tracking url</em> for the
   * application. This is intended to only be used by the proxy itself.
   * 
   * @return the original not-proxied <em>final tracking url</em> for the
   *         application
   */
  public String getFinalTrackingUrl();

  /**
   * Set the final tracking Url of the AM.
   * @param url
   */
  public void setFinalTrackingUrl(String url);
  /**
   * Get the <em>diagnositic information</em> of the attempt 
   * @return <em>diagnositic information</em> of the attempt
   */
  public String getDiagnostics();

  public void setDiagnostics(String diagnostics);

  /**
   * Get the <em>start time</em> of the application.
   * @return <em>start time</em> of the application
   */
  public long getStartTime();

  public void setStartTime(long startTime);

  /**
   * Get the <em>final finish status</em> of the application.
   * @return <em>final finish status</em> of the application
   */
  public FinalApplicationStatus getFinalApplicationStatus();

  public void setFinalApplicationStatus(FinalApplicationStatus finishState);
}
