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

package org.apache.hadoop.yarn.service.utils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import java.io.IOException;

/**
 * Serialized form of an service report which can be persisted
 * and then parsed. It can not be converted back into a
 * real YARN service report
 * 
 * Useful for testing
 */

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(value = JsonInclude.Include.NON_NULL)
public class SerializedApplicationReport {

  public String applicationId;
  public String applicationAttemptId;
  public String name;
  public String applicationType;
  public String user;
  public String queue;
  public String host;
  public Integer rpcPort;
  public String state;
  public String diagnostics;
  public String url;
  /**
   * This value is non-null only when a report is generated from a submission context.
   * The YARN {@link ApplicationReport} structure does not propagate this value
   * from the RM.
   */
  public Long submitTime;
  public Long startTime;
  public Long finishTime;
  public String finalStatus;
  public String origTrackingUrl;
  public Float progress;
  
  public SerializedApplicationReport() {
  }
  
  public SerializedApplicationReport(ApplicationReport report) {
    this.applicationId = report.getApplicationId().toString();
    ApplicationAttemptId attemptId = report.getCurrentApplicationAttemptId();
    this.applicationAttemptId = attemptId != null ? attemptId.toString() : "N/A";
    this.name = report.getName();
    this.applicationType = report.getApplicationType();
    this.user = report.getUser();
    this.queue = report.getQueue();
    this.host = report.getHost();
    this.rpcPort = report.getRpcPort();
    this.state = report.getYarnApplicationState().toString();
    this.diagnostics = report.getDiagnostics();
    this.startTime = report.getStartTime();
    this.finishTime = report.getFinishTime();
    FinalApplicationStatus appStatus = report.getFinalApplicationStatus();
    this.finalStatus = appStatus == null ? "" : appStatus.toString();
    this.progress = report.getProgress();
    this.url = report.getTrackingUrl();
    this.origTrackingUrl= report.getOriginalTrackingUrl();
  }

  @Override
  public String toString() {
    try {
      return ApplicationReportSerDeser.toString(this);
    } catch (IOException e) {
      return super.toString();
    }
  }
}
