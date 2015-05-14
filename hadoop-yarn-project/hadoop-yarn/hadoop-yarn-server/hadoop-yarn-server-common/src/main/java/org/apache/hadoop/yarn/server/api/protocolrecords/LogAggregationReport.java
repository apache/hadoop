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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code LogAggregationReport} is a report for log aggregation status
 * in one NodeManager of an application.
 * <p>
 * It includes details such as:
 * <ul>
 *   <li>{@link ApplicationId} of the application.</li>
 *   <li>{@link LogAggregationStatus}</li>
 *   <li>Diagnostic information</li>
 * </ul>
 *
 */
@Public
@Unstable
public abstract class LogAggregationReport {

  @Public
  @Unstable
  public static LogAggregationReport newInstance(ApplicationId appId,
      LogAggregationStatus status, String diagnosticMessage) {
    LogAggregationReport report = Records.newRecord(LogAggregationReport.class);
    report.setApplicationId(appId);
    report.setLogAggregationStatus(status);
    report.setDiagnosticMessage(diagnosticMessage);
    return report;
  }

  /**
   * Get the <code>ApplicationId</code> of the application.
   * @return <code>ApplicationId</code> of the application
   */
  @Public
  @Unstable
  public abstract ApplicationId getApplicationId();

  @Public
  @Unstable
  public abstract void setApplicationId(ApplicationId appId);

  /**
   * Get the <code>LogAggregationStatus</code>.
   * @return <code>LogAggregationStatus</code>
   */
  @Public
  @Unstable
  public abstract LogAggregationStatus getLogAggregationStatus();

  @Public
  @Unstable
  public abstract void setLogAggregationStatus(
      LogAggregationStatus logAggregationStatus);

  /**
   * Get  the <em>diagnositic information</em> of this log aggregation
   * @return <em>diagnositic information</em> of this log aggregation
   */
  @Public
  @Unstable
  public abstract String getDiagnosticMessage();

  @Public
  @Unstable
  public abstract void setDiagnosticMessage(String diagnosticMessage);
}
