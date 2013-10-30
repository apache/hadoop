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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.util.Records;

/**
 * The class contains the fields that can be determined when
 * <code>RMAppAttempt</code> finishes, and that need to be stored persistently.
 */
@Public
@Unstable
public abstract class ApplicationAttemptFinishData {

  @Public
  @Unstable
  public static ApplicationAttemptFinishData newInstance(
      ApplicationAttemptId appAttemptId, String diagnosticsInfo,
      String trackingURL, FinalApplicationStatus finalApplicationStatus,
      YarnApplicationAttemptState yarnApplicationAttemptState) {
    ApplicationAttemptFinishData appAttemptFD =
        Records.newRecord(ApplicationAttemptFinishData.class);
    appAttemptFD.setApplicationAttemptId(appAttemptId);
    appAttemptFD.setDiagnosticsInfo(diagnosticsInfo);
    appAttemptFD.setTrackingURL(trackingURL);
    appAttemptFD.setFinalApplicationStatus(finalApplicationStatus);
    appAttemptFD.setYarnApplicationAttemptState(yarnApplicationAttemptState);
    return appAttemptFD;
  }

  @Public
  @Unstable
  public abstract ApplicationAttemptId getApplicationAttemptId();

  @Public
  @Unstable
  public abstract void setApplicationAttemptId(
      ApplicationAttemptId applicationAttemptId);

  @Public
  @Unstable
  public abstract String getTrackingURL();

  @Public
  @Unstable
  public abstract void setTrackingURL(String trackingURL);

  @Public
  @Unstable
  public abstract String getDiagnosticsInfo();

  @Public
  @Unstable
  public abstract void setDiagnosticsInfo(String diagnosticsInfo);

  @Public
  @Unstable
  public abstract FinalApplicationStatus getFinalApplicationStatus();

  @Public
  @Unstable
  public abstract void setFinalApplicationStatus(
      FinalApplicationStatus finalApplicationStatus);

  @Public
  @Unstable
  public abstract YarnApplicationAttemptState getYarnApplicationAttemptState();

  @Public
  @Unstable
  public abstract void setYarnApplicationAttemptState(
      YarnApplicationAttemptState yarnApplicationAttemptState);

}
