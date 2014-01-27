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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Records;

/**
 * The class contains the fields that can be determined when <code>RMApp</code>
 * finishes, and that need to be stored persistently.
 */
@Public
@Unstable
public abstract class ApplicationFinishData {

  @Public
  @Unstable
  public static ApplicationFinishData newInstance(ApplicationId applicationId,
      long finishTime, String diagnosticsInfo,
      FinalApplicationStatus finalApplicationStatus,
      YarnApplicationState yarnApplicationState) {
    ApplicationFinishData appFD =
        Records.newRecord(ApplicationFinishData.class);
    appFD.setApplicationId(applicationId);
    appFD.setFinishTime(finishTime);
    appFD.setDiagnosticsInfo(diagnosticsInfo);
    appFD.setFinalApplicationStatus(finalApplicationStatus);
    appFD.setYarnApplicationState(yarnApplicationState);
    return appFD;
  }

  @Public
  @Unstable
  public abstract ApplicationId getApplicationId();

  @Public
  @Unstable
  public abstract void setApplicationId(ApplicationId applicationId);

  @Public
  @Unstable
  public abstract long getFinishTime();

  @Public
  @Unstable
  public abstract void setFinishTime(long finishTime);

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
  public abstract YarnApplicationState getYarnApplicationState();

  @Public
  @Unstable
  public abstract void setYarnApplicationState(
      YarnApplicationState yarnApplicationState);

}
