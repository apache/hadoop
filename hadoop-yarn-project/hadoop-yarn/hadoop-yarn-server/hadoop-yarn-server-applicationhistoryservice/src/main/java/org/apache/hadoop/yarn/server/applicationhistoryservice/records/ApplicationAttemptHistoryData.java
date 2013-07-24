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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

/**
 * The class contains all the fields that need to be stored persistently for
 * <code>RMAppAttempt</code>.
 */
@Public
@Unstable
public interface ApplicationAttemptHistoryData {

  @Public
  @Unstable
  ApplicationAttemptId getApplicationAttemptId();

  @Public
  @Unstable
  void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId);

  @Public
  @Unstable
  String getHost();

  @Public
  @Unstable
  void setHost(String host);

  @Public
  @Unstable
  int getRPCPort();

  @Public
  @Unstable
  void setRPCPort(int rpcPort);

  @Public
  @Unstable
  String getTrackingURL();

  @Public
  @Unstable
  void setTrackingURL(String trackingURL);

  @Public
  @Unstable
  String getDiagnosticsInfo();

  @Public
  @Unstable
  void setDiagnosticsInfo(String diagnosticsInfo);

  @Public
  @Unstable
  FinalApplicationStatus getFinalApplicationStatus();

  @Public
  @Unstable
  void setFinalApplicationStatus(FinalApplicationStatus finalApplicationStatus);

  @Public
  @Unstable
  ContainerId getMasterContainerId();

  @Public
  @Unstable
  void setMasterContainerId(ContainerId masterContainerId);

}
