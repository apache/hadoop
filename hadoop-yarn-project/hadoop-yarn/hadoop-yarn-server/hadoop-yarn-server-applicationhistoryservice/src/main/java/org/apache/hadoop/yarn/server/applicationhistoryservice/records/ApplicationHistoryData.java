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

/**
 * The class contains all the fields that need to be stored persistently for
 * <code>RMApp</code>.
 */
@Public
@Unstable
public interface ApplicationHistoryData {

  @Public
  @Unstable
  ApplicationId getApplicationId();

  @Public
  @Unstable
  void setApplicationId(ApplicationId applicationId);

  @Public
  @Unstable
  String getApplicationName();

  @Public
  @Unstable
  void setApplicationName(String applicationName);

  @Public
  @Unstable
  String getApplicationType();

  @Public
  @Unstable
  void setApplicationType(String applicationType);

  @Public
  @Unstable
  String getUser();

  @Public
  @Unstable
  void setUser(String user);

  @Public
  @Unstable
  String getQueue();

  @Public
  @Unstable
  void setQueue(String queue);

  @Public
  @Unstable
  long getSubmitTime();

  @Public
  @Unstable
  void setSubmitTime(long submitTime);

  @Public
  @Unstable
  long getStartTime();

  @Public
  @Unstable
  void setStartTime(long startTime);

  @Public
  @Unstable
  long getFinishTime();

  @Public
  @Unstable
  void setFinishTime(long finishTime);

  @Public
  @Unstable
  String getDiagnosticsInfo();

  @Public
  @Unstable
  void setDiagnosticsInfo(String diagnosticInfo);

  @Public
  @Unstable
  FinalApplicationStatus getFinalApplicationStatus();

  @Public
  @Unstable
  void setFinalApplicationStatus(FinalApplicationStatus finalApplicationStatus);

}
