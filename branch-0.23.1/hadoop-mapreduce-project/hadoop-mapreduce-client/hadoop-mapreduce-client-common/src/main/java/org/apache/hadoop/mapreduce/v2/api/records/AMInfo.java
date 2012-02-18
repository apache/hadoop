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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;

public interface AMInfo {
  public ApplicationAttemptId getAppAttemptId();
  public long getStartTime();
  public ContainerId getContainerId();
  public String getNodeManagerHost();
  public int getNodeManagerPort();
  public int getNodeManagerHttpPort();

  public void setAppAttemptId(ApplicationAttemptId appAttemptId);
  public void setStartTime(long startTime);
  public void setContainerId(ContainerId containerId);
  public void setNodeManagerHost(String nmHost);
  public void setNodeManagerPort(int nmPort);
  public void setNodeManagerHttpPort(int mnHttpPort);
}