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

package org.apache.hadoop.yarn.api.records;

//TODO: Split separate object for register, deregister and in-RM use.
public interface ApplicationMaster {
  ApplicationId getApplicationId();
  String getHost();
  int getRpcPort();
  String getTrackingUrl();
  ApplicationStatus getStatus();
  ApplicationState getState();
  String getClientToken();
  int getAMFailCount();
  int getContainerCount();
  String getDiagnostics();
  void setApplicationId(ApplicationId appId);
  void setHost(String host);
  void setRpcPort(int rpcPort);
  void setTrackingUrl(String url);
  void setStatus(ApplicationStatus status);
  void setState(ApplicationState state);
  void setClientToken(String clientToken);
  void setAMFailCount(int amFailCount);
  void setContainerCount(int containerCount);
  void setDiagnostics(String diagnostics);
}
