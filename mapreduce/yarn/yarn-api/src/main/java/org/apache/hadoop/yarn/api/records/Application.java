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

public interface Application {

  ApplicationId getApplicationId();
  void setApplicationId(ApplicationId applicationId);

  String getUser();
  void setUser(String user);

  String getQueue();
  void setQueue(String queue);

  String getName();
  void setName(String name);

  ApplicationStatus getStatus();
  void setStatus(ApplicationStatus status);

  ApplicationState getState();
  void setState(ApplicationState state);

  Container getMasterContainer();
  void setMasterContainer(Container masterContainer);

  String getDiagnostics();
  void setDiagnostics(String diagnostics);

  String getTrackingUrl();
  void setTrackingUrl(String url);
}
