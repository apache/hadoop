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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;

public class MockLocalizerHeartbeatResponse
    implements LocalizerHeartbeatResponse {

  LocalizerAction action;
  List<ResourceLocalizationSpec> resourceSpecs;

  MockLocalizerHeartbeatResponse() {
    resourceSpecs = new ArrayList<ResourceLocalizationSpec>();
  }

  MockLocalizerHeartbeatResponse(
      LocalizerAction action, List<ResourceLocalizationSpec> resources) {
    this.action = action;
    this.resourceSpecs = resources;
  }

  public LocalizerAction getLocalizerAction() { return action; }
  public void setLocalizerAction(LocalizerAction action) {
    this.action = action;
  }

  @Override
  public List<ResourceLocalizationSpec> getResourceSpecs() {
    return resourceSpecs;
}

  @Override
  public void setResourceSpecs(List<ResourceLocalizationSpec> resourceSpecs) {
    this.resourceSpecs = resourceSpecs;
  }
}
