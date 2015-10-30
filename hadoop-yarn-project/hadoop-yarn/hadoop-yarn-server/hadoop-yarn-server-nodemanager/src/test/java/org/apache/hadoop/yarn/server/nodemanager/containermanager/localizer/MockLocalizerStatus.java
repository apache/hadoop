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

import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;

public class MockLocalizerStatus implements LocalizerStatus {

  private String locId;
  private List<LocalResourceStatus> stats;

  public MockLocalizerStatus() {
    stats = new ArrayList<LocalResourceStatus>();
  }

  public MockLocalizerStatus(String locId, List<LocalResourceStatus> stats) {
    this.locId = locId;
    this.stats = stats;
  }

  @Override
  public String getLocalizerId() { return locId; }
  @Override
  public List<LocalResourceStatus> getResources() { return stats; }
  @Override
  public void setLocalizerId(String id) { this.locId = id; }
  @Override
  public void addAllResources(List<LocalResourceStatus> rsrcs) {
    stats.addAll(rsrcs);
  }
  @Override
  public LocalResourceStatus getResourceStatus(int index) {
    return stats.get(index);
  }
  @Override
  public void addResourceStatus(LocalResourceStatus resource) {
    stats.add(resource);
  }
  @Override
  public void removeResource(int index) {
    stats.remove(index);
  }
  public void clearResources() { stats.clear(); }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MockLocalizerStatus)) {
      return false;
    }
    MockLocalizerStatus other = (MockLocalizerStatus) o;
    return getLocalizerId().equals(other.getLocalizerId())
      && getResources().containsAll(other.getResources())
      && other.getResources().containsAll(getResources());
  }

  @Override
  public int hashCode() {
    return 4344;
  }

}
