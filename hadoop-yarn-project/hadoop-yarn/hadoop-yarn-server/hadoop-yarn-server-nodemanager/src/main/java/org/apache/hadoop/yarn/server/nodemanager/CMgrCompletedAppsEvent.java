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

package org.apache.hadoop.yarn.server.nodemanager;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEventType;

public class CMgrCompletedAppsEvent extends ContainerManagerEvent {

  private final List<ApplicationId> appsToCleanup;
  private final Reason reason;

  public CMgrCompletedAppsEvent(List<ApplicationId> appsToCleanup, Reason reason) {
    super(ContainerManagerEventType.FINISH_APPS);
    this.appsToCleanup = appsToCleanup;
    this.reason = reason;
  }

  public List<ApplicationId> getAppsToCleanup() {
    return this.appsToCleanup;
  }

  public Reason getReason() {
    return reason;
  }

  public enum Reason {
    /**
     * Application is killed as NodeManager is shut down
     */
    ON_SHUTDOWN, 

    /**
     * Application is killed by ResourceManager
     */
    BY_RESOURCEMANAGER
  }
}
