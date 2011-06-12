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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * The events for application master that are generated within the applications
 * manager.
 */
@Private
@Evolving
public class ApplicationMasterEvents {
  public enum SNEventType {
    SCHEDULE,
    RELEASE,
    CLEANUP
  };

  public enum AMLauncherEventType {
    LAUNCH,
    CLEANUP
  };

  /* event generated for components tracking application adding/tracking/removal */
  public enum ApplicationTrackerEventType {
    ADD,
    REMOVE,
    EXPIRE
  }
  
  public enum ApplicationEventType {

    // Source: ApplicationMasterService -> ASM -> AMTracker-self
    REGISTERED,
    STATUSUPDATE,
    FINISH, // Also by AMLauncher

    // Source: SchedulerNegotiator.
    ALLOCATED,
    RELEASED,

    // Source: ASM -> AMTracker
    ALLOCATE, // Also AMTracker->Self
    FAILED,
    RECOVER,

    // TODO: Nobody Uses!
    REMOVE,
    CLEANUP,

    // Source: AMLauncher
    LAUNCHED,
    LAUNCH_FAILED,

    // Source: AMTracker: Self-event
    LAUNCH,
    FAILED_MAX_RETRIES,

    // Source: AMLivelinessMonitor -> AMTracker
    EXPIRE,

    // Source: ClientRMService -> ASM -> AMTracker
    KILL
  };
}
