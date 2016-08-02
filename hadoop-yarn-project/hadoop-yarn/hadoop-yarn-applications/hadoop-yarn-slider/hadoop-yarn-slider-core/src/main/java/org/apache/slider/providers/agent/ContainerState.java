/*
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

package org.apache.slider.providers.agent;

/** The states a component instance can be. */
public enum ContainerState {
  INIT,           // Container is not net activated
  HEALTHY,     // Agent is heartbeating
  UNHEALTHY,      // Container is unhealthy - no heartbeat for some interval
  HEARTBEAT_LOST;  // Container is lost - request a new instance

  /**
   * Indicates whether or not it is a valid state to produce a command.
   *
   * @return true if command can be issued for this state.
   */
  public boolean canIssueCommands() {
    switch (this) {
      case HEALTHY:
        return true;
      default:
        return false;
    }
  }
}
