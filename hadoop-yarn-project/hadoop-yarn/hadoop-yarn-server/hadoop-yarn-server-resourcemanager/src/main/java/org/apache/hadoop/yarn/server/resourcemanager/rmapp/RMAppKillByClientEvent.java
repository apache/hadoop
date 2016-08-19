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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import java.net.InetAddress;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * An event class that is used to help with logging information
 * when an application KILL event is needed.
 *
 */
public class RMAppKillByClientEvent extends RMAppEvent {

  private final UserGroupInformation callerUGI;
  private final InetAddress ip;

  /**
   * constructor to create an event used for logging during user driven kill
   * invocations.
   *
   * @param appId application id
   * @param diagnostics message about the kill event
   * @param callerUGI caller's user and group information
   * @param remoteIP ip address of the caller
   */
  public RMAppKillByClientEvent(ApplicationId appId, String diagnostics,
      UserGroupInformation callerUGI, InetAddress remoteIP) {
    super(appId, RMAppEventType.KILL, diagnostics);
    this.callerUGI = callerUGI;
    this.ip = remoteIP;
  }

  /**
   * returns the {@link UserGroupInformation} information.
   * @return UserGroupInformation
   */
  public final UserGroupInformation getCallerUGI() {
    return callerUGI;
  }

  /**
   * returns the ip address stored in this event.
   * @return remoteIP
   */
  public final InetAddress getIp() {
    return ip;
  }
}
