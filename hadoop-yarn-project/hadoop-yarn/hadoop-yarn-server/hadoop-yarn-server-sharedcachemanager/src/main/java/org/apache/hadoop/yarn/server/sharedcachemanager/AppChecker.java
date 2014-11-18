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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * An interface for checking whether an app is running so that the cleaner
 * service may determine if it can safely remove a cached entry.
 */
@Private
@Evolving
public abstract class AppChecker extends CompositeService {

  public AppChecker() {
    super("AppChecker");
  }

  public AppChecker(String name) {
    super(name);
  }

  /**
   * Returns whether the app is in an active state.
   * 
   * @return true if the app is found and is not in one of the completed states;
   *         false otherwise
   * @throws YarnException if there is an error in determining the app state
   */
  @Private
  public abstract boolean isApplicationActive(ApplicationId id)
      throws YarnException;

  /**
   * Returns the list of all active apps at the given time.
   * 
   * @return the list of active apps, or an empty list if there is none
   * @throws YarnException if there is an error in obtaining the list
   */
  @Private
  public abstract Collection<ApplicationId> getActiveApplications()
      throws YarnException;
}
