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

package org.apache.hadoop.yarn.server.resourcemanager.resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YARNFeatureNotEnabledException;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for the resource profiles manager. Provides an interface to get
 * the list of available profiles and some helper functions.
 */
public interface ResourceProfilesManager {

  /**
   * Method to handle all initialization steps for ResourceProfilesManager.
   * @param config Configuration object
   * @throws IOException when invalid resource profile names are loaded
   */
  void init(Configuration config) throws IOException;

  /**
   * Get the resource capability associated with given profile name.
   * @param profile name of resource profile
   * @return resource capability for given profile
   *
   * @throws YarnException when any invalid profile name or feature is disabled
   */
  Resource getProfile(String profile) throws YarnException;

  /**
   * Get all supported resource profiles.
   * @return a map of resource objects associated with each profile
   *
   * @throws YARNFeatureNotEnabledException when feature is disabled
   */
  Map<String, Resource> getResourceProfiles() throws
      YARNFeatureNotEnabledException;

  /**
   * Reload profiles based on updated configuration.
   * @throws IOException when invalid resource profile names are loaded
   */
  void reloadProfiles() throws IOException;

  /**
   * Get default supported resource profile.
   * @return resource object which is default
   * @throws YarnException when any invalid profile name or feature is disabled
   */
  Resource getDefaultProfile() throws YarnException;

  /**
   * Get minimum supported resource profile.
   * @return resource object which is minimum
   * @throws YarnException when any invalid profile name or feature is disabled
   */
  Resource getMinimumProfile() throws YarnException;

  /**
   * Get maximum supported resource profile.
   * @return resource object which is maximum
   * @throws YarnException when any invalid profile name or feature is disabled
   */
  Resource getMaximumProfile() throws YarnException;
}
