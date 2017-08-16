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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for the resource profiles manager. Provides an interface to get
 * the list of available profiles and some helper functions.
 */
@Public
@Unstable
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
   */
  Resource getProfile(String profile);

  /**
   * Get all supported resource profiles.
   * @return a map of resource objects associated with each profile
   */
  Map<String, Resource> getResourceProfiles();

  /**
   * Reload profiles based on updated configuration.
   * @throws IOException when invalid resource profile names are loaded
   */
  void reloadProfiles() throws IOException;

  /**
   * Get default supported resource profile.
   * @return resource object which is default
   */
  Resource getDefaultProfile();

  /**
   * Get minimum supported resource profile.
   * @return resource object which is minimum
   */
  Resource getMinimumProfile();

  /**
   * Get maximum supported resource profile.
   * @return resource object which is maximum
   */
  Resource getMaximumProfile();
}
