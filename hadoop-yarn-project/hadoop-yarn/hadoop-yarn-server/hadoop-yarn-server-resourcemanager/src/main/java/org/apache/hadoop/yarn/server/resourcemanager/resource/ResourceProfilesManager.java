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

import java.io.IOException;
import java.util.Map;

/**
 * Interface for the resource profiles manager. Provides an interface to get
 * the list of available profiles and some helper functions.
 */
public interface ResourceProfilesManager {

  void init(Configuration config) throws IOException;

  Resource getProfile(String profile);

  Map<String, Resource> getResourceProfiles();

  void reloadProfiles() throws IOException;

  Resource getDefaultProfile();

  Resource getMinimumProfile();

  Resource getMaximumProfile();
}
