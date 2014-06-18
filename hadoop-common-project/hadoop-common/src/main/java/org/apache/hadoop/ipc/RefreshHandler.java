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
package org.apache.hadoop.ipc;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Used to registry custom methods to refresh at runtime.
 */
@InterfaceStability.Unstable
public interface RefreshHandler {
  /**
   * Implement this method to accept refresh requests from the administrator.
   * @param identifier is the identifier you registered earlier
   * @param args contains a list of string args from the administrator
   * @throws Exception as a shorthand for a RefreshResponse(-1, message)
   * @return a RefreshResponse
   */
  RefreshResponse handleRefresh(String identifier, String[] args);
}
