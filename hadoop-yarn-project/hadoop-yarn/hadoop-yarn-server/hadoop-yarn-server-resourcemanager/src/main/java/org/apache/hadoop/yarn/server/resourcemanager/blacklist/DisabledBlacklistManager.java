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

package org.apache.hadoop.yarn.server.resourcemanager.blacklist;

import java.util.ArrayList;

import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;

/**
 * A {@link BlacklistManager} that returns no blacklists.
 */
public class DisabledBlacklistManager implements BlacklistManager {

  private static final ArrayList<String> EMPTY_LIST = new ArrayList<String>();
  private ResourceBlacklistRequest noBlacklist =
      ResourceBlacklistRequest.newInstance(EMPTY_LIST, EMPTY_LIST);

  @Override
  public void addNode(String node) {
  }

  @Override
  public ResourceBlacklistRequest getBlacklistUpdates() {
    return noBlacklist;
  }

  @Override
  public void refreshNodeHostCount(int nodeHostCount) {
    // Do nothing
  }
}