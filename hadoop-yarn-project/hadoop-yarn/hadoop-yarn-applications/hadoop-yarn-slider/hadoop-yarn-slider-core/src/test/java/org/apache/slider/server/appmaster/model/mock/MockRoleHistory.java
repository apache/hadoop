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

package org.apache.slider.server.appmaster.model.mock;

import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.apache.slider.server.appmaster.state.RoleStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * Subclass to enable access to some of the protected methods.
 */
public class MockRoleHistory extends RoleHistory {

  /**
   * Take a list of provider roles and build the history from them,
   * dynamically creating the role status entries on the way.
   * @param providerRoles provider role list
   * @throws BadConfigException configuration problem with the role list
   */
  public MockRoleHistory(List<ProviderRole> providerRoles) throws
      BadConfigException {
    super(convertRoles(providerRoles), new MockClusterServices());
  }

  static List<RoleStatus> convertRoles(List<ProviderRole> providerRoles) {
    List<RoleStatus> statuses = new ArrayList<>();
    for (ProviderRole role : providerRoles) {
      statuses.add(new RoleStatus(role));
    }
    return statuses;
  }

}
