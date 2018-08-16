/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.manager;

import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.HomeAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.router.UniformRandomRouterPolicy;
import org.junit.Before;

/**
 * Simple test of {@link HomePolicyManager}.
 */
public class TestHomePolicyManager extends BasePolicyManagerTest {

  @Before
  public void setup() {

    wfp = new HomePolicyManager();

    //set expected params that the base test class will use for tests
    expectedPolicyManager = HomePolicyManager.class;
    expectedAMRMProxyPolicy = HomeAMRMProxyPolicy.class;
    expectedRouterPolicy = UniformRandomRouterPolicy.class;
  }
}
