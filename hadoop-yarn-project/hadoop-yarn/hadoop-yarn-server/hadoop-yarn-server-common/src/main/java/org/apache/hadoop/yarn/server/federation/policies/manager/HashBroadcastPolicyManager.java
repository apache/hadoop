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

import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.BroadcastAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.router.HashBasedRouterPolicy;

/**
 * Policy that routes applications via hashing of their queuename, and broadcast
 * resource requests. This picks a {@link HashBasedRouterPolicy} for the router
 * and a {@link BroadcastAMRMProxyPolicy} for the amrmproxy as they are designed
 * to work together.
 */
public class HashBroadcastPolicyManager extends AbstractPolicyManager {

  public HashBroadcastPolicyManager() {
    // this structurally hard-codes two compatible policies for Router and
    // AMRMProxy.
    routerFederationPolicy = HashBasedRouterPolicy.class;
    amrmProxyFederationPolicy = BroadcastAMRMProxyPolicy.class;
  }

}
