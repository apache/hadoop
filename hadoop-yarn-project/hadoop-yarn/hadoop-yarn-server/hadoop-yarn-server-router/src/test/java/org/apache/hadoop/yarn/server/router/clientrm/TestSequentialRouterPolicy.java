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

package org.apache.hadoop.yarn.server.router.clientrm;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.AbstractRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestSequentialRouterPolicy extends AbstractRouterPolicy {

  @Override
  public void reinitialize(FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {
    FederationPolicyInitializationContextValidator.validate(policyContext,
        this.getClass().getCanonicalName());
    setPolicyContext(policyContext);
  }

  @Override
  protected SubClusterId chooseSubCluster(String queue,
      Map<SubClusterId, SubClusterInfo> preSelectSubClusters) throws YarnException {
    /**
      * This strategy is only suitable for testing. We need to obtain subClusters sequentially.
      * We have 3 subClusters, 1 goodSubCluster and 2 badSubClusters.
      * The sc-id of goodSubCluster is 0, and the sc-id of badSubCluster is 1 and 2.
      * We hope Return in reverse order, that is, return 2, 1, 0
      * Return to badCluster first.
      */
    List<SubClusterId> subClusterIds = new ArrayList<>(preSelectSubClusters.keySet());
    if(subClusterIds.size() > 1){
      subClusterIds.sort((o1, o2) -> Integer.parseInt(o2.getId()) - Integer.parseInt(o1.getId()));
    }
    if(CollectionUtils.isNotEmpty(subClusterIds)){
      return subClusterIds.get(0);
    }
    return null;
  }
}
