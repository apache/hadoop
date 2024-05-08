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
package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import com.google.inject.Inject;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * Overview block for the GPG Policies Web UI.
 */
public class GPGPoliciesBlock extends HtmlBlock {

  private final GlobalPolicyGenerator gpg;

  private final FederationStateStoreFacade facade;

  @Inject
  GPGPoliciesBlock(GlobalPolicyGenerator gpg, ViewContext ctx) {
    super(ctx);
    this.gpg = gpg;
    this.facade = FederationStateStoreFacade.getInstance(gpg.getConfig());
  }

  @Override
  protected void render(Block html) {
    try {
      Collection<SubClusterPolicyConfiguration> policies =
          facade.getPoliciesConfigurations().values();
      initYarnFederationPolicies(policies, html);
    } catch (Exception e) {
      LOG.error("Get GPGPolicies Error.", e);
    }
  }

  private void initYarnFederationPolicies(Collection<SubClusterPolicyConfiguration> policies,
      Block html) throws FederationPolicyInitializationException {

    Hamlet.TBODY<Hamlet.TABLE<Hamlet>> tbody = html.table("#policies").
        thead().
        tr().
        th(".queue", "Queue Name").
        th(".policyType", "Policy Type").
        th(".routerPolicyWeights", "Router PolicyWeights").
        th(".amrmPolicyWeights", "Router AMRMPolicyWeights").
        th(".headroomAlpha", "Router Headroom Alpha").
        __().__().
        tbody();

    if (policies != null) {
      for (SubClusterPolicyConfiguration policy : policies) {
        Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Hamlet>>> row = tbody.tr().td(policy.getQueue());
        // Policy Type
        String type = policy.getType();
        row = row.td(type);

        // WeightedPolicyInfo
        ByteBuffer params = policy.getParams();
        WeightedPolicyInfo weightedPolicyInfo = WeightedPolicyInfo.fromByteBuffer(params);
        row = row.td(policyWeight2String(weightedPolicyInfo.getRouterPolicyWeights()));
        row = row.td(policyWeight2String(weightedPolicyInfo.getAMRMPolicyWeights()));
        row.td(String.valueOf(weightedPolicyInfo.getHeadroomAlpha())).__();
      }
    }

    tbody.__().__();
  }

  /**
   * We will convert the PolicyWeight to string format.
   *
   * @param weights PolicyWeight.
   * @return string format PolicyWeight. example: SC-1:0.91, SC-2:0.09
   */
  private String policyWeight2String(Map<SubClusterIdInfo, Float> weights) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<SubClusterIdInfo, Float> entry : weights.entrySet()) {
      sb.append(entry.getKey().toId()).append(": ").append(entry.getValue()).append(", ");
    }
    if (sb.length() > 2) {
      sb.setLength(sb.length() - 2);
    }
    return sb.toString();
  }
}
