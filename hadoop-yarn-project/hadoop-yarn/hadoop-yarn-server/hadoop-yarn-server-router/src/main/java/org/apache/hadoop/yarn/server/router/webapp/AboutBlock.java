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

package org.apache.hadoop.yarn.server.router.webapp;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.webapp.dao.RouterInfo;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * About block for the Router Web UI.
 */
public class AboutBlock extends RouterBlock {

  private final Router router;

  @Inject
  AboutBlock(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
  }

  @Override
  protected void render(Block html) {

    boolean isEnabled = isYarnFederationEnabled();

    // If Yarn Federation is not enabled, the user needs to be prompted.
    initUserHelpInformationDiv(html, isEnabled);

    // Metrics Overview Table
    html.__(MetricsOverviewTable.class);

    // Init Yarn Router Basic Information
    initYarnRouterBasicInformation(isEnabled);

    // InfoBlock
    html.__(InfoBlock.class);
  }

  /**
   * Init Yarn Router Basic Infomation.
   * @param isEnabled true, federation is enabled; false, federation is not enabled.
   */
  private void initYarnRouterBasicInformation(boolean isEnabled) {
    FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance(router.getConfig());
    RouterInfo routerInfo = new RouterInfo(router);
    String lastStartTime =
        DateFormatUtils.format(routerInfo.getStartedOn(), DATE_PATTERN);
    try {
      info("Yarn Router Overview").
          __("Federation Enabled:", String.valueOf(isEnabled)).
          __("Router ID:", routerInfo.getClusterId()).
          __("Router state:", routerInfo.getState()).
          __("Router SubCluster Count:", facade.getSubClusters(true).size()).
          __("Router RMStateStore:", routerInfo.getRouterStateStore()).
          __("Router started on:", lastStartTime).
          __("Router version:", routerInfo.getRouterBuildVersion() +
             " on " + routerInfo.getRouterVersionBuiltOn()).
          __("Hadoop version:", routerInfo.getHadoopBuildVersion() +
             " on " + routerInfo.getHadoopVersionBuiltOn());
    } catch (YarnException e) {
      LOG.error("initYarnRouterBasicInformation error.", e);
    }
  }
}