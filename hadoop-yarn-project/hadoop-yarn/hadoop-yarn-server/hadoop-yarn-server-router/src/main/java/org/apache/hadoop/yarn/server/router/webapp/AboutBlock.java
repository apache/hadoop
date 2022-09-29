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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.webapp.dao.RouterInfo;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    Configuration conf = this.router.getConfig();
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    boolean isEnabled = conf.getBoolean(
        YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);

    FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance();
    try {
      initTestFederationSubCluster(facade);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (YarnException e) {
      e.printStackTrace();
    }

    // If Yarn Federation is not enabled, the user needs to be prompted.
    initUserHelpInformationDiv(html, isEnabled);

    // Metrics Overview Table
    html.__(MetricsOverviewTable.class);

    // Init Yarn Router Basic Information
    initYarnRouterBasicInfomation(isEnabled);

    // InfoBlock
    html.__(InfoBlock.class);
  }

  /**
   * Init Yarn Router Basic Infomation.
   * @param isEnabled true, federation is enabled; false, federation is not enabled.
   */
  private void initYarnRouterBasicInfomation(boolean isEnabled) {
    FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance();
    RouterInfo routerInfo = new RouterInfo(router);
    String lastStartTime =
        DateFormatUtils.format(routerInfo.getStartedOn(), DATE_PATTERN);
    try {
      info("Yarn Router Overview").
          __("Federation Enabled:", String.valueOf(isEnabled)).
          __("Router ID:", routerInfo.getClusterId()).
          __("Router state:", routerInfo.getState()).
          __("Router SubCluster Count:", facade.getSubClusters(false).size()).
          __("Router RMStateStore:", routerInfo.getRouterStateStore()).
          __("Router started on:", lastStartTime).
          __("Router version:", routerInfo.getRouterBuildVersion() +
             " on " + routerInfo.getRouterVersionBuiltOn()).
          __("Hadoop version:", routerInfo.getHadoopBuildVersion() +
             " on " + routerInfo.getHadoopVersionBuiltOn());
    } catch (YarnException e) {
      LOG.error("initYarnRouterBasicInfomation error.", e);
    }
  }

  public void initTestFederationSubCluster(FederationStateStoreFacade facade)
      throws IOException, InterruptedException, YarnException {

    // Initialize subcluster information
    String scAmRMAddress = "5.6.7.8:5";
    String scClientRMAddress = "5.6.7.8:6";
    String scRmAdminAddress = "5.6.7.8:7";
    String scWebAppAddress = "127.0.0.1:8080";

    // Initialize subcluster capability
    String[] capabilityPathItems = new String[] {".", "target", "test-classes", "capability"};
    String capabilityPath = StringUtils.join(capabilityPathItems, File.separator);
    String capabilityJson =
            FileUtils.readFileToString(new File(capabilityPath), StandardCharsets.UTF_8);

    // capability json needs to remove asflicense
    String regex = "\"___asflicense__.*\\n(.*,\\n){1,15}.*\\n.*";
    Pattern p = Pattern.compile(regex);
    Matcher m = p.matcher(capabilityJson);
    capabilityJson = m.replaceAll("").trim();

    // Initialize subcluster sc1
    SubClusterInfo sc1 =
            SubClusterInfo.newInstance(SubClusterId.newInstance("SC-1"),
                    scAmRMAddress, scClientRMAddress, scRmAdminAddress, scWebAppAddress,
                    SubClusterState.SC_RUNNING, Time.now(), capabilityJson);
    Thread.sleep(100);
    sc1.setLastHeartBeat(Time.now());

    // Initialize subcluster sc2
    SubClusterInfo sc2 =
            SubClusterInfo.newInstance(SubClusterId.newInstance("SC-2"),
                    scAmRMAddress, scClientRMAddress, scRmAdminAddress, scWebAppAddress,
                    SubClusterState.SC_RUNNING, Time.now(), capabilityJson);
    Thread.sleep(200);
    sc2.setLastHeartBeat(Time.now());

    FederationStateStore stateStore = facade.getStateStore();
    stateStore.registerSubCluster(SubClusterRegisterRequest.newInstance(sc1));
    stateStore.registerSubCluster(SubClusterRegisterRequest.newInstance(sc2));
  }
}