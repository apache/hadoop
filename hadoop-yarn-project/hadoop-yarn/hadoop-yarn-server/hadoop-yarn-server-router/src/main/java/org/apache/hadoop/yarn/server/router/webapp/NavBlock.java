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

import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Navigation block for the Router Web UI.
 */
public class NavBlock extends RouterBlock {

  private Router router;

  @Inject
  public NavBlock(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
  }

  @Override
  public void render(Block html) {

    Hamlet.UL<Hamlet.DIV<Hamlet>> mainList = html.div("#nav").
        h3("Cluster").
        ul().
        li().a(url(""), "About").__().
        li().a(url("federation"), "Federation").__();

    List<String> subclusters = getSubClusters();

    Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>> subAppsList1 =
        mainList.li().a(url("nodes"), "Nodes").ul().$style("padding:0.3em 1em 0.1em 2em");

    // ### nodes info
    subAppsList1.li().__();
    for (String subcluster : subclusters) {
      subAppsList1.li().a(url("nodes", subcluster), subcluster).__();
    }
    subAppsList1.__().__();

    // ### applications info
    mainList.li().a(url("apps"), "Applications").__();

    // ### tools
    Hamlet.DIV<Hamlet> sectionBefore = mainList.__();
    Configuration conf = new Configuration();
    Hamlet.UL<Hamlet.DIV<Hamlet>> tools = WebPageUtils.appendToolSection(sectionBefore, conf);

    if (tools == null) {
      return;
    }

    tools.__().__();
  }

  private List<String> getSubClusters() {
    List<String> result = new ArrayList<>();
    try {
      FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance();
      Map<SubClusterId, SubClusterInfo> subClustersInfo = facade.getSubClusters(true);
      subClustersInfo.values().stream().forEach(subClusterInfo -> {
        result.add(subClusterInfo.getSubClusterId().getId());
      });
    } catch (Exception e) {
      LOG.error("getSubClusters error.", e);
    }
    return result;
  }
}
