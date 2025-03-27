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
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.List;
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

    String federationText = isYarnFederationEnabled() ? "Federation" : "LocalCluster";

    Hamlet.UL<Hamlet.DIV<Hamlet>> mainList = html.div("#nav").
        h3("Cluster").
        ul().
        li().a(url(""), "About").__().
        li().a(url("federation"), federationText).__();

    List<String> subClusterIds = getActiveSubClusterIds();

    // ### nodes info
    initNodesMenu(mainList, subClusterIds);

    // ### nodelabels info
    initNodeLabelsMenu(mainList, subClusterIds);

    // ### applications info
    initApplicationsMenu(mainList, subClusterIds);

    // ### tools
    Hamlet.DIV<Hamlet> sectionBefore = mainList.__();
    Configuration conf = new Configuration();
    Hamlet.UL<Hamlet.DIV<Hamlet>> tools = WebPageUtils.appendToolSection(sectionBefore, conf);

    if (tools == null) {
      return;
    }

    tools.__().__();
  }
}
