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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.util.StringHelper.ujoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class ContainerPage extends NMView implements NMWebParams {

  @Override
  protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    setTitle("Container " + $(CONTAINER_ID));
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  @Override
  protected Class<? extends SubView> content() {
    return ContainerBlock.class;
  }

  public static class ContainerBlock extends HtmlBlock implements NMWebParams {

    private final Configuration conf;
    private final Context nmContext;
    private final RecordFactory recordFactory;

    @Inject
    public ContainerBlock(Configuration conf, Context nmContext) {
      this.conf = conf;
      this.nmContext = nmContext;
      this.recordFactory = RecordFactoryProvider.getRecordFactory(this.conf);
    }

    @Override
    protected void render(Block html) {
      ContainerId containerID =
        ConverterUtils.toContainerId(this.recordFactory, $(CONTAINER_ID));
      Container container = this.nmContext.getContainers().get(containerID);
      ContainerStatus containerData = container.cloneAndGetContainerStatus();
      info("Container information")
        ._("ContainerID", $(CONTAINER_ID))
        ._("ContainerState", container.getContainerState())
        ._("ExitStatus", containerData.getExitStatus())
        ._("Diagnostics", containerData.getDiagnostics())
        ._("User", container.getUser())
        ._("TotalMemoryNeeded",
            container.getLaunchContext().getResource().getMemory())
        ._("logs", ujoin("containerlogs", $(CONTAINER_ID)), "Link to logs");
      html._(InfoBlock.class);
    }
  }
}
