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

import static org.apache.hadoop.yarn.util.StringHelper.ujoin;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import java.io.IOException;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
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

    private final Context nmContext;

    @Inject
    public ContainerBlock(Context nmContext) {
      this.nmContext = nmContext;
    }

    @Override
    protected void render(Block html) {
      ContainerId containerID;
      try {
        containerID = ConverterUtils.toContainerId($(CONTAINER_ID));
      } catch (IOException e) {
        html.p()._("Invalid containerId " + $(CONTAINER_ID))._();
        return;
      }

      DIV<Hamlet> div = html.div("#content");
      Container container = this.nmContext.getContainers().get(containerID);
      if (container == null) {
        div.h1("Unknown Container. Container might have completed, "
                + "please go back to the previous page and retry.")._();
        return;
      }
      ContainerStatus containerData = container.cloneAndGetContainerStatus();
      int exitCode = containerData.getExitStatus();
      String exiStatus = 
          (exitCode == YarnConfiguration.INVALID_CONTAINER_EXIT_STATUS) ? 
              "N/A" : String.valueOf(exitCode);
      info("Container information")
        ._("ContainerID", $(CONTAINER_ID))
        ._("ContainerState", container.getContainerState())
        ._("ExitStatus", exiStatus)
        ._("Diagnostics", containerData.getDiagnostics())
        ._("User", container.getUser())
        ._("TotalMemoryNeeded",
            container.getLaunchContext().getResource().getMemory())
        ._("logs", ujoin("containerlogs", $(CONTAINER_ID)), "Link to logs");
      html._(InfoBlock.class);
    }
  }
}
