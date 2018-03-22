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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import java.util.Map.Entry;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.BODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class AllContainersPage extends NMView {

  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    setTitle("All containers running on this node");
    set(DATATABLES_ID, "containers");
    set(initID(DATATABLES, "containers"), containersTableInit());
    setTableStyles(html, "containers");
  }

  private String containersTableInit() {
    return tableInit().
        // containerid, executiontype, containerid, log-url
        append(", aoColumns:[").append(getContainersIdColumnDefs())
        .append(", null, null, {bSearchable:false}]} ").toString();
  }

  private String getContainersIdColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("{'sType':'natural', 'aTargets': [0]")
        .append(", 'mRender': parseHadoopID }").toString();
  }
  @Override
  protected Class<? extends SubView> content() {
    return AllContainersBlock.class;
  }

  public static class AllContainersBlock extends HtmlBlock implements
      YarnWebParams {

    private final Context nmContext;

    @Inject
    public AllContainersBlock(Context nmContext) {
      this.nmContext = nmContext;
    }

    @Override
    protected void render(Block html) {
      TBODY<TABLE<BODY<Hamlet>>> tableBody = html.body()
        .table("#containers")
          .thead()
            .tr()
              .td().__("ContainerId").__()
              .td().__("ExecutionType").__()
              .td().__("ContainerState").__()
              .td().__("logs").__()
            .__()
          .__().tbody();
      for (Entry<ContainerId, Container> entry : this.nmContext
          .getContainers().entrySet()) {
        ContainerInfo info = new ContainerInfo(this.nmContext, entry.getValue());
        tableBody
          .tr()
            .td().a(url("container", info.getId()), info.getId())
            .__()
            .td().__(info.getExecutionType()).__()
            .td().__(info.getState()).__()
            .td()
                .a(url(info.getShortLogLink()), "logs").__()
          .__();
      }
      tableBody.__().__().__();
    }

  }
}
