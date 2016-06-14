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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class ApplicationPage extends NMView implements YarnWebParams {

  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);

    set(DATATABLES_ID, "containers");
    set(initID(DATATABLES, "containers"), containersTableInit());
    setTableStyles(html, "containers");
  }

  private String containersTableInit() {
    return tableInit().append(",aoColumns:[null]}").toString();
  }

  @Override
  protected Class<? extends SubView> content() {
    return ApplicationBlock.class;
  }

  public static class ApplicationBlock extends HtmlBlock implements
      YarnWebParams {

    private final Context nmContext;
    private final Configuration conf;
    private final RecordFactory recordFactory;

    @Inject
    public ApplicationBlock(Context nmContext, Configuration conf) {
      this.conf = conf;
      this.nmContext = nmContext;
      this.recordFactory = RecordFactoryProvider.getRecordFactory(this.conf);
    }

    @Override
    protected void render(Block html) {
      ApplicationId applicationID;
      try {
        applicationID = ApplicationId.fromString($(APPLICATION_ID));
      } catch (IllegalArgumentException e) {
        html.p()._("Invalid Application Id " + $(APPLICATION_ID))._();
        return;
      }
      DIV<Hamlet> div = html.div("#content");
      Application app = this.nmContext.getApplications().get(applicationID);
      if (app == null) {
        div.h1("Unknown application with id " + applicationID
            + ". Application might have been completed")._();
        return;
      }
      AppInfo info = new AppInfo(app);
      info("Application's information")
            ._("ApplicationId", info.getId())
            ._("ApplicationState", info.getState())
            ._("User", info.getUser());
      TABLE<Hamlet> containersListBody = html._(InfoBlock.class)
          .table("#containers");
      for (String containerIdStr : info.getContainers()) {
        containersListBody
               .tr().td()
                 .a(url("container", containerIdStr), containerIdStr)
                 ._()._();
      }
      containersListBody._();
    }
  }
}
