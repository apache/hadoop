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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.util.Log4jWarningErrorMetricsAppender;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

import static org.apache.hadoop.util.GenericsUtil.isLog4jLogger;

public class NavBlock extends HtmlBlock implements YarnWebParams {

  private Configuration conf;

  @Inject
  public NavBlock(Configuration conf) {
	 this.conf = conf;
  }
  
  @Override
  protected void render(Block html) {

    boolean addErrorsAndWarningsLink = false;
    if (isLog4jLogger(NMErrorsAndWarningsPage.class)) {
      Log4jWarningErrorMetricsAppender appender = Log4jWarningErrorMetricsAppender.findAppender();
      if (appender != null) {
        addErrorsAndWarningsLink = true;
      }
    }
	
    String RMWebAppURL =
        WebAppUtils.getResolvedRMWebAppURLWithScheme(this.conf);
    UL<DIV<Hamlet>> rmSection = html.
            div("#nav").
            h3("accordion-parent", "ResourceManager").
            ul("container").
            li().a("content", RMWebAppURL, "RM Home").__();
    UL<DIV<Hamlet>> nmSection = rmSection.
            __().
            h3("accordion-parent", "NodeManager").
            ul("container").
            li().a("content", url("node"), "Node Information").__().
            li().a("content", url("allApplications"), "List of Applications").__().
            li().a("content", url("allContainers"), "List of Containers").__();

    DIV<Hamlet> nav = nmSection.__();
    UL<DIV<Hamlet>> tools = WebPageUtils.appendToolSection(nav, conf);

    if (tools == null) {
      return;
    }
    if (addErrorsAndWarningsLink) {
      tools.li().a(url("errors-and-warnings"), "Errors/Warnings").__();
    }
    tools.__().__();
  }

}
