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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Log4jWarningErrorMetricsAppender;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.LI;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import static org.apache.hadoop.util.GenericsUtil.isLog4jLogger;

public class NavBlock extends HtmlBlock {

  @Override public void render(Block html) {
    boolean addErrorsAndWarningsLink = false;
    if (isLog4jLogger(NavBlock.class)) {
      Log4jWarningErrorMetricsAppender appender =
          Log4jWarningErrorMetricsAppender.findAppender();
      if (appender != null) {
        addErrorsAndWarningsLink = true;
      }
    }
    UL<DIV<Hamlet>> mainList = html.
      div("#nav").
        h3("Cluster").
        ul().
          li().a(url("cluster"), "About").__().
          li().a(url("nodes"), "Nodes").__().
          li().a(url("nodelabels"), "Node Labels").__();
    UL<LI<UL<DIV<Hamlet>>>> subAppsList = mainList.
          li().a(url("apps"), "Applications").
            ul();
    subAppsList.li().__();
    for (YarnApplicationState state : YarnApplicationState.values()) {
      subAppsList.
              li().a(url("apps", state.toString()), state.toString()).__();
    }
    subAppsList.__().__();
    UL<DIV<Hamlet>> tools = mainList.
          li().a(url("scheduler"), "Scheduler").__().__().
        h3("Tools").ul();
    tools.li().a("/conf", "Configuration").__().
          li().a("/logs", "Local logs").__().
          li().a("/stacks", "Server stacks").__().
          li().a("/jmx?qry=Hadoop:*", "Server metrics").__();

    if (addErrorsAndWarningsLink) {
      tools.li().a(url("errors-and-warnings"), "Errors/Warnings").__();
    }
    tools.__().__();
  }
}
