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

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Log4jWarningErrorMetricsAppender;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import static org.apache.hadoop.util.GenericsUtil.isLog4jLogger;

public class NavBlock extends HtmlBlock {

  @Override
  public void render(Block html) {
    boolean addErrorsAndWarningsLink = false;
    if (isLog4jLogger(NavBlock.class)) {
      Log4jWarningErrorMetricsAppender appender =
          Log4jWarningErrorMetricsAppender.findAppender();
      if (appender != null) {
        addErrorsAndWarningsLink = true;
      }
    }
    Hamlet.DIV<Hamlet> nav = html.
        div("#nav").
            h3("Application History").
                ul().
                    li().a(url("about"), "About").
                    _().
                    li().a(url("apps"), "Applications").
                        ul().
                            li().a(url("apps",
                                YarnApplicationState.FINISHED.toString()),
                                YarnApplicationState.FINISHED.toString()).
                            _().
                            li().a(url("apps",
                                YarnApplicationState.FAILED.toString()),
                                YarnApplicationState.FAILED.toString()).
                            _().
                            li().a(url("apps",
                                YarnApplicationState.KILLED.toString()),
                                YarnApplicationState.KILLED.toString()).
                            _().
                        _().
                    _().
                _();

    Hamlet.UL<Hamlet.DIV<Hamlet>> tools = nav.h3("Tools").ul();
    tools.li().a("/conf", "Configuration")._()
        .li().a("/logs", "Local logs")._()
        .li().a("/stacks", "Server stacks")._()
        .li().a("/jmx?qry=Hadoop:*", "Server metrics")._();

    if (addErrorsAndWarningsLink) {
      tools.li().a(url("errors-and-warnings"), "Errors/Warnings")._();
    }
    tools._()._();
  }
}
