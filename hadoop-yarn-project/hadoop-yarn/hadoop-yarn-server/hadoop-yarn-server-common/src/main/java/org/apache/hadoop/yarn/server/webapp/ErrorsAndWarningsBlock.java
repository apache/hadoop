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

package org.apache.hadoop.yarn.server.webapp;

import com.google.inject.Inject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.security.AdminACLsManager;
import org.apache.hadoop.yarn.util.Log4jWarningErrorMetricsAppender;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ErrorsAndWarningsBlock extends HtmlBlock {

  long cutoffPeriodSeconds;
  final private AdminACLsManager adminAclsManager;

  @Inject
  ErrorsAndWarningsBlock(ViewContext ctx, Configuration conf) {
    super(ctx);
    // default is to show all errors and warnings
    cutoffPeriodSeconds = Time.now() / 1000;
    String value = ctx.requestContext().get("cutoff", "");
    try {
      cutoffPeriodSeconds = Integer.parseInt(value);
      if (cutoffPeriodSeconds <= 0) {
        cutoffPeriodSeconds = Time.now() / 1000;
      }
    } catch (NumberFormatException ne) {
      cutoffPeriodSeconds = Time.now() / 1000;
    }
    adminAclsManager = new AdminACLsManager(conf);
  }

  @Override
  protected void render(Block html) {
    Log log = LogFactory.getLog(ErrorsAndWarningsBlock.class);

    boolean isAdmin = false;
    UserGroupInformation callerUGI = this.getCallerUGI();

    if (adminAclsManager.areACLsEnabled()) {
      if (callerUGI != null && adminAclsManager.isAdmin(callerUGI)) {
        isAdmin = true;
      }
    } else {
      isAdmin = true;
    }

    if (!isAdmin) {
      html.div().p()._("This page is for admins only.")._()._();
      return;
    }

    if (log instanceof Log4JLogger) {
      html._(ErrorMetrics.class);
      html._(WarningMetrics.class);
      html.div().button().$onclick("reloadPage()").b("View data for the last ")
        ._().select().$id("cutoff").option().$value("60")._("1 min")._()
        .option().$value("300")._("5 min")._().option().$value("900")
        ._("15 min")._().option().$value("3600")._("1 hour")._().option()
        .$value("21600")._("6 hours")._().option().$value("43200")
        ._("12 hours")._().option().$value("86400")._("24 hours")._()._()._();

      String script = "function reloadPage() {"
          + " var timePeriod = $(\"#cutoff\").val();"
          + " document.location.href = '/cluster/errors-and-warnings?cutoff=' + timePeriod"
          + "}";
      script =  script
          + "; function toggleContent(element) {"
          + "  $(element).parent().siblings('.toggle-content').fadeToggle();"
          + "}";

      html.script().$type("text/javascript")._(script)._();

      html.style(".toggle-content { display: none; }");

      Log4jWarningErrorMetricsAppender appender =
          Log4jWarningErrorMetricsAppender.findAppender();
      if (appender == null) {
        return;
      }
      List<Long> cutoff = new ArrayList<>();
      Hamlet.TBODY<Hamlet.TABLE<Hamlet>> errorsTable =
          html.table("#messages").thead().tr().th(".message", "Message")
            .th(".type", "Type").th(".count", "Count")
            .th(".lasttime", "Latest Message Time")._()._().tbody();

      // cutoff has to be in seconds
      cutoff.add((Time.now() - cutoffPeriodSeconds * 1000) / 1000);
      List<Map<String, Log4jWarningErrorMetricsAppender.Element>> errorsData =
          appender.getErrorMessagesAndCounts(cutoff);
      List<Map<String, Log4jWarningErrorMetricsAppender.Element>> warningsData =
          appender.getWarningMessagesAndCounts(cutoff);
      Map<String, List<Map<String, Log4jWarningErrorMetricsAppender.Element>>> sources =
          new HashMap<>();
      sources.put("Error", errorsData);
      sources.put("Warning", warningsData);

      int maxDisplayLength = 80;
      for (Map.Entry<String, List<Map<String, Log4jWarningErrorMetricsAppender.Element>>> source : sources
        .entrySet()) {
        String type = source.getKey();
        List<Map<String, Log4jWarningErrorMetricsAppender.Element>> data =
            source.getValue();
        if (data.size() > 0) {
          Map<String, Log4jWarningErrorMetricsAppender.Element> map = data.get(0);
          for (Map.Entry<String, Log4jWarningErrorMetricsAppender.Element> entry : map
            .entrySet()) {
            String message = entry.getKey();
            Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Hamlet>>> row =
                errorsTable.tr();
            Hamlet.TD<Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Hamlet>>>> cell =
                row.td();
            if (message.length() > maxDisplayLength || message.contains("\n")) {
              String displayMessage = entry.getKey().split("\n")[0];
              if (displayMessage.length() > maxDisplayLength) {
                displayMessage = displayMessage.substring(0, maxDisplayLength);
              }

              cell.pre().a().$href("#").$onclick("toggleContent(this);")
                .$style("white-space: pre")._(displayMessage)._()._().div()
                .$class("toggle-content").pre()._(message)._()._()._();
            } else {
              cell.pre()._(message)._()._();
            }
            Log4jWarningErrorMetricsAppender.Element ele = entry.getValue();
            row.td(type).td(String.valueOf(ele.count))
              .td(Times.format(ele.timestampSeconds * 1000))._();
          }
        }
      }
      errorsTable._()._();
    }
  }

  public static class MetricsBase extends HtmlBlock {
    List<Long> cutoffs;
    List<Integer> values;
    String tableHeading;
    Log4jWarningErrorMetricsAppender appender;

    MetricsBase(ViewContext ctx) {
      super(ctx);
      cutoffs = new ArrayList<>();

      // cutoff has to be in seconds
      long now = Time.now();
      cutoffs.add((now - 60 * 1000) / 1000);
      cutoffs.add((now - 300 * 1000) / 1000);
      cutoffs.add((now - 900 * 1000) / 1000);
      cutoffs.add((now - 3600 * 1000) / 1000);
      cutoffs.add((now - 21600 * 1000) / 1000);
      cutoffs.add((now - 43200 * 1000) / 1000);
      cutoffs.add((now - 84600 * 1000) / 1000);

      Log log = LogFactory.getLog(ErrorsAndWarningsBlock.class);
      if (log instanceof Log4JLogger) {
        appender =
            Log4jWarningErrorMetricsAppender.findAppender();
      }
    }

    List<Long> getCutoffs() {
      return this.cutoffs;
    }

    @Override
    protected void render(Block html) {
      Log log = LogFactory.getLog(ErrorsAndWarningsBlock.class);
      if (log instanceof Log4JLogger) {
        Hamlet.DIV<Hamlet> div =
            html.div().$class("metrics").$style("padding-bottom: 20px");
        div.h3(tableHeading).table("#metricsoverview").thead()
          .$class("ui-widget-header").tr().th().$class("ui-state-default")
          ._("Last 1 minute")._().th().$class("ui-state-default")
          ._("Last 5 minutes")._().th().$class("ui-state-default")
          ._("Last 15 minutes")._().th().$class("ui-state-default")
          ._("Last 1 hour")._().th().$class("ui-state-default")
          ._("Last 6 hours")._().th().$class("ui-state-default")
          ._("Last 12 hours")._().th().$class("ui-state-default")
          ._("Last 24 hours")._()._()._().tbody().$class("ui-widget-content")
          .tr().td(String.valueOf(values.get(0)))
          .td(String.valueOf(values.get(1))).td(String.valueOf(values.get(2)))
          .td(String.valueOf(values.get(3))).td(String.valueOf(values.get(4)))
          .td(String.valueOf(values.get(5))).td(String.valueOf(values.get(6)))
          ._()._()._();
        div._();
      }
    }
  }

  public static class ErrorMetrics extends MetricsBase {

    @Inject
    ErrorMetrics(ViewContext ctx) {
      super(ctx);
      tableHeading = "Error Metrics";
    }

    @Override
    protected void render(Block html) {
      if (appender == null) {
        return;
      }
      values = appender.getErrorCounts(getCutoffs());
      super.render(html);
    }
  }

  public static class WarningMetrics extends MetricsBase {

    @Inject
    WarningMetrics(ViewContext ctx) {
      super(ctx);
      tableHeading = "Warning Metrics";
    }

    @Override
    protected void render(Block html) {
      if (appender == null) {
        return;
      }
      values = appender.getWarningCounts(getCutoffs());
      super.render(html);
    }
  }
}