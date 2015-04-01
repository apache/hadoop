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

import static org.apache.hadoop.yarn.util.StringHelper.join;

import java.util.Collection;

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfo;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.LI;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;

public class FairSchedulerPage extends RmView {
  static final String _Q = ".ui-state-default.ui-corner-all";
  static final float Q_MAX_WIDTH = 0.8f;
  static final float Q_STATS_POS = Q_MAX_WIDTH + 0.05f;
  static final String Q_END = "left:101%";
  static final String Q_GIVEN = "left:0%;background:none;border:1px solid rgba(0,0,0,1)";
  static final String Q_INSTANTANEOUS_FS = "left:0%;background:none;border:1px dashed rgba(0,0,0,1)";
  static final String Q_OVER = "background:rgba(255, 140, 0, 0.8)";
  static final String Q_UNDER = "background:rgba(50, 205, 50, 0.8)";
  static final String STEADY_FAIR_SHARE = "Steady Fair Share";
  static final String INSTANTANEOUS_FAIR_SHARE = "Instantaneous Fair Share";
  @RequestScoped
  static class FSQInfo {
    FairSchedulerQueueInfo qinfo;
  }
  
  static class LeafQueueBlock extends HtmlBlock {
    final FairSchedulerLeafQueueInfo qinfo;

    @Inject LeafQueueBlock(ViewContext ctx, FSQInfo info) {
      super(ctx);
      qinfo = (FairSchedulerLeafQueueInfo)info.qinfo;
    }

    @Override
    protected void render(Block html) {
      ResponseInfo ri = info("\'" + qinfo.getQueueName() + "\' Queue Status").
          _("Used Resources:", qinfo.getUsedResources().toString()).
          _("Num Active Applications:", qinfo.getNumActiveApplications()).
          _("Num Pending Applications:", qinfo.getNumPendingApplications()).
          _("Min Resources:", qinfo.getMinResources().toString()).
          _("Max Resources:", qinfo.getMaxResources().toString());
      int maxApps = qinfo.getMaxApplications();
      if (maxApps < Integer.MAX_VALUE) {
          ri._("Max Running Applications:", qinfo.getMaxApplications());
      }
      ri._(STEADY_FAIR_SHARE + ":", qinfo.getSteadyFairShare().toString());
      ri._(INSTANTANEOUS_FAIR_SHARE + ":", qinfo.getFairShare().toString());
      html._(InfoBlock.class);

      // clear the info contents so this queue's info doesn't accumulate into another queue's info
      ri.clear();
    }
  }
  
  static class QueueBlock extends HtmlBlock {
    final FSQInfo fsqinfo;

    @Inject QueueBlock(FSQInfo info) {
      fsqinfo = info;
    }

    @Override
    public void render(Block html) {
      Collection<FairSchedulerQueueInfo> subQueues = fsqinfo.qinfo.getChildQueues();
      UL<Hamlet> ul = html.ul("#pq");
      for (FairSchedulerQueueInfo info : subQueues) {
        float capacity = info.getMaxResourcesFraction();
        float steadyFairShare = info.getSteadyFairShareMemoryFraction();
        float instantaneousFairShare = info.getFairShareMemoryFraction();
        float used = info.getUsedMemoryFraction();
        LI<UL<Hamlet>> li = ul.
          li().
            a(_Q).$style(width(capacity * Q_MAX_WIDTH)).
              $title(join(join(STEADY_FAIR_SHARE + ":", percent(steadyFairShare)),
                  join(" " + INSTANTANEOUS_FAIR_SHARE + ":", percent(instantaneousFairShare)))).
              span().$style(join(Q_GIVEN, ";font-size:1px;", width(steadyFairShare / capacity))).
                _('.')._().
              span().$style(join(Q_INSTANTANEOUS_FS, ";font-size:1px;",
                  width(instantaneousFairShare/capacity))).
                _('.')._().
              span().$style(join(width(used/capacity),
                ";font-size:1px;left:0%;", used > instantaneousFairShare ? Q_OVER : Q_UNDER)).
                _('.')._().
              span(".q", info.getQueueName())._().
            span().$class("qstats").$style(left(Q_STATS_POS)).
              _(join(percent(used), " used"))._();

        fsqinfo.qinfo = info;
        if (info instanceof FairSchedulerLeafQueueInfo) {
          li.ul("#lq").li()._(LeafQueueBlock.class)._()._();
        } else {
          li._(QueueBlock.class);
        }
        li._();
      }

      ul._();
    }
  }
  
  static class QueuesBlock extends HtmlBlock {
    final FairScheduler fs;
    final FSQInfo fsqinfo;
    
    @Inject QueuesBlock(ResourceManager rm, FSQInfo info) {
      fs = (FairScheduler)rm.getResourceScheduler();
      fsqinfo = info;
    }

    @Override
    public void render(Block html) {
      html._(MetricsOverviewTable.class);
      UL<DIV<DIV<Hamlet>>> ul = html.
        div("#cs-wrapper.ui-widget").
          div(".ui-widget-header.ui-corner-top").
            _("Application Queues")._().
          div("#cs.ui-widget-content.ui-corner-bottom").
            ul();
      if (fs == null) {
        ul.
          li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(Q_END)._("100% ")._().
              span(".q", "default")._()._();
      } else {
        FairSchedulerInfo sinfo = new FairSchedulerInfo(fs);
        fsqinfo.qinfo = sinfo.getRootQueueInfo();
        float used = fsqinfo.qinfo.getUsedMemoryFraction();

        ul.
          li().$style("margin-bottom: 1em").
            span().$style("font-weight: bold")._("Legend:")._().
            span().$class("qlegend ui-corner-all").$style(Q_GIVEN).
              $title("The steady fair shares consider all queues, " +
                  "both active (with running applications) and inactive.").
              _(STEADY_FAIR_SHARE)._().
            span().$class("qlegend ui-corner-all").$style(Q_INSTANTANEOUS_FS).
              $title("The instantaneous fair shares consider only active " +
                  "queues (with running applications).").
              _(INSTANTANEOUS_FAIR_SHARE)._().
            span().$class("qlegend ui-corner-all").$style(Q_UNDER).
              _("Used")._().
            span().$class("qlegend ui-corner-all").$style(Q_OVER).
              _("Used (over fair share)")._().
            span().$class("qlegend ui-corner-all ui-state-default").
              _("Max Capacity")._().
        _().
          li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(join(width(used), ";left:0%;",
                  used > 1 ? Q_OVER : Q_UNDER))._(".")._().
              span(".q", "root")._().
            span().$class("qstats").$style(left(Q_STATS_POS)).
              _(join(percent(used), " used"))._().
            _(QueueBlock.class)._();
      }
      ul._()._().
      script().$type("text/javascript").
          _("$('#cs').hide();")._()._().
      _(FairSchedulerAppsBlock.class);
    }
  }
  
  @Override protected void postHead(Page.HTML<_> html) {
    html.
      style().$type("text/css").
        _("#cs { padding: 0.5em 0 1em 0; margin-bottom: 1em; position: relative }",
          "#cs ul { list-style: none }",
          "#cs a { font-weight: normal; margin: 2px; position: relative }",
          "#cs a span { font-weight: normal; font-size: 80% }",
          "#cs-wrapper .ui-widget-header { padding: 0.2em 0.5em }",
          ".qstats { font-weight: normal; font-size: 80%; position: absolute }",
          ".qlegend { font-weight: normal; padding: 0 1em; margin: 1em }",
          "table.info tr th {width: 50%}")._(). // to center info table
      script("/static/jt/jquery.jstree.js").
      script().$type("text/javascript").
        _("$(function() {",
          "  $('#cs a span').addClass('ui-corner-all').css('position', 'absolute');",
          "  $('#cs').bind('loaded.jstree', function (e, data) {",
          "    var callback = { call:reopenQueryNodes }",
          "    data.inst.open_node('#pq', callback);",
          "   }).",
          "    jstree({",
          "    core: { animation: 188, html_titles: true },",
          "    plugins: ['themeroller', 'html_data', 'ui'],",
          "    themeroller: { item_open: 'ui-icon-minus',",
          "      item_clsd: 'ui-icon-plus', item_leaf: 'ui-icon-gear'",
          "    }",
          "  });",
          "  $('#cs').bind('select_node.jstree', function(e, data) {",
          "    var queues = $('.q', data.rslt.obj);",
          "    var q = '^' + queues.first().text();",
          "    q += queues.length == 1 ? '$' : '\\\\.';",
          "    $('#apps').dataTable().fnFilter(q, 4, true);",
          "  });",
          "  $('#cs').show();",
          "});")._().
        _(SchedulerPageUtil.QueueBlockUtil.class);
  }
  
  @Override protected Class<? extends SubView> content() {
    return QueuesBlock.class;
  }

  @Override
  protected String initAppsTable() {
    return WebPageUtils.appsTableInit(true);
  }

  static String percent(float f) {
    return String.format("%.1f%%", f * 100);
  }

  static String width(float f) {
    return String.format("width:%.1f%%", f * 100);
  }

  static String left(float f) {
    return String.format("left:%.1f%%", f * 100);
  }
}
