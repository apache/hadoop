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

import java.util.ArrayList;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.webapp.AppsBlock;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.LI;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;

class CapacitySchedulerPage extends RmView {
  static final String _Q = ".ui-state-default.ui-corner-all";
  static final float Q_MAX_WIDTH = 0.8f;
  static final float Q_STATS_POS = Q_MAX_WIDTH + 0.05f;
  static final String Q_END = "left:101%";
  static final String Q_GIVEN = "left:0%;background:none;border:1px dashed rgba(0,0,0,0.25)";
  static final String Q_OVER = "background:rgba(255, 140, 0, 0.8)";
  static final String Q_UNDER = "background:rgba(50, 205, 50, 0.8)";

  @RequestScoped
  static class CSQInfo {
    CapacitySchedulerInfo csinfo;
    CapacitySchedulerQueueInfo qinfo;
  }

  static class LeafQueueInfoBlock extends HtmlBlock {
    final CapacitySchedulerLeafQueueInfo lqinfo;

    @Inject LeafQueueInfoBlock(ViewContext ctx, CSQInfo info) {
      super(ctx);
      lqinfo = (CapacitySchedulerLeafQueueInfo) info.qinfo;
    }

    @Override
    protected void render(Block html) {

      ResponseInfo ri = info("\'" + lqinfo.getQueuePath().substring(5) + "\' Queue Status").
          _("Queue State:", lqinfo.getQueueState()).
          _("Used Capacity:", percent(lqinfo.getUsedCapacity() / 100)).
          _("Absolute Used Capacity:", percent(lqinfo.getAbsoluteUsedCapacity() / 100)).
          _("Absolute Capacity:", percent(lqinfo.getAbsoluteCapacity() / 100)).
          _("Absolute Max Capacity:", percent(lqinfo.getAbsoluteMaxCapacity() / 100)).
          _("Used Resources:", lqinfo.getResourcesUsed().toString()).
          _("Num Schedulable Applications:", Integer.toString(lqinfo.getNumActiveApplications())).
          _("Num Non-Schedulable Applications:", Integer.toString(lqinfo.getNumPendingApplications())).
          _("Num Containers:", Integer.toString(lqinfo.getNumContainers())).
          _("Max Applications:", Integer.toString(lqinfo.getMaxApplications())).
          _("Max Applications Per User:", Integer.toString(lqinfo.getMaxApplicationsPerUser())).
          _("Max Application Master Resources:", lqinfo.getAMResourceLimit().toString()).
          _("Used Application Master Resources:", lqinfo.getUsedAMResource().toString()).
          _("Max Application Master Resources Per User:", lqinfo.getUserAMResourceLimit().toString()).
          _("Configured Capacity:", percent(lqinfo.getCapacity() / 100)).
          _("Configured Max Capacity:", percent(lqinfo.getMaxCapacity() / 100)).
          _("Configured Minimum User Limit Percent:", Integer.toString(lqinfo.getUserLimit()) + "%").
          _("Configured User Limit Factor:", String.format("%.1f", lqinfo.getUserLimitFactor())).
          _("Accessible Node Labels:", StringUtils.join(",", lqinfo.getNodeLabels())).
          _("Preemption:", lqinfo.getPreemptionDisabled() ? "disabled" : "enabled");

      html._(InfoBlock.class);

      // clear the info contents so this queue's info doesn't accumulate into another queue's info
      ri.clear();
    }
  }

  static class QueueUsersInfoBlock extends HtmlBlock {
    final CapacitySchedulerLeafQueueInfo lqinfo;

    @Inject
    QueueUsersInfoBlock(ViewContext ctx, CSQInfo info) {
      super(ctx);
      lqinfo = (CapacitySchedulerLeafQueueInfo) info.qinfo;
    }

    @Override
    protected void render(Block html) {
      TBODY<TABLE<Hamlet>> tbody =
          html.table("#userinfo").thead().$class("ui-widget-header").tr().th()
              .$class("ui-state-default")._("User Name")._().th()
              .$class("ui-state-default")._("Max Resource")._().th()
              .$class("ui-state-default")._("Used Resource")._().th()
              .$class("ui-state-default")._("Max AM Resource")._().th()
              .$class("ui-state-default")._("Used AM Resource")._().th()
              .$class("ui-state-default")._("Schedulable Apps")._().th()
              .$class("ui-state-default")._("Non-Schedulable Apps")._()._()._()
              .tbody();

      ArrayList<UserInfo> users = lqinfo.getUsers().getUsersList();
      for (UserInfo userInfo : users) {
        tbody.tr().td(userInfo.getUsername())
            .td(userInfo.getUserResourceLimit().toString())
            .td(userInfo.getResourcesUsed().toString())
            .td(lqinfo.getUserAMResourceLimit().toString())
            .td(userInfo.getAMResourcesUsed().toString())
            .td(Integer.toString(userInfo.getNumActiveApplications()))
            .td(Integer.toString(userInfo.getNumPendingApplications()))._();
      }

      html.div().$class("usersinfo").h5("Active Users Info")._();
      tbody._()._();
    }
  }

  public static class QueueBlock extends HtmlBlock {
    final CSQInfo csqinfo;

    @Inject QueueBlock(CSQInfo info) {
      csqinfo = info;
    }

    @Override
    public void render(Block html) {
      ArrayList<CapacitySchedulerQueueInfo> subQueues =
          (csqinfo.qinfo == null) ? csqinfo.csinfo.getQueues().getQueueInfoList()
              : csqinfo.qinfo.getQueues().getQueueInfoList();
      UL<Hamlet> ul = html.ul("#pq");
      for (CapacitySchedulerQueueInfo info : subQueues) {
        float used = info.getUsedCapacity() / 100;
        float absCap = info.getAbsoluteCapacity() / 100;
        float absMaxCap = info.getAbsoluteMaxCapacity() / 100;
        float absUsedCap = info.getAbsoluteUsedCapacity() / 100;
        LI<UL<Hamlet>> li = ul.
          li().
            a(_Q).$style(width(absMaxCap * Q_MAX_WIDTH)).
              $title(join("Absolute Capacity:", percent(absCap))).
              span().$style(join(Q_GIVEN, ";font-size:1px;", width(absCap/absMaxCap))).
                _('.')._().
              span().$style(join(width(absUsedCap/absMaxCap),
                ";font-size:1px;left:0%;", absUsedCap > absCap ? Q_OVER : Q_UNDER)).
                _('.')._().
              span(".q", info.getQueuePath().substring(5))._().
            span().$class("qstats").$style(left(Q_STATS_POS)).
              _(join(percent(used), " used"))._();

        csqinfo.qinfo = info;
        if (info.getQueues() == null) {
          li.ul("#lq").li()._(LeafQueueInfoBlock.class)._()._();
          li.ul("#lq").li()._(QueueUsersInfoBlock.class)._()._();
        } else {
          li._(QueueBlock.class);
        }
        li._();
      }

      ul._();
    }
  }

  static class QueuesBlock extends HtmlBlock {
    final CapacityScheduler cs;
    final CSQInfo csqinfo;

    @Inject QueuesBlock(ResourceManager rm, CSQInfo info) {
      cs = (CapacityScheduler) rm.getResourceScheduler();
      csqinfo = info;
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
      if (cs == null) {
        ul.
          li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(Q_END)._("100% ")._().
              span(".q", "default")._()._();
      } else {
        CSQueue root = cs.getRootQueue();
        CapacitySchedulerInfo sinfo = new CapacitySchedulerInfo(root);
        csqinfo.csinfo = sinfo;
        csqinfo.qinfo = null;

        float used = sinfo.getUsedCapacity() / 100;
        ul.
          li().$style("margin-bottom: 1em").
            span().$style("font-weight: bold")._("Legend:")._().
            span().$class("qlegend ui-corner-all").$style(Q_GIVEN).
              _("Capacity")._().
            span().$class("qlegend ui-corner-all").$style(Q_UNDER).
              _("Used")._().
            span().$class("qlegend ui-corner-all").$style(Q_OVER).
              _("Used (over capacity)")._().
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
      _(RMAppsBlock.class);
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
          "    var q = $('.q', data.rslt.obj).first().text();",
          "    if (q == 'root') q = '';",
          "    else q = '^' + q.substr(q.lastIndexOf('.') + 1) + '$';",
          "    $('#apps').dataTable().fnFilter(q, 4, true);",
          "  });",
          "  $('#cs').show();",
          "});")._().
      _(SchedulerPageUtil.QueueBlockUtil.class);
  }

  @Override protected Class<? extends SubView> content() {
    return QueuesBlock.class;
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
