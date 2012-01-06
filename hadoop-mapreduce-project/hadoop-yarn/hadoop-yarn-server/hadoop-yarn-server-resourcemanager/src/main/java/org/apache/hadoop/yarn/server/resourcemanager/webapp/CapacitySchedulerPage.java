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

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
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

class CapacitySchedulerPage extends RmView {
  static final String _Q = ".ui-state-default.ui-corner-all";
  static final float WIDTH_F = 0.8f;
  static final String Q_END = "left:101%";
  static final String OVER = "font-size:1px;background:rgba(255, 140, 0, 0.8)";
  static final String UNDER = "font-size:1px;background:rgba(50, 205, 50, 0.8)";
  static final float EPSILON = 1e-8f;

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
          _("Capacity:", percent(lqinfo.getCapacity() / 100)).
          _("Max Capacity:", percent(lqinfo.getMaxCapacity() / 100)).
          _("Used Capacity:", percent(lqinfo.getUsedCapacity() / 100)).
          _("Absolute Capacity:", percent(lqinfo.getAbsoluteCapacity() / 100)).
          _("Absolute Max Capacity:", percent(lqinfo.getAbsoluteMaxCapacity() / 100)).
          _("Utilization:", percent(lqinfo.getUtilization() / 100)).
          _("Used Resources:", lqinfo.getUsedResources().toString()).
          _("Num Active Applications:", Integer.toString(lqinfo.getNumActiveApplications())).
          _("Num Pending Applications:", Integer.toString(lqinfo.getNumPendingApplications())).
          _("Num Containers:", Integer.toString(lqinfo.getNumContainers())).
          _("Max Applications:", Integer.toString(lqinfo.getMaxApplications())).
          _("Max Applications Per User:", Integer.toString(lqinfo.getMaxApplicationsPerUser())).
          _("Max Active Applications:", Integer.toString(lqinfo.getMaxActiveApplications())).
          _("Max Active Applications Per User:", Integer.toString(lqinfo.getMaxActiveApplicationsPerUser())).
          _("User Limit:", Integer.toString(lqinfo.getUserLimit()) + "%").
          _("User Limit Factor:", String.format("%.1f", lqinfo.getUserLimitFactor()));

      html._(InfoBlock.class);

      // clear the info contents so this queue's info doesn't accumulate into another queue's info
      ri.clear();
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
          (csqinfo.qinfo == null) ? csqinfo.csinfo.getSubQueues()
              : csqinfo.qinfo.getSubQueues();
      UL<Hamlet> ul = html.ul();
      for (CapacitySchedulerQueueInfo info : subQueues) {
        float used = info.getUsedCapacity() / 100;
        float set = info.getCapacity() / 100;
        float delta = Math.abs(set - used) + 0.001f;
        float max = info.getMaxCapacity() / 100;
        LI<UL<Hamlet>> li = ul.
          li().
            a(_Q).$style(width(max * WIDTH_F)).
              $title(join("used:", percent(used), " set:", percent(set),
                          " max:", percent(max))).
              //span().$style(Q_END)._(absMaxPct)._().
              span().$style(join(width(delta/max), ';',
                used > set ? OVER : UNDER, ';',
                used > set ? left(set/max) : left(used/max)))._('.')._().
              span(".q", info.getQueuePath().substring(5))._();

        csqinfo.qinfo = info;
        if (info.getSubQueues() == null) {
          li.ul("#lq").li()._(LeafQueueInfoBlock.class)._()._();
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
            a(_Q).$style(width(WIDTH_F)).
              span().$style(Q_END)._("100% ")._().
              span(".q", "default")._()._();
      } else {
        CSQueue root = cs.getRootQueue();
        CapacitySchedulerInfo sinfo = new CapacitySchedulerInfo(root);
        csqinfo.csinfo = sinfo;
        csqinfo.qinfo = null;

        float used = sinfo.getUsedCapacity() / 100;
        float set = sinfo.getCapacity() / 100;
        float delta = Math.abs(set - used) + 0.001f;
        ul.
          li().
            a(_Q).$style(width(WIDTH_F)).
              $title(join("used:", percent(used))).
              span().$style(Q_END)._("100%")._().
              span().$style(join(width(delta), ';', used > set ? OVER : UNDER,
                ';', used > set ? left(set) : left(used)))._(".")._().
              span(".q", "root")._().
            _(QueueBlock.class)._();
      }
      ul._()._().
      script().$type("text/javascript").
          _("$('#cs').hide();")._()._().
      _(AppsBlock.class);
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
          "table.info tr th {width: 50%}")._(). // to center info table
      script("/static/jt/jquery.jstree.js").
      script().$type("text/javascript").
        _("$(function() {",
          "  $('#cs a span').addClass('ui-corner-all').css('position', 'absolute');",
          "  $('#cs').bind('loaded.jstree', function (e, data) {",
          "    data.inst.open_all();",
          "    data.inst.close_node('#lq', true);",
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
          "    $('#apps').dataTable().fnFilter(q, 3, true);",
          "  });",
          "  $('#cs').show();",
          "});")._();
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
