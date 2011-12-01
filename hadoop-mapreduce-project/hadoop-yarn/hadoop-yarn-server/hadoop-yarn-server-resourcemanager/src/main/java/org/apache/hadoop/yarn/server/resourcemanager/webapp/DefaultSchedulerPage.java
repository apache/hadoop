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

import com.google.inject.Inject;

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.*;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import static org.apache.hadoop.yarn.util.StringHelper.*;

class DefaultSchedulerPage extends RmView {
  static final String _Q = ".ui-state-default.ui-corner-all";
  static final float WIDTH_F = 0.8f;
  static final String Q_END = "left:101%";
  static final String OVER = "font-size:1px;background:rgba(255, 140, 0, 0.8)";
  static final String UNDER = "font-size:1px;background:rgba(50, 205, 50, 0.8)";
  static final float EPSILON = 1e-8f;

  static class QueueInfoBlock extends HtmlBlock {
    final RMContext rmContext;
    final FifoScheduler fs;
    final String qName;
    final QueueInfo qInfo;

    @Inject QueueInfoBlock(RMContext context, ViewContext ctx, ResourceManager rm) {
      super(ctx);
      this.rmContext = context;

      fs = (FifoScheduler) rm.getResourceScheduler();
      qName = fs.getQueueInfo("",false,false).getQueueName();
      qInfo = fs.getQueueInfo(qName,true,true);
    }

    @Override public void render(Block html) {
      String minmemoryresource = 
                Integer.toString(fs.getMinimumResourceCapability().getMemory());
      String maxmemoryresource = 
                Integer.toString(fs.getMaximumResourceCapability().getMemory());
      String qstate = (qInfo.getQueueState() == QueueState.RUNNING) ?
                       "Running" :
                           (qInfo.getQueueState() == QueueState.STOPPED) ?
                                  "Stopped" : "Unknown";

      int usedNodeMem      = 0;
      int availNodeMem     = 0;
      int totNodeMem       = 0;
      int nodeContainers   = 0;

      for (RMNode ni : this.rmContext.getRMNodes().values()) {
        SchedulerNodeReport report = fs.getNodeReport(ni.getNodeID());
        usedNodeMem += report.getUsedResource().getMemory();
        availNodeMem += report.getAvailableResource().getMemory();
        totNodeMem += ni.getTotalCapability().getMemory();
        nodeContainers += fs.getNodeReport(ni.getNodeID()).getNumContainers();
      }

      info("\'" + qName + "\' Queue Status").
        _("Queue State:" , qstate).
        _("Minimum Queue Memory Capacity:" , minmemoryresource).
        _("Maximum Queue Memory Capacity:" , maxmemoryresource).
        _("Number of Nodes:" , Integer.toString(this.rmContext.getRMNodes().size())).
        _("Used Node Capacity:" , Integer.toString(usedNodeMem)).
        _("Available Node Capacity:" , Integer.toString(availNodeMem)).
        _("Total Node Capacity:" , Integer.toString(totNodeMem)).
        _("Number of Node Containers:" , Integer.toString(nodeContainers));

      html._(InfoBlock.class);
    }
  }

  static class QueuesBlock extends HtmlBlock {
    final FifoScheduler fs;
    final String qName;
    final QueueInfo qInfo;

    @Inject QueuesBlock(ResourceManager rm) {
      fs = (FifoScheduler) rm.getResourceScheduler();
      qName = fs.getQueueInfo("",false,false).getQueueName();
      qInfo = fs.getQueueInfo(qName,false,false);
    }

    @Override
    public void render(Block html) {
      html._(MetricsOverviewTable.class);
      UL<DIV<DIV<Hamlet>>> ul = html.
        div("#cs-wrapper.ui-widget").
          div(".ui-widget-header.ui-corner-top").
            _("FifoScheduler Queue")._().
          div("#cs.ui-widget-content.ui-corner-bottom").
            ul();

      if (fs == null) {
        ul.
          li().
            a(_Q).$style(width(WIDTH_F)).
              span().$style(Q_END)._("100% ")._().
              span(".q", "default")._()._();
      } else {
        float used = qInfo.getCurrentCapacity();
        float set = qInfo.getCapacity();
        float delta = Math.abs(set - used) + 0.001f;
        ul.
          li().
            a(_Q).$style(width(WIDTH_F)).
              $title(join("used:", percent(used))).
              span().$style(Q_END)._("100%")._().
              span().$style(join(width(delta), ';', used > set ? OVER : UNDER,
                ';', used > set ? left(set) : left(used)))._(".")._().
              span(".q", qName)._().
            _(QueueInfoBlock.class)._();
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
          "    data.inst.open_all(); }).",
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
          "    $('#apps').dataTable().fnFilter(q, 3);",
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
