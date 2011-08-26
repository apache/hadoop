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
import com.google.inject.servlet.RequestScoped;

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.Queue;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.*;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import static org.apache.hadoop.yarn.util.StringHelper.*;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

class CapacitySchedulerPage extends RmView {
  static final String _Q = ".ui-state-default.ui-corner-all";
  static final float WIDTH_F = 0.8f;
  static final String Q_END = "left:101%";
  static final String OVER = "font-size:1px;background:rgba(255, 140, 0, 0.8)";
  static final String UNDER = "font-size:1px;background:rgba(50, 205, 50, 0.8)";
  static final float EPSILON = 1e-8f;

  @RequestScoped
  static class Parent {
    Queue queue;
  }

  public static class QueueBlock extends HtmlBlock {
    final Parent parent;

    @Inject QueueBlock(Parent parent) {
      this.parent = parent;
    }

    @Override
    public void render(Block html) {
      UL<Hamlet> ul = html.ul();
      Queue parentQueue = parent.queue;
      for (Queue queue : parentQueue.getChildQueues()) {
        float used = queue.getUsedCapacity();
        float set = queue.getCapacity();
        float delta = Math.abs(set - used) + 0.001f;
        float max = queue.getMaximumCapacity();
        if (max < EPSILON || max > 1f) max = 1f;
        //String absMaxPct = percent(queue.getAbsoluteMaximumCapacity());
        LI<UL<Hamlet>> li = ul.
          li().
            a(_Q).$style(width(max * WIDTH_F)).
              $title(join("used:", percent(used), " set:", percent(set),
                          " max:", percent(max))).
              //span().$style(Q_END)._(absMaxPct)._().
              span().$style(join(width(delta/max), ';',
                used > set ? OVER : UNDER, ';',
                used > set ? left(set/max) : left(used/max)))._('.')._().
              span(".q", queue.getQueuePath().substring(5))._();
        if (queue instanceof ParentQueue) {
          parent.queue = queue;
          li.
            _(QueueBlock.class);
        }
        li._();
      }
      ul._();
    }
  }

  static class QueuesBlock extends HtmlBlock {
    final CapacityScheduler cs;
    final Parent parent;

    @Inject QueuesBlock(ResourceManager rm, Parent parent) {
      cs = (CapacityScheduler) rm.getResourceScheduler();
      this.parent = parent;
    }

    @Override
    public void render(Block html) {
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
        Queue root = cs.getRootQueue();
        parent.queue = root;
        float used = root.getUsedCapacity();
        float set = root.getCapacity();
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
          "#cs-wrapper .ui-widget-header { padding: 0.2em 0.5em }")._().
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
