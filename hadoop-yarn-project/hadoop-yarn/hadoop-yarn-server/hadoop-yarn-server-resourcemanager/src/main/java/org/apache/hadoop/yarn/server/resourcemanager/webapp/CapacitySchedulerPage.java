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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.PartitionQueueCapacitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.PartitionResourcesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.webapp.AppBlock;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
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
  static final String Q_GIVEN =
      "left:0%;background:none;border:1px dashed #BFBFBF";
  static final String Q_OVER = "background:#FFA333";
  static final String Q_UNDER = "background:#5BD75B";

  @RequestScoped
  static class CSQInfo {
    CapacitySchedulerInfo csinfo;
    CapacitySchedulerQueueInfo qinfo;
    String label;
    boolean isExclusiveNodeLabel;
  }

  static class LeafQueueInfoBlock extends HtmlBlock {
    final CapacitySchedulerLeafQueueInfo lqinfo;
    private String nodeLabel;

    @Inject LeafQueueInfoBlock(ViewContext ctx, CSQInfo info) {
      super(ctx);
      lqinfo = (CapacitySchedulerLeafQueueInfo) info.qinfo;
      nodeLabel = info.label;
    }

    @Override
    protected void render(Block html) {
      if (nodeLabel == null) {
        renderLeafQueueInfoWithoutParition(html);
      } else {
        renderLeafQueueInfoWithPartition(html);
      }
    }

    private void renderLeafQueueInfoWithPartition(Block html) {
      String nodeLabelDisplay = nodeLabel.length() == 0
          ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION : nodeLabel;
      // first display the queue's label specific details :
      ResponseInfo ri =
          info("\'" + lqinfo.getQueuePath().substring(5)
              + "\' Queue Status for Partition \'" + nodeLabelDisplay + "\'");
      renderQueueCapacityInfo(ri, nodeLabel);
      html._(InfoBlock.class);
      // clear the info contents so this queue's info doesn't accumulate into
      // another queue's info
      ri.clear();

      // second display the queue specific details :
      ri =
          info("\'" + lqinfo.getQueuePath().substring(5) + "\' Queue Status")
              ._("Queue State:", lqinfo.getQueueState());
      renderCommonLeafQueueInfo(ri);

      html._(InfoBlock.class);
      // clear the info contents so this queue's info doesn't accumulate into
      // another queue's info
      ri.clear();
    }

    private void renderLeafQueueInfoWithoutParition(Block html) {
      ResponseInfo ri =
          info("\'" + lqinfo.getQueuePath().substring(5) + "\' Queue Status")
              ._("Queue State:", lqinfo.getQueueState());
      renderQueueCapacityInfo(ri, "");
      renderCommonLeafQueueInfo(ri);
      html._(InfoBlock.class);
      // clear the info contents so this queue's info doesn't accumulate into
      // another queue's info
      ri.clear();
    }

    private void renderQueueCapacityInfo(ResponseInfo ri, String label) {
      PartitionQueueCapacitiesInfo capacities =
          lqinfo.getCapacities().getPartitionQueueCapacitiesInfo(label);
      PartitionResourcesInfo resourceUsages =
          lqinfo.getResources().getPartitionResourceUsageInfo(label);

      // Get UserInfo from first user to calculate AM Resource Limit per user.
      ResourceInfo userAMResourceLimit = null;
      ArrayList<UserInfo> usersList = lqinfo.getUsers().getUsersList();
      if (usersList.isEmpty()) {
        // If no users are present, consider AM Limit for that queue.
        userAMResourceLimit = resourceUsages.getAMLimit();
      } else {
        userAMResourceLimit = usersList.get(0)
            .getResourceUsageInfo().getPartitionResourceUsageInfo(label)
            .getAMLimit();
      }
      ResourceInfo amUsed = (resourceUsages.getAmUsed() == null)
          ? new ResourceInfo(Resources.none())
          : resourceUsages.getAmUsed();
      ri.
      _("Used Capacity:", percent(capacities.getUsedCapacity() / 100)).
      _("Configured Capacity:", percent(capacities.getCapacity() / 100)).
      _("Configured Max Capacity:", percent(capacities.getMaxCapacity() / 100)).
      _("Absolute Used Capacity:", percent(capacities.getAbsoluteUsedCapacity() / 100)).
      _("Absolute Configured Capacity:", percent(capacities.getAbsoluteCapacity() / 100)).
      _("Absolute Configured Max Capacity:", percent(capacities.getAbsoluteMaxCapacity() / 100)).
      _("Used Resources:", resourceUsages.getUsed().toString()).
      _("Configured Max Application Master Limit:", StringUtils.format("%.1f",
          capacities.getMaxAMLimitPercentage())).
      _("Max Application Master Resources:",
          resourceUsages.getAMLimit().toString()).
      _("Used Application Master Resources:",
          amUsed.toString()).
      _("Max Application Master Resources Per User:",
          userAMResourceLimit.toString());
    }

    private void renderCommonLeafQueueInfo(ResponseInfo ri) {
      ri.
      _("Num Schedulable Applications:", Integer.toString(lqinfo.getNumActiveApplications())).
      _("Num Non-Schedulable Applications:", Integer.toString(lqinfo.getNumPendingApplications())).
      _("Num Containers:", Integer.toString(lqinfo.getNumContainers())).
      _("Max Applications:", Integer.toString(lqinfo.getMaxApplications())).
      _("Max Applications Per User:", Integer.toString(lqinfo.getMaxApplicationsPerUser())).
      _("Configured Minimum User Limit Percent:", Integer.toString(lqinfo.getUserLimit()) + "%").
      _("Configured User Limit Factor:", lqinfo.getUserLimitFactor()).
      _("Accessible Node Labels:", StringUtils.join(",", lqinfo.getNodeLabels())).
      _("Ordering Policy: ", lqinfo.getOrderingPolicyInfo()).
      _("Preemption:", lqinfo.getPreemptionDisabled() ? "disabled" : "enabled").
      _("Default Node Label Expression:",
              lqinfo.getDefaultNodeLabelExpression() == null
                  ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION
                  : lqinfo.getDefaultNodeLabelExpression()).
      _("Default Application Priority:",
              Integer.toString(lqinfo.getDefaultApplicationPriority()));
    }
  }

  static class QueueUsersInfoBlock extends HtmlBlock {
    final CapacitySchedulerLeafQueueInfo lqinfo;
    private String nodeLabel;

    @Inject
    QueueUsersInfoBlock(ViewContext ctx, CSQInfo info) {
      super(ctx);
      lqinfo = (CapacitySchedulerLeafQueueInfo) info.qinfo;
      nodeLabel = info.label;
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
        ResourceInfo resourcesUsed = userInfo.getResourcesUsed();
        PartitionResourcesInfo resourceUsages = lqinfo
            .getResources()
            .getPartitionResourceUsageInfo((nodeLabel == null) ? "" : nodeLabel);
        if (nodeLabel != null) {
          resourcesUsed = userInfo.getResourceUsageInfo()
              .getPartitionResourceUsageInfo(nodeLabel).getUsed();
        }
        ResourceInfo amUsed = (resourceUsages.getAmUsed() == null)
            ? new ResourceInfo(Resources.none())
            : resourceUsages.getAmUsed();
        tbody.tr().td(userInfo.getUsername())
            .td(userInfo.getUserResourceLimit().toString())
            .td(resourcesUsed.toString())
            .td(resourceUsages.getAMLimit().toString())
            .td(amUsed.toString())
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
      ArrayList<CapacitySchedulerQueueInfo> subQueues = (csqinfo.qinfo == null)
          ? csqinfo.csinfo.getQueues().getQueueInfoList()
          : csqinfo.qinfo.getQueues().getQueueInfoList();

      UL<Hamlet> ul = html.ul("#pq");
      float used;
      float absCap;
      float absMaxCap;
      float absUsedCap;
      for (CapacitySchedulerQueueInfo info : subQueues) {
        String nodeLabel = (csqinfo.label == null) ? "" : csqinfo.label;
        //DEFAULT_NODE_LABEL_PARTITION is accessible to all queues
        //other exclsiveNodeLabels are accessible only if configured
        if (!nodeLabel.isEmpty()// i.e. its DEFAULT_NODE_LABEL_PARTITION
            && csqinfo.isExclusiveNodeLabel
            && !info.getNodeLabels().contains("*")
            && !info.getNodeLabels().contains(nodeLabel)) {
          continue;
        }
        PartitionQueueCapacitiesInfo partitionQueueCapsInfo = info
            .getCapacities().getPartitionQueueCapacitiesInfo(nodeLabel);
        used = partitionQueueCapsInfo.getUsedCapacity() / 100;
        absCap = partitionQueueCapsInfo.getAbsoluteCapacity() / 100;
        absMaxCap = partitionQueueCapsInfo.getAbsoluteMaxCapacity() / 100;
        absUsedCap = partitionQueueCapsInfo.getAbsoluteUsedCapacity() / 100;

        LI<UL<Hamlet>> li = ul.
          li().
            a(_Q).$style(width(absMaxCap * Q_MAX_WIDTH)).
              $title(join("Absolute Capacity:", percent(absCap))).
              span().$style(join(Q_GIVEN, ";font-size:1px;", width(absCap/absMaxCap))).
                _('.')._().
              span().$style(join(width(absUsedCap/absMaxCap),
                ";font-size:1px;left:0%;", absUsedCap > absCap ? Q_OVER : Q_UNDER)).
                _('.')._().
              span(".q", "Queue: "+info.getQueuePath().substring(5))._().
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
    private final ResourceManager rm;
    private List<RMNodeLabel> nodeLabelsInfo;

    @Inject QueuesBlock(ResourceManager rm, CSQInfo info) {
      cs = (CapacityScheduler) rm.getResourceScheduler();
      csqinfo = info;
      this.rm = rm;
      RMNodeLabelsManager nodeLabelManager =
          rm.getRMContext().getNodeLabelManager();
      nodeLabelsInfo = nodeLabelManager.pullRMNodeLabelsInfo();
    }

    @Override
    public void render(Block html) {
      html._(MetricsOverviewTable.class);

      UserGroupInformation callerUGI = this.getCallerUGI();
      boolean isAdmin = false;
      ApplicationACLsManager aclsManager = rm.getApplicationACLsManager();
      if (aclsManager.areACLsEnabled()) {
        if (callerUGI != null && aclsManager.isAdmin(callerUGI)) {
          isAdmin = true;
        }
      } else {
        isAdmin = true;
      }

      // only show button to dump CapacityScheduler debug logs to admins
      if (isAdmin) {
        html.div()
          .button()
          .$style(
              "border-style: solid; border-color: #000000; border-width: 1px;"
                  + " cursor: hand; cursor: pointer; border-radius: 4px")
          .$onclick("confirmAction()").b("Dump scheduler logs")._().select()
          .$id("time").option().$value("60")._("1 min")._().option()
          .$value("300")._("5 min")._().option().$value("600")._("10 min")._()
          ._()._();

        StringBuilder script = new StringBuilder();
        script
          .append("function confirmAction() {")
          .append(" b = confirm(\"Are you sure you wish to generate"
              + " scheduler logs?\");")
          .append(" if (b == true) {")
          .append(" var timePeriod = $(\"#time\").val();")
          .append(" $.ajax({")
          .append(" type: 'POST',")
          .append(" url: '/ws/v1/cluster/scheduler/logs',")
          .append(" contentType: 'text/plain',")
          .append(AppBlock.getCSRFHeaderString(rm.getConfig()))
          .append(" data: 'time=' + timePeriod,")
          .append(" dataType: 'text'")
          .append(" }).done(function(data){")
          .append(" setTimeout(function(){")
          .append(" alert(\"Scheduler log is being generated.\");")
          .append(" }, 1000);")
          .append(" }).fail(function(data){")
          .append(
              " alert(\"Scheduler log generation failed. Please check the"
                  + " ResourceManager log for more informtion.\");")
          .append(" console.log(data);").append(" });").append(" }")
          .append("}");

        html.script().$type("text/javascript")._(script.toString())._();
      }

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
          _();

        float used = 0;

        CSQueue root = cs.getRootQueue();
        CapacitySchedulerInfo sinfo = new CapacitySchedulerInfo(root, cs);
        csqinfo.csinfo = sinfo;

        boolean hasAnyLabelLinkedToNM = false;
        if (null != nodeLabelsInfo) {
          for (RMNodeLabel label : nodeLabelsInfo) {
            if (label.getLabelName().length() == 0) {
              // Skip DEFAULT_LABEL
              continue;
            }
            if (label.getNumActiveNMs() > 0) {
              hasAnyLabelLinkedToNM = true;
              break;
            }
          }
        }
        if (!hasAnyLabelLinkedToNM) {
          used = sinfo.getUsedCapacity() / 100;
          //label is not enabled in the cluster or there's only "default" label,
          ul.li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(join(width(used), ";left:0%;",
                  used > 1 ? Q_OVER : Q_UNDER))._(".")._().
              span(".q", "Queue: root")._().
            span().$class("qstats").$style(left(Q_STATS_POS)).
              _(join(percent(used), " used"))._().
            _(QueueBlock.class)._();
        } else {
          for (RMNodeLabel label : nodeLabelsInfo) {
            csqinfo.qinfo = null;
            csqinfo.label = label.getLabelName();
            csqinfo.isExclusiveNodeLabel = label.getIsExclusive();
            String nodeLabelDisplay = csqinfo.label.length() == 0
                ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION : csqinfo.label;
            PartitionQueueCapacitiesInfo capacities = sinfo.getCapacities()
                .getPartitionQueueCapacitiesInfo(csqinfo.label);
            used = capacities.getUsedCapacity() / 100;
            String partitionUiTag =
                "Partition: " + nodeLabelDisplay + " " + label.getResource();
            ul.li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(join(width(used), ";left:0%;",
                  used > 1 ? Q_OVER : Q_UNDER))._(".")._().
              span(".q", partitionUiTag)._().
            span().$class("qstats").$style(left(Q_STATS_POS)).
              _(join(percent(used), " used"))._()._();

            //for the queue hierarchy under label
            UL<Hamlet> underLabel = html.ul("#pq");
            underLabel.li().
            a(_Q).$style(width(Q_MAX_WIDTH)).
              span().$style(join(width(used), ";left:0%;",
                  used > 1 ? Q_OVER : Q_UNDER))._(".")._().
              span(".q", "Queue: root")._().
            span().$class("qstats").$style(left(Q_STATS_POS)).
              _(join(percent(used), " used"))._().
            _(QueueBlock.class)._()._();
          }
        }
      }
      ul._()._().
      script().$type("text/javascript").
          _("$('#cs').hide();")._()._().
      _(RMAppsBlock.class);
      html._(HealthBlock.class);
    }
  }

  public static class HealthBlock extends HtmlBlock {

    final CapacityScheduler cs;

    @Inject
    HealthBlock(ResourceManager rm) {
      cs = (CapacityScheduler) rm.getResourceScheduler();
    }

    @Override
    public void render(HtmlBlock.Block html) {
      SchedulerHealth healthInfo = cs.getSchedulerHealth();
      DIV<Hamlet> div = html.div("#health");
      div.h4("Aggregate scheduler counts");
      TBODY<TABLE<DIV<Hamlet>>> tbody =
          div.table("#lastrun").thead().$class("ui-widget-header").tr().th()
            .$class("ui-state-default")._("Total Container Allocations(count)")
            ._().th().$class("ui-state-default")
            ._("Total Container Releases(count)")._().th()
            .$class("ui-state-default")
            ._("Total Fulfilled Reservations(count)")._().th()
            .$class("ui-state-default")._("Total Container Preemptions(count)")
            ._()._()._().tbody();
      tbody
        .$class("ui-widget-content")
        .tr()
        .td(
          String.valueOf(cs.getRootQueueMetrics()
            .getAggregateAllocatedContainers()))
        .td(
          String.valueOf(cs.getRootQueueMetrics()
            .getAggegatedReleasedContainers()))
        .td(healthInfo.getAggregateFulFilledReservationsCount().toString())
        .td(healthInfo.getAggregatePreemptionCount().toString())._()._()._();
      div.h4("Last scheduler run");
      tbody =
          div.table("#lastrun").thead().$class("ui-widget-header").tr().th()
            .$class("ui-state-default")._("Time")._().th()
            .$class("ui-state-default")._("Allocations(count - resources)")._()
            .th().$class("ui-state-default")._("Reservations(count - resources)")
            ._().th().$class("ui-state-default")._("Releases(count - resources)")
            ._()._()._().tbody();
      tbody
        .$class("ui-widget-content")
        .tr()
        .td(Times.format(healthInfo.getLastSchedulerRunTime()))
        .td(
          healthInfo.getAllocationCount().toString() + " - "
              + healthInfo.getResourcesAllocated().toString())
        .td(
          healthInfo.getReservationCount().toString() + " - "
              + healthInfo.getResourcesReserved().toString())
        .td(
          healthInfo.getReleaseCount().toString() + " - "
              + healthInfo.getResourcesReleased().toString())._()._()._();
      Map<String, SchedulerHealth.DetailedInformation> info = new HashMap<>();
      info.put("Allocation", healthInfo.getLastAllocationDetails());
      info.put("Reservation", healthInfo.getLastReservationDetails());
      info.put("Release", healthInfo.getLastReleaseDetails());
      info.put("Preemption", healthInfo.getLastPreemptionDetails());

      for (Map.Entry<String, SchedulerHealth.DetailedInformation> entry : info
        .entrySet()) {
        String containerId = "N/A";
        String nodeId = "N/A";
        String queue = "N/A";
        String table = "#" + entry.getKey();
        div.h4("Last " + entry.getKey());
        tbody =
            div.table(table).thead().$class("ui-widget-header").tr().th()
              .$class("ui-state-default")._("Time")._().th()
              .$class("ui-state-default")._("Container Id")._().th()
              .$class("ui-state-default")._("Node Id")._().th()
              .$class("ui-state-default")._("Queue")._()._()._().tbody();
        SchedulerHealth.DetailedInformation di = entry.getValue();
        if (di.getTimestamp() != 0) {
          containerId = di.getContainerId().toString();
          nodeId = di.getNodeId().toString();
          queue = di.getQueue();
        }
        tbody.$class("ui-widget-content").tr()
          .td(Times.format(di.getTimestamp())).td(containerId).td(nodeId)
          .td(queue)._()._()._();
      }
      div._();
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
          "    if (q == 'Queue: root') q = '';",
          "    else {",
          "      q = q.substr(q.lastIndexOf(':') + 2);",
          "      q = '^' + q.substr(q.lastIndexOf('.') + 1) + '$';",
          "    }",
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
    return StringUtils.formatPercent(f, 1);
  }

  static String width(float f) {
    return StringUtils.format("width:%.1f%%", f * 100);
  }

  static String left(float f) {
    return StringUtils.format("left:%.1f%%", f * 100);
  }
}
