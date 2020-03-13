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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI._EVEN;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._ODD;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceRequestInfo;
import org.apache.hadoop.yarn.server.webapp.AppAttemptBlock;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class RMAppAttemptBlock extends AppAttemptBlock{

  private final ResourceManager rm;
  protected Configuration conf;
  private final static String DIAGNOSTICS_SCRIPT_BODY = new StringBuilder()
      .append("var refresh = false;")
      .append("var haveGotAppDiagnostic = false;")
      .append("function getArray(objOrArray) {")
      .append("  if(Array.isArray(objOrArray)){")
      .append("    return objOrArray;")
      .append("  } else {")
      .append("    return [objOrArray];")
      .append("  }")
      .append("}")
      .append("function getTableDataFromObjectArray(objectArray, fields) {")
      .append("  if (objectArray == undefined) {")
      .append("    return [];")
      .append("  }")
      .append("  var data = [];")
      .append("  $.each(objectArray, function(i, object) {")
      .append("    var dataItem = [];")
      .append("    for (var i=0; i<fields.length; i++) {")
      .append("      dataItem.push(object[fields[i]] != undefined ? ")
      .append("object[fields[i]] : '');")
      .append("    }")
      .append("    data.push(dataItem);")
      .append("  });")
      .append("  return data;")
      .append("}")
      .append("function sendRequest(requestURL, doneCallBackFunc,")
      .append("                      failCallBackFunc) {")
      .append(" $.ajax({")
      .append("  type: 'GET',")
      .append("  url: requestURL,")
      .append("  contentType: 'text/plain'")
      .append(" }).done(doneCallBackFunc).fail(failCallBackFunc);")
      .append("}")
      .append("function parseQueryAppActResponse(responseObj) {")
      .append(" var requestDiagnostics = [], dateTime, appDiagnostic;")
      .append(" responseObj = responseObj['appActivities'];")
      .append(" if(responseObj.hasOwnProperty('allocations')){")
      .append("  var allocations = getArray(responseObj['allocations']);")
      .append("  var allocation = allocations[0];")
      .append("  dateTime = allocation['dateTime'];")
      .append("  if(allocation.hasOwnProperty('diagnostic')){")
      .append("   appDiagnostic = allocation['diagnostic'];")
      .append("  }")
      .append("  if(allocation.hasOwnProperty('children')){")
      .append("   var requestAllocations = ")
      .append("     getArray(allocation['children']);")
      .append("   $.each(requestAllocations, function (i, requestAllocation) {")
      .append("    if(requestAllocation.hasOwnProperty('children')){")
      .append("     var allocationAttempts = ")
      .append("       getArray(requestAllocation['children']);")
      .append("     var diagnosticSummary = requestAllocation.hasOwnProperty(")
      .append("       'diagnostic')? requestAllocation['diagnostic']+'. ':'';")
      .append("     $.each(allocationAttempts, function(i,allocationAttempt) {")
      .append("      if (i > 0) {")
      .append("       diagnosticSummary += ', ';")
      .append("      }")
      .append("      diagnosticSummary += allocationAttempt['count'] + ' ' +")
      .append("        (allocationAttempt['count']=='1' ? 'node ' : 'nodes ')")
      .append("        + allocationAttempt['allocationState'];")
      .append("      if (allocationAttempt.hasOwnProperty('diagnostic')) {")
      .append("       diagnosticSummary += ' with diagnostic:' + ")
      .append("                   allocationAttempt['diagnostic'];")
      .append("      }")
      .append("     });")
      .append("     delete requestAllocation['children'];")
      .append("     requestAllocation['diagnostic']=diagnosticSummary;")
      .append("    }")
      .append("    requestDiagnostics.push(requestAllocation);")
      .append("   });")
      .append("  }")
      .append(" }")
      .append(" return [requestDiagnostics, dateTime, appDiagnostic];")
      .append("}")
      .append("function handleQueryAppActDone(data){")
      .append("  console.log('App activities:', data);")
      .append("  var results = parseQueryAppActResponse(data);")
      .append("  console.log('parsed results:', results);")
      .append("  var requestDiagnostics = results[0];")
      .append("  var dateTime = results[1];")
      .append("  var appDiagnostic = results[2];")
      .append("  haveGotAppDiagnostic = false;")
      .append("  if (appDiagnostic != undefined) {")
      .append("    haveGotAppDiagnostic = true;")
      .append("    $('#appDiagnostic').html('App diagnostic: '")
      .append("        + appDiagnostic);")
      .append("  }")
      .append("  $('#diagnosticsTableDiv').empty();")
      .append("  if (requestDiagnostics.length > 0) {")
      .append("    haveGotAppDiagnostic = true;")
      .append("    tableData = getTableDataFromObjectArray(requestDiagnostics,")
      .append(" ['requestPriority', 'allocationRequestId', 'allocationState',")
      .append(" 'diagnostic']);")
      .append("    $('#diagnosticsTableDiv').append('<table ")
      .append("      id=\"diagnosticsTable\"></table>');")
      .append("    $('#diagnosticsTable').dataTable( {")
      .append("      'aaData': tableData,")
      .append("      'aoColumns': [")
      .append("        { 'sTitle': 'Priority' },")
      .append("        { 'sTitle': 'AllocationRequestId' },")
      .append("        { 'sTitle': 'AllocationState' },")
      .append("        { 'sTitle': 'DiagnosticSummary' }")
      .append("      ],")
      .append("      bJQueryUI:true, sPaginationType: 'full_numbers',")
      .append("      iDisplayLength:20, aLengthMenu:[20, 40, 60, 80, 100]")
      .append("    });")
      .append("  }")
      .append("  if (haveGotAppDiagnostic) {")
      .append("    $('#refreshDiagnosticsBtn').attr('disabled',false);")
      .append("    $('#diagnosticsUpdateTime').html('Updated at ' + dateTime);")
      .append("  } else {")
      .append("    $('#diagnosticsUpdateTime').html('');")
      .append("    if (refresh) {")
      .append("      refreshSchedulerDiagnostics();")
      .append("    }")
      .append("  }")
      .append("}")
      .append("function handleRequestFailure(data){")
      .append(" alert('Request app activities REST API failed.');")
      .append(" console.log(data);")
      .append("}")
      .append("function handleRefreshAppActDone(data){")
      .append(" refresh = true;")
      .append(" setTimeout(function(){")
      .append("  queryAppDiagnostics();")
      .append(" }, 2000);")
      .append("}")
      .append("function refreshAppDiagnostics() {")
      .append(" $('#refreshDiagnosticsBtn').attr('disabled',true);")
      .append(" sendRequest(refreshAppActivitiesURL,handleRefreshAppActDone,")
      .append("             handleRequestFailure);")
      .append("}")
      .append("function queryAppDiagnostics() {")
      .append(" sendRequest(getAppActivitiesURL,handleQueryAppActDone,")
      .append("             handleRequestFailure);")
      .append("}")
      .append("function parseHierarchicalQueue(node, nodeInfos,")
      .append("    parentNodePath, partition) {")
      .append("  var nodePath = parentNodePath + '/' + node['name'];")
      .append("  if (node.hasOwnProperty('name')){")
      .append("    if (node['diagnostic'] == undefined || node['diagnostic']")
      .append("      .indexOf(ignoreActivityContent) == -1){")
      .append("     var nodeInfo = {};")
      .append("     nodeInfo['partition'] = partition;")
      .append("     nodeInfo['path'] = nodePath;")
      .append("     nodeInfo['allocationState'] = node['allocationState'];")
      .append("     nodeInfo['diagnostic'] = ")
      .append("      node['diagnostic'] == undefined ? '':node['diagnostic'];")
      .append("     nodeInfos.push(nodeInfo);")
      .append("    }")
      .append("  }")
      .append("  if (node.hasOwnProperty('children')){")
      .append("    var children = getArray(node['children']);")
      .append("    $.each(children, function (i, child) {")
      .append("      parseHierarchicalQueue(child, nodeInfos, nodePath,")
      .append("        partition);")
      .append("    });")
      .append("  }")
      .append("}")
      .append("function parseQueueDiagnostics(data) {")
      .append("  var nodeInfos = [], dateTime;")
      .append("  if(data.hasOwnProperty('dateTime')){")
      .append("      dateTime = data['dateTime'];")
      .append("  }")
      .append("  if(data.hasOwnProperty('allocations')){")
      .append("    var allocations = getArray(data['allocations']);")
      .append("    $.each(allocations, function (i, allocation) {")
      .append("      if (allocation.hasOwnProperty('root')) {")
      .append("        var root = allocation['root'];")
      .append("        var partition = allocation['partition'];")
      .append("        parseHierarchicalQueue(root, nodeInfos, '', partition);")
      .append("      }")
      .append("    });")
      .append("  }")
      .append("  return [nodeInfos, dateTime];")
      .append("}")
      .append("function handleRefreshSchedulerActDone(data){")
      .append(" console.log('handleRefreshSchedulerActDone', data);")
      .append(" setTimeout(function(){")
      .append("  querySchedulerDiagnostics();")
      .append("  $('#refreshDiagnosticsBtn').attr('disabled',false);")
      .append(" }, 100);")
      .append("}")
      .append("function handleQuerySchedulerActDone(data){")
      .append("  console.log('Scheduler activities:', data);")
      .append("  data = data['activities'];")
      .append("  var results = parseQueueDiagnostics(data);")
      .append("  var nodeInfos = results[0];")
      .append("  var dateTime = results[1];")
      .append("  $('#diagnosticsTableDiv').empty();")
      .append("  if(dateTime != undefined){")
      .append("   $('#diagnosticsUpdateTime').html('App diagnostics not found!")
      .append(" Got useful scheduler activities updated at '+dateTime);")
      .append("    tableData = getTableDataFromObjectArray(nodeInfos, ")
      .append(" ['partition', 'path', 'allocationState', 'diagnostic']);")
      .append("    $('#diagnosticsTableDiv').append('<table ")
      .append("      id=\"diagnosticsTable\"></table>');")
      .append("    $('#diagnosticsTable').dataTable({")
      .append("      'aaData': tableData,")
      .append("      'aoColumns': [")
      .append("        { 'sTitle': 'Partition' },")
      .append("        { 'sTitle': 'SchedulingNode' },")
      .append("        { 'sTitle': 'AllocationState' },")
      .append("        { 'sTitle': 'Diagnostic' }")
      .append("      ],")
      .append("      bJQueryUI:true, sPaginationType: 'full_numbers',")
      .append("      iDisplayLength:20, aLengthMenu:[20, 40, 60, 80, 100]")
      .append("    });")
      .append("  } else if(data.hasOwnProperty('diagnostic')){")
      .append("    $('#diagnosticsUpdateTime').html(data['diagnostic']);")
      .append("  }")
      .append("  $('#refreshDiagnosticsBtn').attr('disabled',false);")
      .append("}")
      .append("function refreshSchedulerDiagnostics() {")
      .append(" $('#refreshDiagnosticsBtn').attr('disabled',true);")
      .append(" sendRequest(schedulerActivitiesURL,")
      .append("   handleRefreshSchedulerActDone,handleRequestFailure);")
      .append("}")
      .append("function querySchedulerDiagnostics() {")
      .append(" sendRequest(schedulerActivitiesURL,")
      .append("   handleQuerySchedulerActDone,handleRequestFailure);")
      .append("}")
      .append("queryAppDiagnostics();").toString();

  @Inject
  RMAppAttemptBlock(ViewContext ctx, ResourceManager rm, Configuration conf) {
    super(null, ctx);
    this.rm = rm;
    this.conf = conf;
  }

  private void createResourceRequestsTable(Block html) {
    AppInfo app =
        new AppInfo(rm, rm.getRMContext().getRMApps()
          .get(this.appAttemptId.getApplicationId()), true,
          WebAppUtils.getHttpSchemePrefix(conf));

    List<ResourceRequestInfo> resourceRequests = app.getResourceRequests();
    if (resourceRequests == null || resourceRequests.isEmpty()) {
      return;
    }

    DIV<Hamlet> div = html.div(_INFO_WRAP);
    // Requests Table
    TBODY<TABLE<DIV<Hamlet>>> tbody = div
        .h3("Total Outstanding Resource Requests: "
            + getTotalResource(resourceRequests))
        .table("#resourceRequests").thead().tr().th(".priority", "Priority")
        .th(".allocationRequestId", "AllocationRequestId")
        .th(".resource", "ResourceName").th(".capacity", "Capability")
        .th(".containers", "NumContainers")
        .th(".relaxlocality", "RelaxLocality")
        .th(".labelexpression", "NodeLabelExpression")
        .th(".executiontype", "ExecutionType")
        .th(".allocationTags", "AllocationTags")
        .th(".placementConstraint", "PlacementConstraint").__().__().tbody();

    StringBuilder resourceRequestTableData = new StringBuilder("[\n");
    for (ResourceRequestInfo resourceRequest  : resourceRequests) {
      if (resourceRequest.getNumContainers() == 0) {
        continue;
      }
      resourceRequestTableData.append("[\"")
          .append(String.valueOf(resourceRequest.getPriority())).append("\",\"")
          .append(String.valueOf(resourceRequest.getAllocationRequestId()))
          .append("\",\"")
          .append(resourceRequest.getResourceName() == null ? "N/A"
              : resourceRequest.getResourceName())
          .append("\",\"")
          .append(StringEscapeUtils.escapeEcmaScript(StringEscapeUtils
              .escapeHtml4(String.valueOf(resourceRequest.getCapability()))))
          .append("\",\"")
          .append(String.valueOf(resourceRequest.getNumContainers()))
          .append("\",\"")
          .append(String.valueOf(resourceRequest.getRelaxLocality()))
          .append("\",\"")
          .append(resourceRequest.getNodeLabelExpression() == null ? "N/A"
              : resourceRequest.getNodeLabelExpression())
          .append("\",\"")
          .append(resourceRequest.getExecutionTypeRequest() == null ? "N/A"
              : resourceRequest.getExecutionTypeRequest().getExecutionType())
          .append("\",\"")
          .append(resourceRequest.getAllocationTags() == null ? "N/A" :
              StringUtils.join(resourceRequest.getAllocationTags(), ","))
          .append("\",\"")
          .append(resourceRequest.getPlacementConstraint() == null ? "N/A"
              : resourceRequest.getPlacementConstraint())
          .append("\"],\n");
    }
    if (resourceRequestTableData
        .charAt(resourceRequestTableData.length() - 2) == ',') {
      resourceRequestTableData.delete(resourceRequestTableData.length() - 2,
          resourceRequestTableData.length() - 1);
    }
    resourceRequestTableData.append("]");
    html.script().$type("text/javascript")
        .__("var resourceRequestsTableData=" + resourceRequestTableData).__();
    tbody.__().__();

    // create diagnostics table when CS is enabled
    createDiagnosticsTable(html, div);

    div.__();
  }

  private Resource getTotalResource(List<ResourceRequestInfo> requests) {
    Resource totalResource = Resource.newInstance(0, 0);
    if (requests == null) {
      return totalResource;
    }
    for (ResourceRequestInfo request : requests) {
      if (request.getNumContainers() == 0) {
        continue;
      }
      if (request.getResourceName() == null || request.getResourceName()
          .equals(ResourceRequest.ANY)) {
        Resources.addTo(
            totalResource,
            Resources.multiply(request.getCapability().getResource(),
            request.getNumContainers()));
      }
    }
    return totalResource;
  }

  private void createContainerLocalityTable(Block html) {
    RMAppAttemptMetrics attemptMetrics = null;
    RMAppAttempt attempt = getRMAppAttempt();
    if (attempt != null) {
      attemptMetrics = attempt.getRMAppAttemptMetrics();
    }
    
    if (attemptMetrics == null) {
      return;
    }

    DIV<Hamlet> div = html.div(_INFO_WRAP);
    TABLE<DIV<Hamlet>> table =
        div.h3(
          "Total Allocated Containers: "
              + attemptMetrics.getTotalAllocatedContainers()).h3("Each table cell"
            + " represents the number of NodeLocal/RackLocal/OffSwitch containers"
            + " satisfied by NodeLocal/RackLocal/OffSwitch resource requests.").table(
          "#containerLocality");
    table.
      tr().
        th(_TH, "").
        th(_TH, "Node Local Request").
        th(_TH, "Rack Local Request").
        th(_TH, "Off Switch Request").
        __();

    String[] containersType =
        { "Num Node Local Containers (satisfied by)", "Num Rack Local Containers (satisfied by)",
            "Num Off Switch Containers (satisfied by)" };
    boolean odd = false;
    for (int i = 0; i < attemptMetrics.getLocalityStatistics().length; i++) {
      table.tr((odd = !odd) ? _ODD : _EVEN).td(containersType[i])
        .td(String.valueOf(attemptMetrics.getLocalityStatistics()[i][0]))
        .td(i == 0 ? "" : String.valueOf(attemptMetrics.getLocalityStatistics()[i][1]))
        .td(i <= 1 ? "" : String.valueOf(attemptMetrics.getLocalityStatistics()[i][2])).__();
    }
    table.__();
    div.__();
  }

  private boolean isApplicationInFinalState(YarnApplicationAttemptState state) {
    return state == YarnApplicationAttemptState.FINISHED
        || state == YarnApplicationAttemptState.FAILED
        || state == YarnApplicationAttemptState.KILLED;
  }

  @Override
  protected void createAttemptHeadRoomTable(Block html) {
    RMAppAttempt attempt = getRMAppAttempt();
    if (attempt != null) {
      if (!isApplicationInFinalState(YarnApplicationAttemptState
          .valueOf(attempt.getAppAttemptState().toString()))) {
        RMAppAttemptMetrics metrics = attempt.getRMAppAttemptMetrics();
        DIV<Hamlet> pdiv = html.__(InfoBlock.class).div(_INFO_WRAP);
        info("Application Attempt Overview").clear();
        info("Application Attempt Metrics").__(
          "Application Attempt Headroom : ", metrics == null ? "N/A" :
            metrics.getApplicationAttemptHeadroom());
        pdiv.__();
      }
    }
  }

  private RMAppAttempt getRMAppAttempt() {
    ApplicationId appId = this.appAttemptId.getApplicationId();
    RMAppAttempt attempt = null;
    RMApp rmApp = rm.getRMContext().getRMApps().get(appId);
    if (rmApp != null) { 
      attempt = rmApp.getAppAttempts().get(appAttemptId);
    }
    return attempt;
  }

  protected void generateOverview(ApplicationAttemptReport appAttemptReport,
      Collection<ContainerReport> containers, AppAttemptInfo appAttempt,
      String node) {

    RMAppAttempt rmAppAttempt = getRMAppAttempt();
    // nodes which are blacklisted by the application
    String appBlacklistedNodes =
        getNodeString(rmAppAttempt.getBlacklistedNodes());
    // nodes which are blacklisted by the RM for AM launches
    String rmBlackListedNodes =
        getNodeString(rmAppAttempt.getAMBlacklistManager()
          .getBlacklistUpdates().getBlacklistAdditions());

    info("Application Attempt Overview")
      .__(
        "Application Attempt State:",
        appAttempt.getAppAttemptState() == null ? UNAVAILABLE : appAttempt
          .getAppAttemptState())
        .__("Started:", Times.format(appAttempt.getStartedTime()))
        .__("Elapsed:",
            org.apache.hadoop.util.StringUtils.formatTime(Times.elapsed(
                appAttempt.getStartedTime(), appAttempt.getFinishedTime())))
      .__(
        "AM Container:",
        appAttempt.getAmContainerId() == null || containers == null
            || !hasAMContainer(appAttemptReport.getAMContainerId(), containers)
            ? null : root_url("container", appAttempt.getAmContainerId()),
        appAttempt.getAmContainerId() == null ? "N/A" :
          String.valueOf(appAttempt.getAmContainerId()))
      .__("Node:", node)
      .__(
        "Tracking URL:",
        appAttempt.getTrackingUrl() == null
            || appAttempt.getTrackingUrl().equals(UNAVAILABLE) ? null
            : root_url(appAttempt.getTrackingUrl()),
        appAttempt.getTrackingUrl() == null
            || appAttempt.getTrackingUrl().equals(UNAVAILABLE)
            ? "Unassigned"
            : appAttempt.getAppAttemptState() == YarnApplicationAttemptState.FINISHED
                || appAttempt.getAppAttemptState() == YarnApplicationAttemptState.FAILED
                || appAttempt.getAppAttemptState() == YarnApplicationAttemptState.KILLED
                ? "History" : "ApplicationMaster")
      .__(
        "Diagnostics Info:",
        appAttempt.getDiagnosticsInfo() == null ? "" : appAttempt
          .getDiagnosticsInfo())
      .__("Nodes blacklisted by the application:", appBlacklistedNodes)
      .__("Nodes blacklisted by the system:", rmBlackListedNodes);
  }

  private String getNodeString(Collection<String> nodes) {
    String concatinatedString = "-";
    if (null != nodes && !nodes.isEmpty()) {
      concatinatedString = StringUtils.join(nodes, ", ");
    }
    return concatinatedString;
  }

  @Override
  protected void createTablesForAttemptMetrics(Block html) {
    createContainerLocalityTable(html);
    createResourceRequestsTable(html);
  }

  @Override
  protected List<ContainerReport> getContainers(
      final GetContainersRequest request) throws YarnException, IOException {
    return rm.getClientRMService().getContainers(request).getContainerList();
  }

  @Override
  protected ApplicationAttemptReport getApplicationAttemptReport(
      final GetApplicationAttemptReportRequest request)
      throws YarnException, IOException {
    return rm.getClientRMService().getApplicationAttemptReport(request)
        .getApplicationAttemptReport();
  }

  private void createDiagnosticsTable(Block html, DIV<Hamlet> parentDiv) {
    if (!(rm.getResourceScheduler() instanceof CapacityScheduler)) {
      return;
    }
    String appActivitiesURL =
        new StringBuilder().append(RMWSConsts.RM_WEB_SERVICE_PATH).append(
            RMWSConsts.SCHEDULER_APP_ACTIVITIES.replace("{appid}",
                this.appAttemptId.getApplicationId().toString())).append("?")
            .toString();
    String refreshAppActivitiesURL = appActivitiesURL + "actions=refresh";
    String getAppActivitiesURL = appActivitiesURL
        + "actions=get&groupBy=diagnostic&summarize=true";
    String refreshAndGetAppActivitiesURL = appActivitiesURL
        + "actions=refresh&actions=get&groupBy=diagnostic&summarize=true";
    String schedulerActivitiesURL =
        new StringBuilder().append(RMWSConsts.RM_WEB_SERVICE_PATH)
            .append(RMWSConsts.SCHEDULER_ACTIVITIES).append("?")
            .append(RMWSConsts.GROUP_BY).append("=diagnostic").toString();

    DIV<DIV<Hamlet>> div = parentDiv.div();
    div.p().__("Diagnostics in cache ").__("(For more details refer to ")
        .a(refreshAndGetAppActivitiesURL, "App Activities").__(" or ")
        .a(schedulerActivitiesURL, "Scheduler Activities").__(")").__();
    div.button().$id("refreshDiagnosticsBtn")
        .$style("border-style: solid; border-color: #000000; border-width: 1px;"
                + " cursor: hand; cursor: pointer; border-radius: 4px")
        .$onclick("refreshAppDiagnostics()").b("Refresh").__();
    div.p().$id("diagnosticsUpdateTime").__();
    div.p().$id("appDiagnostic").__();
    div.div("#diagnosticsTableDiv").__();

    div.__();

    StringBuilder script = new StringBuilder();
    script
        .append("var refreshAppActivitiesURL = '")
        .append(refreshAppActivitiesURL).append("';")
        .append("var getAppActivitiesURL = '")
        .append(getAppActivitiesURL).append("';")
        .append("var schedulerActivitiesURL = '")
        .append(schedulerActivitiesURL).append("';")
        .append("var ignoreActivityContent = '")
        .append("does not need more resource';")
        .append(DIAGNOSTICS_SCRIPT_BODY);

    html.script().$type("text/javascript").__(script.toString()).__();
  }
}
