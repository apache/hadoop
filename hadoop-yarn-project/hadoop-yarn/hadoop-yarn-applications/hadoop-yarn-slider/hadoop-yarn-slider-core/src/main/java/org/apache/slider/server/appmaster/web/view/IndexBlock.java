/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web.view;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.LI;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.UL;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.RoleStatistics;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.registry.docstore.ExportEntry;
import org.apache.slider.core.registry.docstore.PublishedExports;
import org.apache.slider.core.registry.docstore.PublishedExportsSet;
import org.apache.slider.providers.MonitorDetail;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.slider.server.appmaster.web.rest.RestPaths.LIVE_COMPONENTS;

/**
 * The main content on the Slider AM web page
 */
public class IndexBlock extends SliderHamletBlock {
  private static final Logger log = LoggerFactory.getLogger(IndexBlock.class);

  /**
   * Message printed when application is at full size.
   *
   * {@value}
   */
  public static final String ALL_CONTAINERS_ALLOCATED = "all containers allocated";

  @Inject
  public IndexBlock(WebAppApi slider) {
    super(slider);
  }

  @Override
  protected void render(Block html) {
    doIndex(html, getProviderName());
  }

  // An extra method to make testing easier since you can't make an instance of Block
  @VisibleForTesting
  protected void doIndex(Hamlet html, String providerName) {
    ClusterDescription clusterStatus = appState.getClusterStatus();
    String name = clusterStatus.name;
    if (name != null && (name.startsWith(" ") || name.endsWith(" "))) {
      name = "'" + name + "'";
    } 
    DIV<Hamlet> div = html.div("general_info")
                          .h1("index_header",
                              "Application: " + name);

    ApplicationLivenessInformation liveness =
        appState.getApplicationLivenessInformation();
    String livestatus = liveness.allRequestsSatisfied
        ? ALL_CONTAINERS_ALLOCATED
        : String.format("Awaiting %d containers", liveness.requestsOutstanding);
    Hamlet.TABLE<DIV<Hamlet>> table1 = div.table();
    table1.tr()
          .td("Status")
          .td(livestatus)
          ._();
    table1.tr()
          .td("Total number of containers")
          .td(Integer.toString(appState.getNumOwnedContainers()))
          ._();
    table1.tr()
          .td("Create time: ")
          .td(getInfoAvoidingNulls(StatusKeys.INFO_CREATE_TIME_HUMAN))
          ._();
    table1.tr()
          .td("Running since: ")
          .td(getInfoAvoidingNulls(StatusKeys.INFO_LIVE_TIME_HUMAN))
          ._();
    table1.tr()
          .td("Time last flexed: ")
          .td(getInfoAvoidingNulls(StatusKeys.INFO_FLEX_TIME_HUMAN))
          ._();
    table1.tr()
          .td("Application storage path: ")
          .td(clusterStatus.dataPath)
          ._();
    table1.tr()
          .td("Application configuration path: ")
          .td(clusterStatus.originConfigurationPath)
          ._();
    table1._();
    div._();
    div = null;

    DIV<Hamlet> containers = html.div("container_instances")
      .h3("Component Instances");

    int aaRoleWithNoSuitableLocations = 0;
    int aaRoleWithOpenRequest = 0;
    int roleWithOpenRequest = 0;

    Hamlet.TABLE<DIV<Hamlet>> table = containers.table();
    Hamlet.TR<Hamlet.THEAD<Hamlet.TABLE<DIV<Hamlet>>>> header = table.thead().tr();
    trb(header, "Component");
    trb(header, "Desired");
    trb(header, "Actual");
    trb(header, "Outstanding Requests");
    trb(header, "Failed");
    trb(header, "Failed to start");
    trb(header, "Placement");
    header._()._();  // tr & thead

    List<RoleStatus> roleStatuses = appState.cloneRoleStatusList();
    Collections.sort(roleStatuses, new RoleStatus.CompareByName());
    for (RoleStatus status : roleStatuses) {
      String roleName = status.getName();
      String nameUrl = apiPath(LIVE_COMPONENTS) + "/" + roleName;
      String aatext;
      if (status.isAntiAffinePlacement()) {
        boolean aaRequestOutstanding = status.isAARequestOutstanding();
        int pending = (int)status.getPendingAntiAffineRequests();
        aatext = buildAADetails(aaRequestOutstanding, pending);
        if (SliderUtils.isSet(status.getLabelExpression())) {
          aatext += " (label: " + status.getLabelExpression() + ")";
        }
        if (pending > 0 && !aaRequestOutstanding) {
          aaRoleWithNoSuitableLocations ++;
        } else if (aaRequestOutstanding) {
          aaRoleWithOpenRequest++;
        }
      } else {
        if (SliderUtils.isSet(status.getLabelExpression())) {
          aatext = "label: " + status.getLabelExpression();
        } else {
          aatext = "";
        }
        if (status.getRequested() > 0) {
          roleWithOpenRequest ++;
        }
      }
      table.tr()
        .td().a(nameUrl, roleName)._()
        .td(String.format("%d", status.getDesired()))
        .td(String.format("%d", status.getActual()))
        .td(String.format("%d", status.getRequested()))
        .td(String.format("%d", status.getFailed()))
        .td(String.format("%d", status.getStartFailed()))
        .td(aatext)
        ._();
    }

    // empty row for some more spacing
    table.tr()._();
    // close table
    table._();

    containers._();
    containers = null;

    // some spacing
    html.div()._();
    html.div()._();

    DIV<Hamlet> diagnostics = html.div("diagnostics");

    List<String> statusEntries = new ArrayList<>(0);
    if (roleWithOpenRequest > 0) {
      statusEntries.add(String.format("%d %s with requests unsatisfiable by cluster",
          roleWithOpenRequest, plural(roleWithOpenRequest, "component")));
    }
    if (aaRoleWithNoSuitableLocations > 0) {
      statusEntries.add(String.format("%d anti-affinity %s no suitable nodes in the cluster",
        aaRoleWithNoSuitableLocations,
        plural(aaRoleWithNoSuitableLocations, "component has", "components have")));
    }
    if (aaRoleWithOpenRequest > 0) {
      statusEntries.add(String.format("%d anti-affinity %s with requests unsatisfiable by cluster",
        aaRoleWithOpenRequest,
        plural(aaRoleWithOpenRequest, "component has", "components have")));

    }
    if (!statusEntries.isEmpty()) {
      diagnostics.h3("Diagnostics");
      Hamlet.TABLE<DIV<Hamlet>> diagnosticsTable = diagnostics.table();
      for (String entry : statusEntries) {
        diagnosticsTable.tr().td(entry)._();
      }
      diagnosticsTable._();
    }
    diagnostics._();

    DIV<Hamlet> provider_info = html.div("provider_info");
    provider_info.h3(providerName + " information");
    UL<Hamlet> ul = html.ul();
    addProviderServiceOptions(providerService, ul, clusterStatus);
    ul._();
    provider_info._();

    DIV<Hamlet> exports = html.div("exports");
    exports.h3("Exports");
    ul = html.ul();
    enumeratePublishedExports(appState.getPublishedExportsSet(), ul);
    ul._();
    exports._();
  }

  @VisibleForTesting
  String buildAADetails(boolean outstanding, int pending) {
    return String.format("Anti-affinity:%s %d pending %s",
      (outstanding ? " 1 active request and" : ""),
      pending, plural(pending, "request"));
  }

  private String plural(int n, String singular) {
    return plural(n, singular, singular + "s");
  }
  private String plural(int n, String singular, String plural) {
    return n == 1 ? singular : plural;
  }

  private void trb(Hamlet.TR tr,
      String text) {
    tr.td().b(text)._();
  }

  private String getProviderName() {
    return providerService.getHumanName();
  }

  private String getInfoAvoidingNulls(String key) {
    String createTime = appState.getClusterStatus().getInfo(key);

    return null == createTime ? "N/A" : createTime;
  }

  protected void addProviderServiceOptions(ProviderService provider,
      UL ul, ClusterDescription clusterStatus) {
    Map<String, MonitorDetail> details = provider.buildMonitorDetails(
        clusterStatus);
    if (null == details) {
      return;
    }
    // Loop over each entry, placing the text in the UL, adding an anchor when the URL is non-null/empty
    for (Entry<String, MonitorDetail> entry : details.entrySet()) {
      MonitorDetail detail = entry.getValue();
      if (SliderUtils.isSet(detail.getValue()) ) {
        LI item = ul.li();
        item.span().$class("bold")._(entry.getKey())._();
        item._(" - ");
        if (detail.isUrl()) {
          // Render an anchor if the value is a URL
          item.a(detail.getValue(), detail.getValue())._();
        } else {
          item._(detail.getValue())._();
        }
      } else {
        ul.li(entry.getKey());
      }
    }
  }

  protected void enumeratePublishedExports(PublishedExportsSet exports, UL<Hamlet> ul) {
    for(String key : exports.keys()) {
      PublishedExports export = exports.get(key);
      LI<UL<Hamlet>> item = ul.li();
      item.span().$class("bold")._(export.description)._();
      UL sublist = item.ul();
      for (Entry<String, List<ExportEntry>> entry : export.entries.entrySet()) {
        LI sublistItem = sublist.li()._(entry.getKey());
        for (ExportEntry exportEntry : entry.getValue()) {
          sublistItem._(exportEntry.getValue());
        }
        sublistItem._();
      }
      sublist._();
      item._();
    }
  }
}
