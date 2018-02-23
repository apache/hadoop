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

package org.apache.hadoop.yarn.webapp.log;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_OWNER;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_ID;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.ENTITY_STRING;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NM_NODENAME;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.LogAggregationWebUtils;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import com.google.inject.Inject;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class AggregatedLogsBlock extends HtmlBlock {

  private final Configuration conf;
  private final LogAggregationFileControllerFactory factory;

  @Inject
  AggregatedLogsBlock(Configuration conf) {
    this.conf = conf;
    factory = new LogAggregationFileControllerFactory(conf);
  }

  @Override
  protected void render(Block html) {
    ContainerId containerId = LogAggregationWebUtils
        .verifyAndGetContainerId(html, $(CONTAINER_ID));
    NodeId nodeId = LogAggregationWebUtils
        .verifyAndGetNodeId(html, $(NM_NODENAME));
    String appOwner = LogAggregationWebUtils
        .verifyAndGetAppOwner(html, $(APP_OWNER));
    boolean isValid = true;
    try {
      LogAggregationWebUtils.getLogStartIndex(
          html, $("start"));
    } catch (NumberFormatException ne) {
      html.h1().__("Invalid log start value: " + $("start")).__();
      isValid = false;
    }
    try {
      LogAggregationWebUtils.getLogEndIndex(
          html, $("end"));
    } catch (NumberFormatException ne) {
      html.h1().__("Invalid log end value: " + $("end")).__();
      isValid = false;
    }

    if (containerId == null || nodeId == null || appOwner == null
        || appOwner.isEmpty() || !isValid) {
      return;
    }

    ApplicationId applicationId = containerId.getApplicationAttemptId()
        .getApplicationId();
    String logEntity = $(ENTITY_STRING);
    if (logEntity == null || logEntity.isEmpty()) {
      logEntity = containerId.toString();
    }

    String nmApplicationLogUrl = getApplicationLogURL(applicationId);
    if (!conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
      html.h1()
          .__("Aggregation is not enabled. Try the nodemanager at " + nodeId)
          .__();
      if(nmApplicationLogUrl != null) {
        html.h1()
            .__("Or see application log at " + nmApplicationLogUrl)
            .__();
      }
      return;
    }

    LogAggregationFileController fileController;
    try {
      fileController = this.factory.getFileControllerForRead(
          applicationId, appOwner);
    } catch (Exception fnf) {
      html.h1()
          .__("Logs not available for " + logEntity
              + ". Aggregation may not be complete, "
              + "Check back later or try the nodemanager at " + nodeId).__();
      if(nmApplicationLogUrl != null)  {
        html.h1()
            .__("Or see application log at " + nmApplicationLogUrl)
            .__();
      }
      return;
    }

    fileController.renderAggregatedLogsBlock(html, this.context());
  }

  private String getApplicationLogURL(ApplicationId applicationId) {
    String appId = applicationId.toString();
    if (appId == null || appId.isEmpty()) {
      return null;
    }
    String nodeId = $(NM_NODENAME);
    if(nodeId == null || nodeId.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    String scheme = YarnConfiguration.useHttps(this.conf) ? "https://":
        "http://";
    sb.append(scheme).append(nodeId).append("/node/application/").append(appId);
    return sb.toString();
  }
}