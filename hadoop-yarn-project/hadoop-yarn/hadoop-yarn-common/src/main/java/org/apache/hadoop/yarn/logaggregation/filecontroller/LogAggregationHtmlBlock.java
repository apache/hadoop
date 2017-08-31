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

package org.apache.hadoop.yarn.logaggregation.filecontroller;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_OWNER;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_ID;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.ENTITY_STRING;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NM_NODENAME;

import com.google.inject.Inject;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.logaggregation.LogAggregationWebUtils;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

/**
 * Base class to implement Aggregated Logs Block.
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public abstract class LogAggregationHtmlBlock extends HtmlBlock {

  @Inject
  public LogAggregationHtmlBlock(ViewContext ctx) {
    super(ctx);
  }

  protected BlockParameters verifyAndParseParameters(Block html) {
    BlockParameters params = new BlockParameters();
    ContainerId containerId = LogAggregationWebUtils
        .verifyAndGetContainerId(html, $(CONTAINER_ID));
    params.setContainerId(containerId);

    NodeId nodeId = LogAggregationWebUtils
        .verifyAndGetNodeId(html, $(NM_NODENAME));
    params.setNodeId(nodeId);

    String appOwner = LogAggregationWebUtils
        .verifyAndGetAppOwner(html, $(APP_OWNER));
    params.setAppOwner(appOwner);

    boolean isValid = true;
    long start = -4096;
    try {
      start = LogAggregationWebUtils.getLogStartIndex(
          html, $("start"));
    } catch (NumberFormatException ne) {
      html.h1().__("Invalid log start value: " + $("start")).__();
      isValid = false;
    }
    params.setStartIndex(start);

    long end = Long.MAX_VALUE;
    try {
      end = LogAggregationWebUtils.getLogEndIndex(
          html, $("end"));
    } catch (NumberFormatException ne) {
      html.h1().__("Invalid log start value: " + $("end")).__();
      isValid = false;
    }
    params.setEndIndex(end);

    if (containerId == null || nodeId == null || appOwner == null
        || appOwner.isEmpty() || !isValid) {
      return null;
    }

    ApplicationId appId = containerId.getApplicationAttemptId()
        .getApplicationId();
    params.setAppId(appId);

    String logEntity = $(ENTITY_STRING);
    if (logEntity == null || logEntity.isEmpty()) {
      logEntity = containerId.toString();
    }
    params.setLogEntity(logEntity);

    return params;
  }

  protected boolean checkAcls(Configuration conf, ApplicationId appId,
      String owner, Map<ApplicationAccessType, String> appAcls,
      String remoteUser) {
    ApplicationACLsManager aclsManager = new ApplicationACLsManager(
        conf);
    aclsManager.addApplication(appId, appAcls);

    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null && !aclsManager.checkAccess(callerUGI,
        ApplicationAccessType.VIEW_APP, owner, appId)) {
      return false;
    }
    return true;
  }

  protected static class BlockParameters {
    private ApplicationId appId;
    private ContainerId containerId;
    private NodeId nodeId;
    private String appOwner;
    private long start;
    private long end;
    private String logEntity;

    public ApplicationId getAppId() {
      return appId;
    }

    public void setAppId(ApplicationId appId) {
      this.appId = appId;
    }

    public ContainerId getContainerId() {
      return containerId;
    }

    public void setContainerId(ContainerId containerId) {
      this.containerId = containerId;
    }

    public NodeId getNodeId() {
      return nodeId;
    }

    public void setNodeId(NodeId nodeId) {
      this.nodeId = nodeId;
    }

    public String getAppOwner() {
      return appOwner;
    }

    public void setAppOwner(String appOwner) {
      this.appOwner = appOwner;
    }

    public long getStartIndex() {
      return start;
    }

    public void setStartIndex(long startIndex) {
      this.start = startIndex;
    }

    public long getEndIndex() {
      return end;
    }

    public void setEndIndex(long endIndex) {
      this.end = endIndex;
    }

    public String getLogEntity() {
      return logEntity;
    }

    public void setLogEntity(String logEntity) {
      this.logEntity = logEntity;
    }
  }
}
