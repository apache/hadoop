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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
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
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
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
      html.h1().__("Invalid log end value: " + $("end")).__();
      isValid = false;
    }
    params.setEndIndex(end);

    long startTime = 0;
    try {
      startTime = LogAggregationWebUtils.getLogStartTime(
          $("start.time"));
    } catch (NumberFormatException ne) {
      html.h1().__("Invalid log start time value: " + $("start.time")).__();
      isValid = false;
    }
    params.setStartTime(startTime);

    long endTime = Long.MAX_VALUE;
    try {
      endTime = LogAggregationWebUtils.getLogEndTime(
          $("end.time"));
      if (endTime < startTime) {
        html.h1().__("Invalid log end time value: " + $("end.time") +
            ". It should be larger than start time value:" + startTime).__();
        isValid = false;
      }
    } catch (NumberFormatException ne) {
      html.h1().__("Invalid log end time value: " + $("end.time")).__();
      isValid = false;
    }
    params.setEndTime(endTime);

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

  protected long[] checkParseRange(Block html, long startIndex,
      long endIndex, long startTime, long endTime, long logLength, String logType) {
    long start = startIndex < 0
        ? logLength + startIndex : startIndex;
    start = start < 0 ? 0 : start;
    start = start > logLength ? logLength : start;
    long end = endIndex < 0
        ? logLength + endIndex : endIndex;
    end = end < 0 ? 0 : end;
    end = end > logLength ? logLength : end;
    end = end < start ? start : end;

    long toRead = end - start;
    if (toRead < logLength) {
      html.p().__("Showing " + toRead + " bytes of " + logLength
          + " total. Click ").a(url("logs", $(NM_NODENAME), $(CONTAINER_ID),
          $(ENTITY_STRING), $(APP_OWNER),
          logType, "?start=0&start.time=" + startTime
              + "&end.time=" + endTime), "here").
          __(" for the full log.").__();
    }
    return new long[]{start, end};
  }

  protected void processContainerLog(Block html, long[] range, InputStream in,
      int bufferSize, byte[] cbuf) throws IOException {
    long totalSkipped = 0;
    long start = range[0];
    long toRead = range[1] - range[0];
    while (totalSkipped < start) {
      long ret = in.skip(start - totalSkipped);
      if (ret == 0) {
        //Read one byte
        int nextByte = in.read();
        // Check if we have reached EOF
        if (nextByte == -1) {
          throw new IOException("Premature EOF from container log");
        }
        ret = 1;
      }
      totalSkipped += ret;
    }

    int len = 0;
    int currentToRead = toRead > bufferSize ? bufferSize : (int) toRead;
    Hamlet.PRE<Hamlet> pre = html.pre();

    while (toRead > 0 && (len = in.read(cbuf, 0, currentToRead)) > 0) {
      pre.__(new String(cbuf, 0, len, Charset.forName("UTF-8")));
      toRead = toRead - len;
      currentToRead = toRead > bufferSize ? bufferSize : (int) toRead;
    }

    pre.__();
  }

  protected static class BlockParameters {
    private ApplicationId appId;
    private ContainerId containerId;
    private NodeId nodeId;
    private String appOwner;
    private long start;
    private long end;
    private String logEntity;
    private long startTime;
    private long endTime;

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

    public long getStartTime() {
      return startTime;
    }

    public void setStartTime(long startTime) {
      this.startTime = startTime;
    }

    public long getEndTime() {
      return endTime;
    }

    public void setEndTime(long endTime) {
      this.endTime = endTime;
    }
  }
}
