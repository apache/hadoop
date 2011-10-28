package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.apache.hadoop.yarn.server.nodemanager.webapp.NMWebParams.CONTAINER_ID;
import static org.apache.hadoop.yarn.server.nodemanager.webapp.NMWebParams.NM_NODENAME;
import static org.apache.hadoop.yarn.server.nodemanager.webapp.NMWebParams.ENTITY_STRING;
import static org.apache.hadoop.yarn.server.nodemanager.webapp.NMWebParams.APP_OWNER;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.LogAggregationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class AggregatedLogsBlock extends HtmlBlock {

  private final Configuration conf;

  @Inject
  AggregatedLogsBlock(Configuration conf) {
    this.conf = conf;
  }

  @Override
  protected void render(Block html) {
    ContainerId containerId = verifyAndGetContainerId(html);
    NodeId nodeId = verifyAndGetNodeId(html);
    String appOwner = verifyAndGetAppOwner(html);
    if (containerId == null || nodeId == null || appOwner == null
        || appOwner.isEmpty()) {
      return;
    }
    
    ApplicationId applicationId =
        containerId.getApplicationAttemptId().getApplicationId();
    String logEntity = $(ENTITY_STRING);
    if (logEntity == null || logEntity.isEmpty()) {
      logEntity = containerId.toString();
    }

    Path remoteRootLogDir =
        new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    AggregatedLogFormat.LogReader reader = null;
    try {
      reader =
          new AggregatedLogFormat.LogReader(conf,
              LogAggregationService.getRemoteNodeLogFileForApp(
                  remoteRootLogDir, applicationId, appOwner, nodeId,
                  LogAggregationService.getRemoteNodeLogDirSuffix(conf)));
    } catch (FileNotFoundException e) {
      // ACLs not available till the log file is opened.
      html.h1()
          ._("Logs not available for "
              + logEntity
              + ". Aggregation may not be complete, "
              + "Check back later or try the nodemanager on "
              + nodeId)._();
      return;
    } catch (IOException e) {
      html.h1()._("Error getting logs for " + logEntity)._();
      LOG.error("Error getting logs for " + logEntity, e);
      return;
    }

    String owner = null;
    Map<ApplicationAccessType, String> appAcls = null;
    try {
      owner = reader.getApplicationOwner();
      appAcls = reader.getApplicationAcls();
    } catch (IOException e) {
      html.h1()._("Error getting logs for " + logEntity)._();
      LOG.error("Error getting logs for " + logEntity, e);
      return;
    }
    ApplicationACLsManager aclsManager = new ApplicationACLsManager(conf);
    aclsManager.addApplication(applicationId, appAcls);

    String remoteUser = request().getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null
        && !aclsManager.checkAccess(callerUGI, ApplicationAccessType.VIEW_APP,
            owner, applicationId)) {
      html.h1()
          ._("User [" + remoteUser
              + "] is not authorized to view the logs for " + logEntity)._();
      return;
    }

    DataInputStream valueStream;
    LogKey key = new LogKey();
    try {
      valueStream = reader.next(key);
      while (valueStream != null
          && !key.toString().equals(containerId.toString())) {
        valueStream = reader.next(key);
      }
      if (valueStream == null) {
        html.h1()._(
            "Logs not available for " + logEntity
                + ". Could be caused by the rentention policy")._();
        return;
      }
      writer().write("<pre>");
      AggregatedLogFormat.LogReader.readAcontainerLogs(valueStream, writer());
      writer().write("</pre>");
      return;
    } catch (IOException e) {
      html.h1()._("Error getting logs for " + logEntity)._();
      LOG.error("Error getting logs for " + logEntity, e);
      return;
    }
  }

  private ContainerId verifyAndGetContainerId(Block html) {
    String containerIdStr = $(CONTAINER_ID);
    if (containerIdStr == null || containerIdStr.isEmpty()) {
      html.h1()._("Cannot get container logs without a ContainerId")._();
      return null;
    }
    ContainerId containerId = null;
    try {
      containerId = ConverterUtils.toContainerId(containerIdStr);
    } catch (IllegalArgumentException e) {
      html.h1()
          ._("Cannot get container logs for invalid containerId: "
              + containerIdStr)._();
      return null;
    }
    return containerId;
  }

  private NodeId verifyAndGetNodeId(Block html) {
    String nodeIdStr = $(NM_NODENAME);
    if (nodeIdStr == null || nodeIdStr.isEmpty()) {
      html.h1()._("Cannot get container logs without a NodeId")._();
      return null;
    }
    NodeId nodeId = null;
    try {
      nodeId = ConverterUtils.toNodeId(nodeIdStr);
    } catch (IllegalArgumentException e) {
      html.h1()._("Cannot get container logs. Invalid nodeId: " + nodeIdStr)
          ._();
      return null;
    }
    return nodeId;
  }
  
  private String verifyAndGetAppOwner(Block html) {
    String appOwner = $(APP_OWNER);
    if (appOwner == null || appOwner.isEmpty()) {
      html.h1()._("Cannot get container logs without an app owner")._();
    }
    return appOwner;
  }
}