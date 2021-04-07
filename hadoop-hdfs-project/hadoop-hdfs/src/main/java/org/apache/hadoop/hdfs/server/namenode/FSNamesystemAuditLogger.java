/*
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

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.net.InetAddress;
import java.util.Arrays;

import static org.apache.commons.text.StringEscapeUtils.escapeJava;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY;

/**
 * FSNamesystem Default AuditLogger implementation;used when no access logger
 * is defined in the config file. It can also be explicitly listed in the
 * config file.
 */
@VisibleForTesting
@InterfaceAudience.Private
class FSNamesystemAuditLogger extends DefaultAuditLogger {

  @Override
  public void initialize(Configuration conf) {
    isCallerContextEnabled = conf.getBoolean(
        HADOOP_CALLER_CONTEXT_ENABLED_KEY,
        HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT);
    callerContextMaxLen = conf.getInt(
        HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY,
        HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT);
    callerSignatureMaxLen = conf.getInt(
        HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY,
        HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT);
    logTokenTrackingId = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY,
        DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT);

    debugCmdSet.addAll(Arrays.asList(conf.getTrimmedStrings(
        DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_DEBUG_CMDLIST)));
  }

  @Override
  public void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus status, CallerContext callerContext, UserGroupInformation ugi,
      DelegationTokenSecretManager dtSecretManager) {

    if (FSNamesystem.auditLog.isDebugEnabled() ||
        (FSNamesystem.auditLog.isInfoEnabled() && !debugCmdSet.contains(cmd))) {
      final StringBuilder sb = STRING_BUILDER.get();
      src = escapeJava(src);
      dst = escapeJava(dst);
      sb.setLength(0);
      sb.append("allowed=").append(succeeded).append("\t")
          .append("ugi=").append(userName).append("\t")
          .append("ip=").append(addr).append("\t")
          .append("cmd=").append(cmd).append("\t")
          .append("src=").append(src).append("\t")
          .append("dst=").append(dst).append("\t");
      if (null == status) {
        sb.append("perm=null");
      } else {
        sb.append("perm=")
            .append(status.getOwner()).append(":")
            .append(status.getGroup()).append(":")
            .append(status.getPermission());
      }
      if (logTokenTrackingId) {
        sb.append("\t").append("trackingId=");
        String trackingId = null;
        if (ugi != null && dtSecretManager != null
            && ugi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.TOKEN) {
          for (TokenIdentifier tid : ugi.getTokenIdentifiers()) {
            if (tid instanceof DelegationTokenIdentifier) {
              DelegationTokenIdentifier dtid =
                  (DelegationTokenIdentifier) tid;
              trackingId = dtSecretManager.getTokenTrackingId(dtid);
              break;
            }
          }
        }
        sb.append(trackingId);
      }
      sb.append("\t").append("proto=")
          .append(Server.getProtocol());
      if (isCallerContextEnabled &&
          callerContext != null &&
          callerContext.isContextValid()) {
        sb.append("\t").append("callerContext=");
        if (callerContext.getContext().length() > callerContextMaxLen) {
          sb.append(callerContext.getContext(), 0, callerContextMaxLen);
        } else {
          sb.append(callerContext.getContext());
        }
        if (callerContext.getSignature() != null &&
            callerContext.getSignature().length > 0 &&
            callerContext.getSignature().length <= callerSignatureMaxLen) {
          sb.append(":")
              .append(new String(callerContext.getSignature(),
                  CallerContext.SIGNATURE_ENCODING));
        }
      }
      logAuditMessage(sb.toString());
    }
  }

  @Override
  public void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus status, UserGroupInformation ugi,
      DelegationTokenSecretManager dtSecretManager) {
    this.logAuditEvent(succeeded, userName, addr, cmd, src, dst, status,
        null /*CallerContext*/, ugi, dtSecretManager);
  }

  public void logAuditMessage(String message) {
    FSNamesystem.auditLog.info(message);
  }
}
