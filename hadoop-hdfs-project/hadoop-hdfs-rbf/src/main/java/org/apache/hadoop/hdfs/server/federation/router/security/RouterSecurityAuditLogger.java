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

package org.apache.hadoop.hdfs.server.federation.router.security;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT;

public class RouterSecurityAuditLogger {

  public static final Logger AUDIT_LOG = LoggerFactory.getLogger(
      RouterSecurityManager.class.getName() + ".audit");

  private static final ThreadLocal<StringBuilder> STRING_BUILDER =
      new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  private int callerContextMaxLen;
  private int callerSignatureMaxLen;

  public RouterSecurityAuditLogger(Configuration conf) {
    callerContextMaxLen = conf.getInt(
        HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY,
        HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT);
    callerSignatureMaxLen = conf.getInt(
        HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY,
        HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT);
  }

  public void logAuditEvent(boolean succeeded, String userName,
                            InetAddress addr, String cmd,
                            CallerContext callerContext, String tokenId) {
    if (AUDIT_LOG.isDebugEnabled() || AUDIT_LOG.isInfoEnabled()) {
      logAuditMessage(
          creatAuditLog(succeeded, userName, addr, cmd, callerContext,
              tokenId));
    }
  }

  @VisibleForTesting
  public String creatAuditLog(boolean succeeded, String userName,
                              InetAddress addr, String cmd,
                              CallerContext callerContext, String tokenId) {
    final StringBuilder sb = STRING_BUILDER.get();
    sb.setLength(0);
    sb.append("allowed=").append(succeeded).append("\t");
    sb.append("ugi=").append(userName).append("\t");
    sb.append("ip=").append(addr).append("\t");
    sb.append("cmd=").append(cmd).append("\t");

    sb.append("\t").append("toeknId=");
    sb.append(tokenId);

    sb.append("\t").append("proto=");
    sb.append(Server.getProtocol());
    if (
        callerContext != null &&
            callerContext.isContextValid()) {
      sb.append("\t").append("callerContext=");
      if (callerContext.getContext().length() > callerContextMaxLen) {
        sb.append(callerContext.getContext().substring(0,
            callerContextMaxLen));
      } else {
        sb.append(callerContext.getContext());
      }
      if (callerContext.getSignature() != null &&
          callerContext.getSignature().length > 0 &&
          callerContext.getSignature().length <= callerSignatureMaxLen) {
        sb.append(":");
        sb.append(new String(callerContext.getSignature(),
            CallerContext.SIGNATURE_ENCODING));
      }
    }
    return sb.toString();
  }

  private void logAuditMessage(String message) {
    AUDIT_LOG.info(message);
  }

}