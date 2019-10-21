/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.server;

import org.apache.hadoop.hdds.function.FunctionWithServiceException;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.protocolPB.ProtocolMessageMetrics;

import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.ServiceException;
import io.opentracing.Scope;
import org.slf4j.Logger;

/**
 * Dispatch message after tracing and message logging for insight.
 * <p>
 * This is a generic utility to dispatch message in ServerSide translators.
 * <p>
 * It logs the message type/content on DEBUG/TRACING log for insight and create
 * a new span based on the tracing information.
 */
public class OzoneProtocolMessageDispatcher<REQUEST, RESPONSE> {

  private String serviceName;

  private final ProtocolMessageMetrics protocolMessageMetrics;

  private Logger logger;

  public OzoneProtocolMessageDispatcher(String serviceName,
      ProtocolMessageMetrics protocolMessageMetrics, Logger logger) {
    this.serviceName = serviceName;
    this.protocolMessageMetrics = protocolMessageMetrics;
    this.logger = logger;
  }

  public RESPONSE processRequest(
      REQUEST request,
      FunctionWithServiceException<REQUEST, RESPONSE> methodCall,
      ProtocolMessageEnum type,
      String traceId) throws ServiceException {
    Scope scope = TracingUtil
        .importAndCreateScope(type.toString(), traceId);
    try {
      if (logger.isTraceEnabled()) {
        logger.trace(
            "{} {} request is received: <json>{}</json>",
            serviceName,
            type.toString(),
            request.toString().replaceAll("\n", "\\\\n"));
      } else if (logger.isDebugEnabled()) {
        logger.debug("{} {} request is received",
            serviceName, type.toString());
      }
      protocolMessageMetrics.increment(type);

      RESPONSE response = methodCall.apply(request);

      if (logger.isTraceEnabled()) {
        logger.trace(
            "{} {} request is processed. Response: "
                + "<json>{}</json>",
            serviceName,
            type.toString(),
            response.toString().replaceAll("\n", "\\\\n"));
      }
      return response;

    } finally {
      scope.close();
    }
  }
}
