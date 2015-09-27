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
package org.apache.hadoop.tracing;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.core.TracerPool;

/**
 * This class provides functions for managing the tracer configuration at
 * runtime via an RPC protocol.
 */
@InterfaceAudience.Private
public class TracerConfigurationManager implements TraceAdminProtocol {
  private static final Log LOG =
      LogFactory.getLog(TracerConfigurationManager.class);

  private final String confPrefix;
  private final Configuration conf;

  public TracerConfigurationManager(String confPrefix, Configuration conf) {
    this.confPrefix = confPrefix;
    this.conf = conf;
  }

  public synchronized SpanReceiverInfo[] listSpanReceivers()
      throws IOException {
    TracerPool pool = TracerPool.getGlobalTracerPool();
    SpanReceiver[] receivers = pool.getReceivers();
    SpanReceiverInfo[] info = new SpanReceiverInfo[receivers.length];
    for (int i = 0; i < receivers.length; i++) {
      SpanReceiver receiver = receivers[i];
      info[i] = new SpanReceiverInfo(receiver.getId(),
          receiver.getClass().getName());
    }
    return info;
  }

  public synchronized long addSpanReceiver(SpanReceiverInfo info)
      throws IOException {
    StringBuilder configStringBuilder = new StringBuilder();
    String prefix = "";
    for (ConfigurationPair pair : info.configPairs) {
      configStringBuilder.append(prefix).append(pair.getKey()).
          append(" = ").append(pair.getValue());
      prefix = ", ";
    }
    SpanReceiver rcvr = null;
    try {
      rcvr = new SpanReceiver.Builder(TraceUtils.wrapHadoopConf(
              confPrefix, conf, info.configPairs)).
          className(info.getClassName().trim()).
          build();
    } catch (RuntimeException e) {
      LOG.info("Failed to add SpanReceiver " + info.getClassName() +
          " with configuration " + configStringBuilder.toString(), e);
      throw e;
    }
    TracerPool.getGlobalTracerPool().addReceiver(rcvr);
    LOG.info("Successfully added SpanReceiver " + info.getClassName() +
        " with configuration " + configStringBuilder.toString());
    return rcvr.getId();
  }

  public synchronized void removeSpanReceiver(long spanReceiverId)
      throws IOException {
    SpanReceiver[] receivers =
        TracerPool.getGlobalTracerPool().getReceivers();
    for (SpanReceiver receiver : receivers) {
      if (receiver.getId() == spanReceiverId) {
        TracerPool.getGlobalTracerPool().removeAndCloseReceiver(receiver);
        LOG.info("Successfully removed SpanReceiver " + spanReceiverId +
            " with class " + receiver.getClass().getName());
        return;
      }
    }
    throw new IOException("There is no span receiver with id " + spanReceiverId);
  }
}
