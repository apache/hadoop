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
package org.apache.hadoop.ozone.insight;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.client.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.server.PrometheusMetricsSink;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.insight.LoggerSource.Level;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.protobuf.ProtocolMessageEnum;
import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForClients;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT;

/**
 * Default implementation of Insight point logic.
 */
public abstract class BaseInsightPoint implements InsightPoint {

  /**
   * List the related metrics.
   */
  @Override
  public List<MetricGroupDisplay> getMetrics() {
    return new ArrayList<>();
  }

  /**
   * List the related configuration.
   */
  @Override
  public List<Class> getConfigurationClasses() {
    return new ArrayList<>();
  }

  /**
   * List the related loggers.
   *
   * @param verbose true if verbose logging is requested.
   * @param filters additional key value pair to further filter the output.
   *                (eg. datanode=123-2323-datanode-id)
   */
  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose,
      Map<String, String> filters) {
    List<LoggerSource> loggers = new ArrayList<>();
    return loggers;
  }

  /**
   * Create scm client.
   */
  public ScmClient createScmClient(OzoneConfiguration ozoneConf)
      throws IOException {

    if (!HddsUtils.getHostNameFromConfigKeys(ozoneConf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY).isPresent()) {

      throw new IllegalArgumentException(
          ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY
              + " should be set in ozone-site.xml");
    }

    long version = RPC.getProtocolVersion(
        StorageContainerLocationProtocolPB.class);
    InetSocketAddress scmAddress =
        getScmAddressForClients(ozoneConf);
    int containerSizeGB = (int) ozoneConf.getStorageSize(
        OZONE_SCM_CONTAINER_SIZE, OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.GB);
    ContainerOperationClient
        .setContainerSizeB(containerSizeGB * OzoneConsts.GB);

    RPC.setProtocolEngine(ozoneConf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    StorageContainerLocationProtocol client =
        TracingUtil.createProxy(
            new StorageContainerLocationProtocolClientSideTranslatorPB(
                RPC.getProxy(StorageContainerLocationProtocolPB.class, version,
                    scmAddress, UserGroupInformation.getCurrentUser(),
                    ozoneConf,
                    NetUtils.getDefaultSocketFactory(ozoneConf),
                    Client.getRpcTimeout(ozoneConf))),
            StorageContainerLocationProtocol.class, ozoneConf);
    return new ContainerOperationClient(
        client, new XceiverClientManager(ozoneConf));
  }

  /**
   * Convenient method to define default log levels.
   */
  public Level defaultLevel(boolean verbose) {
    return verbose ? Level.TRACE : Level.DEBUG;
  }

  /**
   * Default metrics for any message type based RPC ServerSide translators.
   */
  public void addProtocolMessageMetrics(List<MetricGroupDisplay> metrics,
      String prefix,
      Component.Type component,
      ProtocolMessageEnum[] types) {

    MetricGroupDisplay messageTypeCounters =
        new MetricGroupDisplay(component, "Message type counters");
    for (ProtocolMessageEnum type : types) {
      String typeName = type.toString();
      MetricDisplay metricDisplay = new MetricDisplay("Number of " + typeName,
          prefix + "_" + PrometheusMetricsSink
              .normalizeName(typeName));
      messageTypeCounters.addMetrics(metricDisplay);
    }
    metrics.add(messageTypeCounters);
  }

  /**
   * Rpc metrics for any hadoop rpc endpoint.
   */
  public void addRpcMetrics(List<MetricGroupDisplay> metrics,
      Component.Type component,
      Map<String, String> filter) {
    MetricGroupDisplay connection =
        new MetricGroupDisplay(component, "RPC connections");
    connection.addMetrics(new MetricDisplay("Open connections",
        "rpc_num_open_connections", filter));
    connection.addMetrics(
        new MetricDisplay("Dropped connections", "rpc_num_dropped_connections",
            filter));
    connection.addMetrics(
        new MetricDisplay("Received bytes", "rpc_received_bytes",
            filter));
    connection.addMetrics(
        new MetricDisplay("Sent bytes", "rpc_sent_bytes",
            filter));
    metrics.add(connection);

    MetricGroupDisplay queue = new MetricGroupDisplay(component, "RPC queue");
    queue.addMetrics(new MetricDisplay("RPC average queue time",
        "rpc_rpc_queue_time_avg_time", filter));
    queue.addMetrics(
        new MetricDisplay("RPC call queue length", "rpc_call_queue_length",
            filter));
    metrics.add(queue);

    MetricGroupDisplay performance =
        new MetricGroupDisplay(component, "RPC performance");
    performance.addMetrics(new MetricDisplay("RPC processing time average",
        "rpc_rpc_processing_time_avg_time", filter));
    performance.addMetrics(
        new MetricDisplay("Number of slow calls", "rpc_rpc_slow_calls",
            filter));
    metrics.add(performance);
  }

  @Override
  public boolean filterLog(Map<String, String> filters, String logLine) {
    if (filters == null) {
      return true;
    }
    boolean result = true;
    for (Entry<String, String> entry : filters.entrySet()) {
      if (!logLine.matches(
          String.format(".*\\[%s=%s\\].*", entry.getKey(), entry.getValue()))) {
        return false;
      }
    }
    return true;
  }
}
