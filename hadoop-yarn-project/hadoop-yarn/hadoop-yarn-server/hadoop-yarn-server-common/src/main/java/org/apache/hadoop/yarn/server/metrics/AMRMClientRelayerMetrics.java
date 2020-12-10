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

package org.apache.hadoop.yarn.server.metrics;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Metrics for FederationInterceptor Internals.
 */
@InterfaceAudience.Private
@Metrics(about = "Performance and usage metrics for YARN AMRMClientRelayer",
    context = "fedr")
public final class AMRMClientRelayerMetrics implements MetricsSource{

  /**
   * Easier classification of request types for logging metrics.
   */
  public enum RequestType {
    Guaranteed, Opportunistic, Promote, Demote;

    @Override
    public String toString() {
      switch (this) {
      case Guaranteed:
        return "G";
      case Opportunistic:
        return "O";
      case Promote:
        return "P";
      case Demote:
        return "D";
      default:
        throw new IllegalArgumentException();
      }
    }
  }

  private static AtomicBoolean isInitialized = new AtomicBoolean(false);

  private static final MetricsInfo RECORD_INFO =
      info("AMRMClientRelayerMetrics",
          "Metrics for the Yarn AMRMClientRelayer");

  private static volatile AMRMClientRelayerMetrics instance = null;
  private static MetricsRegistry registry;

  // The metrics are set up as a map from string (typically sub cluster id) to
  // request type (Guaranteed, Opp, Promote, Demote) to the counter.
  // The counters are constructed lazily when the first metric entry
  // comes in.
  // For some metrics, request type is not applicable.
  private final Map<String, Map<RequestType, MutableGaugeLong>>
      rmClientPending = new ConcurrentHashMap<>();

  private final Map<String, Map<RequestType, MutableQuantiles>> fulfillLatency =
      new ConcurrentHashMap<>();

  private final Map<String, Map<RequestType, MutableGaugeLong>>
      requestedQps = new ConcurrentHashMap<>();

  private final Map<String, Map<RequestType, MutableGaugeLong>>
      fulfilledQps = new ConcurrentHashMap<>();

  private final Map<String, MutableGaugeLong> rmMasterSlaveSwitch =
      new ConcurrentHashMap<>();

  private final Map<String, MutableGaugeLong> heartbeatFailure =
      new ConcurrentHashMap<>();

  private final Map<String, MutableGaugeLong> heartbeatSuccess =
      new ConcurrentHashMap<>();
  private final Map<String, MutableQuantiles> heartbeatLatency =
      new ConcurrentHashMap<>();

  /**
   * Initialize the singleton instance.
   *
   * @return the singleton
   */
  public static AMRMClientRelayerMetrics getInstance() {
    if (!isInitialized.get()) {
      synchronized (AMRMClientRelayerMetrics.class) {
        if (instance == null) {
          instance = new AMRMClientRelayerMetrics();
          DefaultMetricsSystem.instance().register(RECORD_INFO.name(),
              RECORD_INFO.description(), instance);
          isInitialized.set(true);
        }
      }
    }
    return instance;
  }

  private AMRMClientRelayerMetrics()  {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "AMRMClientRelayer");
  }

  public static RequestType getRequestType(ExecutionType execType) {
    if (execType == null || execType.equals(ExecutionType.GUARANTEED)) {
      return RequestType.Guaranteed;
    }
    return RequestType.Opportunistic;
  }

  @VisibleForTesting
  protected MutableGaugeLong getPendingMetric(String instanceId,
      RequestType type) {
    synchronized (rmClientPending) {
      if (!rmClientPending.containsKey(instanceId)) {
        rmClientPending.put(instanceId,
            new ConcurrentHashMap<RequestType, MutableGaugeLong>());
      }
      if (!rmClientPending.get(instanceId).containsKey(type)) {
        rmClientPending.get(instanceId).put(type, registry
            .newGauge(type.toString() + "Pending" + instanceId,
                "Remove pending of " + type + " for " + instanceId, 0L));
      }
    }
    return rmClientPending.get(instanceId).get(type);
  }

  public void incrClientPending(String instanceId, RequestType type, int diff) {
    getPendingMetric(instanceId, type).incr(diff);
  }

  public void decrClientPending(String instanceId, RequestType type, int diff) {
    getPendingMetric(instanceId, type).decr(diff);
  }

  @VisibleForTesting
  protected void setClientPending(String instanceId, RequestType type,
      int val) {
    getPendingMetric(instanceId, type).set(val);
  }

  @VisibleForTesting
  protected MutableQuantiles getFulfillLatencyMetric(String instanceId,
      RequestType type) {
    synchronized (fulfillLatency) {
      if (!fulfillLatency.containsKey(instanceId)) {
        fulfillLatency.put(instanceId,
            new ConcurrentHashMap<RequestType, MutableQuantiles>());
      }
      if (!fulfillLatency.get(instanceId).containsKey(type)) {
        fulfillLatency.get(instanceId).put(type, registry
            .newQuantiles(type.toString() + "FulfillLatency" + instanceId,
                "FulfillLatency of " + type + " for " + instanceId, "ops",
                "latency", 60));
      }
    }
    return fulfillLatency.get(instanceId).get(type);
  }

  public void addFulfillLatency(String instanceId, RequestType type,
      long latency) {
    getFulfillLatencyMetric(instanceId, type).add(latency);
  }

  public void addFulfillLatency(String instanceId, ContainerUpdateType type,
      long latency) {
    switch(type) {
    case DEMOTE_EXECUTION_TYPE:
      addFulfillLatency(instanceId, RequestType.Demote, latency);
      break;
    case PROMOTE_EXECUTION_TYPE:
      addFulfillLatency(instanceId, RequestType.Promote, latency);
      break;
    default:
      break;
    }
  }

  @VisibleForTesting
  protected MutableGaugeLong getRequestedQPSMetric(String instanceId,
      RequestType type) {
    synchronized (requestedQps) {
      if (!requestedQps.containsKey(instanceId)) {
        requestedQps.put(instanceId,
            new ConcurrentHashMap<RequestType, MutableGaugeLong>());
      }
      if (!requestedQps.get(instanceId).containsKey(type)) {
        requestedQps.get(instanceId)
            .put(type, registry.newGauge(
                info(type.toString() + "RequestedOps" + instanceId,
                    "Requested operations of " + type + " for " + instanceId),
                0L));
      }
    }
    return requestedQps.get(instanceId).get(type);
  }

  public void addRequestedQPS(String instanceId, RequestType type,
      long numEntries) {
    getRequestedQPSMetric(instanceId, type).incr(numEntries);
  }

  @VisibleForTesting
  protected MutableGaugeLong getFulfilledQPSMetric(String instanceId,
      RequestType type) {
    synchronized (fulfilledQps) {
      if (!fulfilledQps.containsKey(instanceId)) {
        fulfilledQps.put(instanceId,
            new ConcurrentHashMap<RequestType, MutableGaugeLong>());
      }
      if (!fulfilledQps.get(instanceId).containsKey(type)) {
        fulfilledQps.get(instanceId)
            .put(type, registry.newGauge(
                info(type.toString() + "FulfilledOps" + instanceId,
                    "Fulfilled operations of " + type + " for " + instanceId),
                0L));
      }
    }
    return fulfilledQps.get(instanceId).get(type);
  }

  public void addFulfilledQPS(String instanceId, RequestType type,
      long numEntries) {
    getFulfilledQPSMetric(instanceId, type).incr(numEntries);
  }

  public void addFulfilledQPS(String instanceId, ContainerUpdateType type,
      long latency) {
    switch(type) {
    case DEMOTE_EXECUTION_TYPE:
      addFulfilledQPS(instanceId, RequestType.Demote, latency);
      break;
    case PROMOTE_EXECUTION_TYPE:
      addFulfilledQPS(instanceId, RequestType.Promote, latency);
      break;
    default:
      break;
    }
  }

  public void incrClientPending(String scId, ContainerUpdateType type,
      int diff) {
    switch(type) {
    case DEMOTE_EXECUTION_TYPE:
      incrClientPending(scId, RequestType.Demote, diff);
      break;
    case PROMOTE_EXECUTION_TYPE:
      incrClientPending(scId, RequestType.Promote, diff);
      break;
    default:
      break;
    }
  }

  public void decrClientPending(String scId, ContainerUpdateType type,
      int diff) {
    switch(type) {
    case DEMOTE_EXECUTION_TYPE:
      decrClientPending(scId, RequestType.Demote, diff);
      break;
    case PROMOTE_EXECUTION_TYPE:
      decrClientPending(scId, RequestType.Promote, diff);
      break;
    default:
      break;
    }
  }

  @VisibleForTesting
  protected MutableGaugeLong getRMMasterSlaveSwitchMetric(
      String instanceId) {
    synchronized (rmMasterSlaveSwitch) {
      if (!rmMasterSlaveSwitch.containsKey(instanceId)) {
        rmMasterSlaveSwitch.put(instanceId, registry.newGauge(
            info("RMMasterSlaveSwitch" + instanceId,
                "Number of RM master slave switch"), 0L));
      }
    }
    return rmMasterSlaveSwitch.get(instanceId);
  }

  public void incrRMMasterSlaveSwitch(String instanceId) {
    getRMMasterSlaveSwitchMetric(instanceId).incr();
  }

  @VisibleForTesting
  protected MutableQuantiles getHeartbeatLatencyMetric(String instanceId) {
    synchronized (heartbeatLatency) {
      if (!heartbeatLatency.containsKey(instanceId)) {
        heartbeatLatency.put(instanceId, registry
            .newQuantiles("HeartbeatLatency" + instanceId,
                "HeartbeatLatency for " + instanceId, "ops", "latency", 60));
      }
    }
    return heartbeatLatency.get(instanceId);
  }

  @VisibleForTesting
  protected MutableGaugeLong getHeartbeatFailureMetric(
      String instanceId) {
    synchronized (heartbeatFailure) {
      if (!heartbeatFailure.containsKey(instanceId)) {
        heartbeatFailure.put(instanceId, registry.newGauge(
            info("HeartbeatFailure" + instanceId,
                "Number of Heartbeat Failures"), 0L));
      }
    }
    return heartbeatFailure.get(instanceId);
  }

  public void addHeartbeatFailure(String instanceId, long latency) {
    getHeartbeatFailureMetric(instanceId).incr();

    getHeartbeatLatencyMetric(instanceId).add(latency);
  }

  @VisibleForTesting
  protected MutableGaugeLong getHeartbeatSuccessMetric(
      String instanceId) {
    synchronized (heartbeatSuccess) {
      if (!heartbeatSuccess.containsKey(instanceId)) {
        heartbeatSuccess.put(instanceId, registry.newGauge(
            info("HeartbeatSuccess" + instanceId,
                "Number of Heartbeat"), 0L));
      }
    }
    return heartbeatSuccess.get(instanceId);
  }

  public void addHeartbeatSuccess(String instanceId, long latency) {
    getHeartbeatSuccessMetric(instanceId).incr();

    getHeartbeatLatencyMetric(instanceId).add(latency);
  }

  @Override
  public void getMetrics(MetricsCollector builder, boolean all) {
    registry.snapshot(builder.addRecord(registry.info().name()), all);
  }
}
