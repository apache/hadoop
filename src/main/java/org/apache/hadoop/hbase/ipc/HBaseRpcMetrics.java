/**
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

import java.lang.reflect.Method;

/**
 *
 * This class is for maintaining  the various RPC statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 * for example:
 *  <p> {@link #rpcQueueTime}.inc(time)
 *
 */
public class HBaseRpcMetrics implements Updater {
  public static final String NAME_DELIM = "$";
  private MetricsRecord metricsRecord;
  private static Log LOG = LogFactory.getLog(HBaseRpcMetrics.class);
  private final HBaseRPCStatistics rpcStatistics;

  public HBaseRpcMetrics(String hostName, String port) {
    MetricsContext context = MetricsUtil.getContext("rpc");
    metricsRecord = MetricsUtil.createRecord(context, "metrics");

    metricsRecord.setTag("port", port);

    LOG.info("Initializing RPC Metrics with hostName="
        + hostName + ", port=" + port);

    context.registerUpdater(this);

    initMethods(HMasterInterface.class);
    initMethods(HMasterRegionInterface.class);
    initMethods(HRegionInterface.class);
    rpcStatistics = new HBaseRPCStatistics(this.registry, hostName, port);
  }


  /**
   * The metrics variables are public:
   *  - they can be set directly by calling their set/inc methods
   *  -they can also be read directly - e.g. JMX does this.
   */
  public final MetricsRegistry registry = new MetricsRegistry();

  public MetricsTimeVaryingRate rpcQueueTime = new MetricsTimeVaryingRate("RpcQueueTime", registry);
  public MetricsTimeVaryingRate rpcProcessingTime = new MetricsTimeVaryingRate("RpcProcessingTime", registry);

  //public Map <String, MetricsTimeVaryingRate> metricsList = Collections.synchronizedMap(new HashMap<String, MetricsTimeVaryingRate>());

  private void initMethods(Class<? extends VersionedProtocol> protocol) {
    for (Method m : protocol.getDeclaredMethods()) {
      if (get(m.getName()) == null)
        create(m.getName());
    }
  }

  private MetricsTimeVaryingRate get(String key) {
    return (MetricsTimeVaryingRate) registry.get(key);
  }
  private MetricsTimeVaryingRate create(String key) {
    return new MetricsTimeVaryingRate(key, this.registry);
  }

  public void inc(String name, int amt) {
    MetricsTimeVaryingRate m = get(name);
    if (m == null) {
      LOG.warn("Got inc() request for method that doesnt exist: " +
      name);
      return; // ignore methods that dont exist.
    }
    m.inc(amt);
  }

  /**
   * Generate metrics entries for all the methods defined in the list of
   * interfaces.  A {@link MetricsTimeVaryingRate} counter will be created for
   * each {@code Class.getMethods().getName()} entry.
   * @param ifaces Define metrics for all methods in the given classes
   */
  public void createMetrics(Class<?>[] ifaces) {
    createMetrics(ifaces, false);
  }

  /**
   * Generate metrics entries for all the methods defined in the list of
   * interfaces.  A {@link MetricsTimeVaryingRate} counter will be created for
   * each {@code Class.getMethods().getName()} entry.
   *
   * <p>
   * If {@code prefixWithClass} is {@code true}, each metric will be named as
   * {@code [Class.getSimpleName()].[Method.getName()]}.  Otherwise each metric
   * will just be named according to the method -- {@code Method.getName()}.
   * </p>
   * @param ifaces Define metrics for all methods in the given classes
   * @param prefixWithClass If {@code true}, each metric will be named as
   *     "classname.method"
   */
  public void createMetrics(Class<?>[] ifaces, boolean prefixWithClass) {
    for (Class<?> iface : ifaces) {
      Method[] methods = iface.getMethods();
      for (Method method : methods) {
        String attrName = prefixWithClass ?
            getMetricName(iface, method.getName()) : method.getName();
        if (get(attrName) == null)
          create(attrName);
      }
    }
  }

  public static String getMetricName(Class<?> c, String method) {
    return c.getSimpleName() + NAME_DELIM + method;
  }

  /**
   * Push the metrics to the monitoring subsystem on doUpdate() call.
   * @param context ctx
   */
  public void doUpdates(MetricsContext context) {
    rpcQueueTime.pushMetric(metricsRecord);
    rpcProcessingTime.pushMetric(metricsRecord);

    synchronized (registry) {
      // Iterate through the registry to propagate the different rpc metrics.

      for (String metricName : registry.getKeyList() ) {
        MetricsTimeVaryingRate value = (MetricsTimeVaryingRate) registry.get(metricName);

        value.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }

  public void shutdown() {
    if (rpcStatistics != null)
      rpcStatistics.shutdown();
  }
}