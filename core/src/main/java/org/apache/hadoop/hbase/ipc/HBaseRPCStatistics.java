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

import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.metrics.util.MetricsDynamicMBeanBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;

import javax.management.ObjectName;

/**
 * Exports HBase RPC statistics recorded in {@link HBaseRpcMetrics} as an MBean
 * for JMX monitoring.
 */
public class HBaseRPCStatistics extends MetricsDynamicMBeanBase {
  private final ObjectName mbeanName;

  @SuppressWarnings({"UnusedDeclaration"})
  public HBaseRPCStatistics(MetricsRegistry registry,
      String hostName, String port) {
	  super(registry, "HBaseRPCStatistics");

    String name = String.format("RPCStatistics-%s", 
        (port != null ? port : "unknown"));

    mbeanName = MBeanUtil.registerMBean("HBase", name, this);
  }

  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
  }

}
