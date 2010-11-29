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
package org.apache.hadoop.hbase.metrics;

import org.apache.hadoop.hbase.metrics.MetricsMBeanBase;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.metrics.util.MetricsRegistry;

import javax.management.ObjectName;

/**
 * Exports HBase system information as an MBean for JMX observation.
 */
public class HBaseInfo {
  protected static class HBaseInfoMBean extends MetricsMBeanBase {
    private final ObjectName mbeanName;
  
    public HBaseInfoMBean(MetricsRegistry registry, String rsName) {
      super(registry, "HBaseInfo");
      mbeanName = MBeanUtil.registerMBean("HBase",
          "Info", this);
    }
  
    public void shutdown() {
      if (mbeanName != null)
        MBeanUtil.unregisterMBean(mbeanName);
    }
  }

  protected final MetricsRecord mr;
  protected final HBaseInfoMBean mbean;
  protected MetricsRegistry registry = new MetricsRegistry();

  private static HBaseInfo theInstance = null;
  public synchronized static HBaseInfo init() {
      if (theInstance == null) {
        theInstance = new HBaseInfo();
      }
      return theInstance;
  }
  
  // HBase jar info
  private MetricsString date = new MetricsString("date", registry,
      org.apache.hadoop.hbase.util.VersionInfo.getDate());
  private MetricsString revision = new MetricsString("revision", registry, 
      org.apache.hadoop.hbase.util.VersionInfo.getRevision());
  private MetricsString url = new MetricsString("url", registry,
      org.apache.hadoop.hbase.util.VersionInfo.getUrl());
  private MetricsString user = new MetricsString("user", registry,
      org.apache.hadoop.hbase.util.VersionInfo.getUser());
  private MetricsString version = new MetricsString("version", registry,
      org.apache.hadoop.hbase.util.VersionInfo.getVersion());

  // Info on the HDFS jar that HBase has (aka: HDFS Client)
  private MetricsString hdfsDate = new MetricsString("hdfsDate", registry,
      org.apache.hadoop.util.VersionInfo.getDate());
  private MetricsString hdfsRev = new MetricsString("hdfsRevision", registry,
      org.apache.hadoop.util.VersionInfo.getRevision());
  private MetricsString hdfsUrl = new MetricsString("hdfsUrl", registry,
      org.apache.hadoop.util.VersionInfo.getUrl());
  private MetricsString hdfsUser = new MetricsString("hdfsUser", registry,
      org.apache.hadoop.util.VersionInfo.getUser());
  private MetricsString hdfsVer = new MetricsString("hdfsVersion", registry,
      org.apache.hadoop.util.VersionInfo.getVersion());

  protected HBaseInfo() {
    MetricsContext context = MetricsUtil.getContext("hbase");
    mr = MetricsUtil.createRecord(context, "info");
    String name = Thread.currentThread().getName();
    mr.setTag("Info", name);

    // export for JMX
    mbean = new HBaseInfoMBean(this.registry, name);
  }

}
