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
package org.apache.hadoop.ozone.insight.scm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StorageContainerLocationProtocolService;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.insight.LoggerSource;
import org.apache.hadoop.ozone.insight.MetricGroupDisplay;

/**
 * Insight metric to check the SCM block location protocol behaviour.
 */
public class ScmProtocolContainerLocationInsight extends BaseInsightPoint {

  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose,
      Map<String, String> filters) {
    List<LoggerSource> loggers = new ArrayList<>();
    loggers.add(
        new LoggerSource(Type.SCM,
            StorageContainerLocationProtocolServerSideTranslatorPB.class,
            defaultLevel(verbose)));
    new LoggerSource(Type.SCM,
        StorageContainerLocationProtocolService.class,
        defaultLevel(verbose));
    return loggers;
  }

  @Override
  public List<MetricGroupDisplay> getMetrics() {
    List<MetricGroupDisplay> metrics = new ArrayList<>();

    Map<String, String> filter = new HashMap<>();
    filter.put("servername", "StorageContainerLocationProtocolService");

    addRpcMetrics(metrics, Type.SCM, filter);

    addProtocolMessageMetrics(metrics, "scm_container_location_protocol",
        Type.SCM, StorageContainerLocationProtocolProtos.Type.values());

    return metrics;
  }

  @Override
  public String getDescription() {
    return "SCM Container location protocol endpoint";
  }

}
