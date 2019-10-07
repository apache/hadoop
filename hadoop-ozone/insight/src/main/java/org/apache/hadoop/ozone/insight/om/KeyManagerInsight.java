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
package org.apache.hadoop.ozone.insight.om;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.insight.LoggerSource;
import org.apache.hadoop.ozone.insight.MetricDisplay;
import org.apache.hadoop.ozone.insight.MetricGroupDisplay;
import org.apache.hadoop.ozone.om.KeyManagerImpl;

/**
 * Insight implementation for the key management related operations.
 */
public class KeyManagerInsight extends BaseInsightPoint {

  @Override
  public List<MetricGroupDisplay> getMetrics() {
    List<MetricGroupDisplay> display = new ArrayList<>();

    MetricGroupDisplay state =
        new MetricGroupDisplay(Type.OM, "Key related metrics");
    state
        .addMetrics(new MetricDisplay("Number of keys", "om_metrics_num_keys"));
    state.addMetrics(new MetricDisplay("Number of key operations",
        "om_metrics_num_key_ops"));

    display.add(state);

    MetricGroupDisplay key =
        new MetricGroupDisplay(Type.OM, "Key operation stats");
    for (String operation : new String[] {"allocate", "commit", "lookup",
        "list", "delete"}) {
      key.addMetrics(new MetricDisplay(
          "Number of key " + operation + "s (failure + success)",
          "om_metrics_num_key_" + operation));
      key.addMetrics(
          new MetricDisplay("Number of failed key " + operation + "s",
              "om_metrics_num_key_" + operation + "_fails"));
    }
    display.add(key);

    return display;
  }

  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose,
      Map<String, String> filters) {
    List<LoggerSource> loggers = new ArrayList<>();
    loggers.add(
        new LoggerSource(Type.OM, KeyManagerImpl.class,
            defaultLevel(verbose)));
    return loggers;
  }

  @Override
  public String getDescription() {
    return "OM Key Manager";
  }

}
