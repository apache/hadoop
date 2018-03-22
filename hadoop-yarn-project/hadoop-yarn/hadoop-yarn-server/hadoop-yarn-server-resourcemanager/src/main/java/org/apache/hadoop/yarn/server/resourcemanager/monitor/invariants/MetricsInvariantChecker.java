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
package org.apache.hadoop.yarn.server.resourcemanager.monitor.invariants;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This policy checks at every invocation that a given set of invariants
 * (specified in a file) are respected over QueueMetrics and JvmMetrics. The
 * file may contain arbitrary (Javascrip) boolean expression over the metrics
 * variables.
 *
 * The right set of invariants depends on the deployment environment, a large
 * number of complex invariant can make this check expensive.
 *
 * The MetricsInvariantChecker can be configured to throw a RuntimeException or
 * simlpy warn in the logs if an invariant is not respected.
 */
public class MetricsInvariantChecker extends InvariantsChecker {

  private static final Logger LOG =
      LoggerFactory.getLogger(MetricsInvariantChecker.class);
  public static final String INVARIANTS_FILE =
      "yarn.resourcemanager.invariant-checker.file";

  private MetricsSystem metricsSystem;
  private MetricsCollectorImpl collector;
  private SimpleBindings bindings;
  private ScriptEngineManager manager;
  private Compilable scriptEngine;
  private String invariantFile;
  private Map<String, CompiledScript> invariants;
  private CompiledScript combinedInvariants;

  // set of metrics we monitor
  private QueueMetrics queueMetrics;
  private JvmMetrics jvmMetrics;

  @Override
  public void init(Configuration config, RMContext rmContext,
      ResourceScheduler scheduler) {

    super.init(config, rmContext, scheduler);

    this.metricsSystem = DefaultMetricsSystem.instance();
    this.queueMetrics =
        QueueMetrics.forQueue(metricsSystem, "root", null, false, getConf());
    this.jvmMetrics = (JvmMetrics) metricsSystem.getSource("JvmMetrics");

    // at first collect all metrics
    collector = new MetricsCollectorImpl();
    queueMetrics.getMetrics(collector, true);
    jvmMetrics.getMetrics(collector, true);

    // prepare bindings and evaluation engine
    this.bindings = new SimpleBindings();
    this.manager = new ScriptEngineManager();
    this.scriptEngine = (Compilable) manager.getEngineByName("JavaScript");

    // load metrics invariant from file
    this.invariantFile = getConf().get(MetricsInvariantChecker.INVARIANTS_FILE);

    this.invariants = new HashMap<>();

    // preload all bindings
    queueMetrics.getMetrics(collector, true);
    jvmMetrics.getMetrics(collector, true);
    for (MetricsRecord record : collector.getRecords()) {
      for (AbstractMetric am : record.metrics()) {
        bindings.put(am.name().replace(' ', '_'), am.value());
      }
    }

    StringBuilder sb = new StringBuilder();
    try {
      List<String> tempInv =
          Files.readLines(new File(invariantFile), Charsets.UTF_8);


      boolean first = true;
      // precompile individual invariants
      for (String inv : tempInv) {

        if(first) {
          first = false;
        } else {
          sb.append("&&");
        }

        invariants.put(inv, scriptEngine.compile(inv));
        sb.append(" (");
        sb.append(inv);
        sb.append(") ");
      }

      // create a single large combined invariant for speed of checking
      combinedInvariants = scriptEngine.compile(sb.toString());

    } catch (IOException e) {
      throw new RuntimeException(
          "Error loading invariant file: " + e.getMessage());
    } catch (ScriptException e) {
      throw new RuntimeException("Error compiling invariant " + e.getMessage());
    }

  }

  @Override
  public void editSchedule() {
    // grab all changed metrics and update bindings
    collector.clear();
    queueMetrics.getMetrics(collector, false);
    jvmMetrics.getMetrics(collector, false);

    for (MetricsRecord record : collector.getRecords()) {
      for (AbstractMetric am : record.metrics()) {
        bindings.put(am.name().replace(' ', '_'), am.value());
      }
    }

    // evaluate all invariants with new bindings
    try {

      // fastpath check all invariants at once (much faster)
      boolean allInvHold = (boolean) combinedInvariants.eval(bindings);

      // if any fails, check individually to produce more insightful log
      if (!allInvHold) {
        for (Map.Entry<String, CompiledScript> e : invariants.entrySet()) {
          boolean invariantsHold = (boolean) e.getValue().eval(bindings);
          if (!invariantsHold) {
            // filter bindings to produce minimal set
            Map<String, Object> matchingBindings =
                extractMatchingBindings(e.getKey(), bindings);
            logOrThrow("Invariant \"" + e.getKey()
                + "\" is NOT holding, with bindings: " + matchingBindings);
          }
        }
      }
    } catch (ScriptException e) {
      logOrThrow(e.getMessage());
    }
  }

  private static Map<String, Object> extractMatchingBindings(String inv,
      SimpleBindings allBindings) {
    Map<String, Object> matchingBindings = new HashMap<>();
    for (Map.Entry<String, Object> s : allBindings.entrySet()) {
      if (inv.contains(s.getKey())) {
        matchingBindings.put(s.getKey(), s.getValue());
      }
    }
    return matchingBindings;
  }
}
