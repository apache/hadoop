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

package org.apache.hadoop.yarn.server.nodemanager.health;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class provides functionality of checking the health of a node and
 * reporting back to the service for which the health checker has been asked to
 * report.
 *
 * It is a {@link CompositeService}: every {@link Service} must be registered
 * first in serviceInit, and should also implement the {@link HealthReporter}
 * interface - otherwise an exception is thrown.
 *
 * Calling functions of HealthReporter merge its dependent
 * services' reports.
 *
 * @see HealthReporter
 * @see LocalDirsHandlerService
 * @see TimedHealthReporterService
 */
public class NodeHealthCheckerService extends CompositeService
    implements HealthReporter {

  public static final Logger LOG =
      LoggerFactory.getLogger(NodeHealthCheckerService.class);
  private static final int MAX_SCRIPTS = 4;

  private List<HealthReporter> reporters;
  private LocalDirsHandlerService dirsHandler;
  private ExceptionReporter exceptionReporter;

  public static final String SEPARATOR = ";";

  public NodeHealthCheckerService(
      LocalDirsHandlerService dirHandlerService) {
    super(NodeHealthCheckerService.class.getName());

    this.reporters = new ArrayList<>();
    this.dirsHandler = dirHandlerService;
    this.exceptionReporter = new ExceptionReporter();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    reporters.add(exceptionReporter);
    addHealthReporter(dirsHandler);
    String[] configuredScripts = conf.getTrimmedStrings(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPTS,
        YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_SCRIPTS);
    if (configuredScripts.length > MAX_SCRIPTS) {
      throw new IllegalArgumentException("Due to performance reasons " +
          "running more than " + MAX_SCRIPTS + "scripts is not allowed.");
    }
    for (String configuredScript : configuredScripts) {
      addHealthReporter(NodeHealthScriptRunner.newInstance(
          configuredScript, conf));
    }
    super.serviceInit(conf);
  }

  /**
   * Adds a {@link Service} implementing the {@link HealthReporter} interface,
   * if that service has not been added to this {@link CompositeService} yet.
   *
   * @param service to add
   * @throws Exception if not a {@link HealthReporter}
   *         implementation is provided to this function
   */
  @VisibleForTesting
  void addHealthReporter(Service service) throws Exception {
    if (service != null) {
      if (getServices().stream()
          .noneMatch(x -> x.getName().equals(service.getName()))) {
        if (!(service instanceof HealthReporter)) {
          throw new Exception("Attempted to add service to " +
              "NodeHealthCheckerService that does not implement " +
              "HealthReporter.");
        }
        reporters.add((HealthReporter) service);
        addService(service);
      } else {
        LOG.debug("Omitting duplicate service: {}.", service.getName());
      }
    }
  }

  /**
   * Joining the health reports of the dependent services.
   *
   * @return the report string about the health of the node
   */
  @Override
  public String getHealthReport() {
    ArrayList<String> reports = reporters.stream()
        .map(reporter -> Strings.emptyToNull(reporter.getHealthReport()))
        .collect(Collectors.toCollection(ArrayList::new));
    return Joiner.on(SEPARATOR).skipNulls().join(reports);
  }

  /**
   * @return <em>true</em> if the node is healthy
   */
  @Override
  public boolean isHealthy() {
    return reporters.stream().allMatch(HealthReporter::isHealthy);
  }

  /**
   * @return when the last time the node health status is reported
   */
  @Override
  public long getLastHealthReportTime() {
    Optional<Long> max = reporters.stream()
        .map(HealthReporter::getLastHealthReportTime).max(Long::compareTo);
    return max.orElse(0L);
  }

  /**
   * @return the disk handler
   */
  public LocalDirsHandlerService getDiskHandler() {
    return dirsHandler;
  }

  /**
   * Propagating an exception to {@link ExceptionReporter}.
   * @param exception the exception to propagate
   */
  public void reportException(Exception exception) {
    exceptionReporter.reportException(exception);
  }
}
