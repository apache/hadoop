/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.report;

import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.container.common.statemachine
    .DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class responsible for scheduling the reports based on the
 * configured interval. All the ReportPublishers should extend this class.
 */
public abstract class ReportPublisher<T extends GeneratedMessage>
    implements Configurable, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(
      ReportPublisher.class);

  private Configuration config;
  private StateContext context;
  private ScheduledExecutorService executor;

  /**
   * Initializes ReportPublisher with stateContext and executorService.
   *
   * @param stateContext Datanode state context
   * @param executorService ScheduledExecutorService to schedule reports
   */
  public void init(StateContext stateContext,
                   ScheduledExecutorService executorService) {
    this.context = stateContext;
    this.executor = executorService;
    this.executor.schedule(this,
        getReportFrequency(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void run() {
    publishReport();
    if (!executor.isShutdown() ||
        !(context.getState() == DatanodeStates.SHUTDOWN)) {
      executor.schedule(this,
          getReportFrequency(), TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Generates and publishes the report to datanode state context.
   */
  private void publishReport() {
    try {
      context.addReport(getReport());
    } catch (IOException e) {
      LOG.error("Exception while publishing report.", e);
    }
  }

  /**
   * Returns the frequency in which this particular report has to be scheduled.
   *
   * @return report interval in milliseconds
   */
  protected abstract long getReportFrequency();

  /**
   * Generate and returns the report which has to be sent as part of heartbeat.
   *
   * @return datanode report
   */
  protected abstract T getReport() throws IOException;

  /**
   * Returns {@link StateContext}.
   *
   * @return stateContext report
   */
  protected StateContext getContext() {
    return context;
  }

}
