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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.metrics.RBFMetrics;
import org.apache.hadoop.hdfs.server.federation.metrics.NamenodeBeanMetrics;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.service.AbstractService;

/**
 * Service to manage the metrics of the Router.
 */
public class RouterMetricsService extends AbstractService {

  /** Router for this metrics. */
  private final Router router;

  /** Router metrics. */
  private RouterMetrics routerMetrics;
  /** Federation metrics. */
  private RBFMetrics rbfMetrics;
  /** Namenode mock metrics. */
  private NamenodeBeanMetrics nnMetrics;


  public RouterMetricsService(final Router router) {
    super(RouterMetricsService.class.getName());
    this.router = router;
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    this.routerMetrics = RouterMetrics.create(configuration);
  }

  @Override
  protected void serviceStart() throws Exception {
    // Wrapper for all the FSNamesystem JMX interfaces
    this.nnMetrics = new NamenodeBeanMetrics(this.router);

    // Federation MBean JMX interface
    this.rbfMetrics = new RBFMetrics(this.router);
  }

  @Override
  protected void serviceStop() throws Exception {
    // Remove JMX interfaces
    if (this.rbfMetrics != null) {
      this.rbfMetrics.close();
    }

    // Remove Namenode JMX interfaces
    if (this.nnMetrics != null) {
      this.nnMetrics.close();
    }

    // Shutdown metrics
    if (this.routerMetrics != null) {
      this.routerMetrics.shutdown();
    }
  }

  /**
   * Get the metrics system for the Router.
   *
   * @return Router metrics.
   */
  public RouterMetrics getRouterMetrics() {
    return this.routerMetrics;
  }

  /**
   * Get the federation metrics.
   *
   * @return Federation metrics.
   */
  public RBFMetrics getRBFMetrics() {
    return this.rbfMetrics;
  }

  /**
   * Get the Namenode metrics.
   *
   * @return Namenode metrics.
   */
  public NamenodeBeanMetrics getNamenodeMetrics() {
    return this.nnMetrics;
  }

  /**
   * Get the JVM metrics for the Router.
   *
   * @return JVM metrics.
   */
  public JvmMetrics getJvmMetrics() {
    if (this.routerMetrics == null) {
      return null;
    }
    return this.routerMetrics.getJvmMetrics();
  }
}
