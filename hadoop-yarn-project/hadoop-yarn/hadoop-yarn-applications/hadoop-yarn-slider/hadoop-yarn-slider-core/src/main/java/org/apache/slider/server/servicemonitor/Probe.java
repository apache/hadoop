/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.server.servicemonitor;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Base class of all probes.
 */
public abstract class Probe implements MonitorKeys {

  protected final Configuration conf;
  private String name;

  // =======================================================
  /*
   * These fields are all used by the probe loops
   * to maintain state. Please Leave them alone.
   */
  public int successCount;
  public int failureCount;
  public long bootstrapStarted;
  public long bootstrapFinished;
  private boolean booted = false;

  // =======================================================

  /**
   * Create a probe of a specific name
   *
   * @param name probe name
   * @param conf configuration being stored.
   */
  public Probe(String name, Configuration conf) {
    this.name = name;
    this.conf = conf;
  }


  protected void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }


  @Override
  public String toString() {
    return getName() +
           " {" +
           "successCount=" + successCount +
           ", failureCount=" + failureCount +
           '}';
  }

  /**
   * perform any prelaunch initialization
   */
  public void init() throws IOException {

  }

  /**
   * Ping the endpoint. All exceptions must be caught and included in the
   * (failure) status.
   *
   * @param livePing is the ping live: true for live; false for boot time
   * @return the status
   */
  public abstract ProbeStatus ping(boolean livePing);

  public void beginBootstrap() {
    bootstrapStarted = System.currentTimeMillis();
  }

  public void endBootstrap() {
    setBooted(true);
    bootstrapFinished = System.currentTimeMillis();
  }

  public boolean isBooted() {
    return booted;
  }

  public void setBooted(boolean booted) {
    this.booted = booted;
  }
}
