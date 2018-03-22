/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * State of a <code>SubCluster</code>.
 * </p>
 */
@Private
@Unstable
public enum SubClusterState {
  /** Newly registered subcluster, before the first heartbeat. */
  SC_NEW,

  /** Subcluster is registered and the RM sent a heartbeat recently. */
  SC_RUNNING,

  /** Subcluster is unhealthy. */
  SC_UNHEALTHY,

  /** Subcluster is in the process of being out of service. */
  SC_DECOMMISSIONING,

  /** Subcluster is out of service. */
  SC_DECOMMISSIONED,

  /** RM has not sent a heartbeat for some configured time threshold. */
  SC_LOST,

  /** Subcluster has unregistered. */
  SC_UNREGISTERED;

  public boolean isUnusable() {
    return (this != SC_RUNNING && this != SC_NEW);
  }

  public boolean isActive() {
    return this == SC_RUNNING;
  }

  public boolean isFinal() {
    return (this == SC_UNREGISTERED || this == SC_DECOMMISSIONED
        || this == SC_LOST);
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(SubClusterState.class);

  /**
   * Convert a string into {@code SubClusterState}.
   *
   * @param x the string to convert in SubClusterState
   * @return the respective {@code SubClusterState}
   */
  public static SubClusterState fromString(String x) {
    try {
      return SubClusterState.valueOf(x);
    } catch (Exception e) {
      LOG.error("Invalid SubCluster State value in the StateStore does not"
          + " match with the YARN Federation standard.");
      return null;
    }
  }
}
