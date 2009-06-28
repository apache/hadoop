/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.transactional;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionLocation;

/**
 * Holds client-side transaction information. Client's use them as opaque
 * objects passed around to transaction operations.
 * 
 */
public class TransactionState {
  static final Log LOG = LogFactory.getLog(TransactionState.class);

  private final long transactionId;

  private Set<HRegionLocation> participatingRegions = new HashSet<HRegionLocation>();

  TransactionState(final long transactionId) {
    this.transactionId = transactionId;
  }

  boolean addRegion(final HRegionLocation hregion) {
    boolean added = participatingRegions.add(hregion);

    if (added) {
      LOG.debug("Adding new hregion ["
          + hregion.getRegionInfo().getRegionNameAsString()
          + "] to transaction [" + transactionId + "]");
    }

    return added;
  }

  Set<HRegionLocation> getParticipatingRegions() {
    return participatingRegions;
  }

  /**
   * Get the transactionId.
   * 
   * @return Return the transactionId.
   */
  public long getTransactionId() {
    return transactionId;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "id: " + transactionId + ", particpants: "
        + participatingRegions.size();
  }
}
