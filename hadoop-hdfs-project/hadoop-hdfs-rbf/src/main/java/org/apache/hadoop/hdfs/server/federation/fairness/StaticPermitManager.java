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
package org.apache.hadoop.hdfs.server.federation.fairness;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdfs.server.federation.fairness.Permit.PermitType;

/**
 * A class that manages permits in an exclusive mode.
 */
public class StaticPermitManager implements AbstractPermitManager {
  private final static Logger LOG = LoggerFactory.getLogger(StaticPermitManager.class);
  private final String nsId;
  private final int version;
  private final int permitCap;
  private final Semaphore dedicatedPermits;
  private final Map<PermitType, Permit> cachedPermits;

  public StaticPermitManager(
      final String nsId, final int version,
      final int permitCap) {
    this.nsId = nsId;
    this.version = version;
    this.permitCap = permitCap;
    this.dedicatedPermits = new Semaphore(permitCap);
    this.cachedPermits = Permit.getPermitBaseOnVersion(version);
    LOG.info("{} has {} dedicated permits.", nsId, permitCap);
  }

  @Override
  public String toString() {
    return "[StaticPermitManager, nsId=" + nsId
        + ", version=" + version
        + ", permitCap=" + permitCap
        + ", availablePermits=" + this.dedicatedPermits.availablePermits()
        + "]";
  }

  @Override
  public Permit acquirePermit() {
    Permit permit = Permit.getNoPermit();
    try {
      if (dedicatedPermits.tryAcquire(1, TimeUnit.SECONDS)) {
        permit = this.cachedPermits.get(PermitType.DEDICATED);
      }
    } catch (InterruptedException e) {
      // ignore
    }
    return permit;
  }

  @Override
  public void releasePermit(Permit permit) {
    if (permit.getPermitVersion() == version) {
      if (permit.getPermitType() == PermitType.DEDICATED) {
        this.dedicatedPermits.release();
      }
    } else {
      LOG.warn("Wrong permit version for {}, expect {}, actual {}",
          nsId, version, permit.getPermitVersion());
    }
  }

  @Override
  public void drainPermits() {
    this.dedicatedPermits.drainPermits();
  }

  @Override
  public int availablePermits() {
    return this.dedicatedPermits.availablePermits();
  }
}