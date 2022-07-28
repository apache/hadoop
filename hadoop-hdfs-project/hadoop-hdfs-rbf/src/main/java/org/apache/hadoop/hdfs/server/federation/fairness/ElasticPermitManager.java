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
 * A class that manages permits in an exclusive and shared mode.
 */
public class ElasticPermitManager implements AbstractPermitManager {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticPermitManager.class);
  private final String nsId;
  /** Dedicated Permit Numbers. **/
  private final int dedicatedCap;
  private final Semaphore dedicatedPermits;
  /** The maximum number of elastic permits that can be acquired. **/
  private final int maximumELNumberCanUse;
  private final Semaphore maximumELPermitsCanUse;
  /** Total elastic permits. **/
  private final Semaphore totalElasticPermits;
  /** A version to ensure that permits can be correctly recycled,
   * after dynamically refreshing FairnessPolicyController. **/
  private final int version;
  /** A cached permits Map. **/
  private final Map<PermitType, Permit> cachedPermits;

  ElasticPermitManager(String nsId, int dedicatedCap,
      int maximumELNumberCanUse, Semaphore totalElasticPermits, int version) {
    this.nsId = nsId;
    this.dedicatedCap = dedicatedCap;
    this.dedicatedPermits = new Semaphore(dedicatedCap);
    this.maximumELNumberCanUse = maximumELNumberCanUse;
    this.maximumELPermitsCanUse = new Semaphore(maximumELNumberCanUse);
    this.totalElasticPermits = totalElasticPermits;
    this.version = version;
    this.cachedPermits = Permit.getPermitBaseOnVersion(version);
    LOG.info("New NSPermitManager " + this);
  }

  @Override
  public String toString() {
    return "NSPermitManager[nsId=" + nsId
        + ", dedicatedPermitsNumber=" + dedicatedCap
        + ", sharedPermitsNumber=" + maximumELNumberCanUse
        + ", totalSharedPermits=" + totalElasticPermits
        + ", version=" + version + "]";
  }

  @Override
  public Permit acquirePermit() {
    Permit permit = this.cachedPermits.get(PermitType.NO_PERMIT);
    try {
      if (acquireDedicatedPermit()) {
        permit = this.cachedPermits.get(PermitType.DEDICATED);
      } else if (acquireElasticPermit()) {
        permit = this.cachedPermits.get(PermitType.SHARED);
      }
    } catch (InterruptedException e) {
      // ignore
    }
    return permit;
  }

  /**
   * Try to acquire one permit from Dedicated Semaphore.
   * Tips: without timeout to ensure lower latency
   */
  private boolean acquireDedicatedPermit() throws InterruptedException {
    return this.dedicatedPermits.tryAcquire();
  }

  /**
   * Try to acquire one permit from Total Elastic Permits.
   */
  private boolean acquireElasticPermit() throws InterruptedException {
    boolean result = false;
    if (maximumELNumberCanUse > 0 && totalElasticPermits != null) {
      if (this.maximumELPermitsCanUse.tryAcquire(1, TimeUnit.SECONDS)) {
        if (this.totalElasticPermits.tryAcquire(1, TimeUnit.SECONDS)) {
          result = true;
        } else {
          this.maximumELPermitsCanUse.release();
        }
      }
    }
    return result;
  }

  @Override
  public void releasePermit(Permit permit) {
    if (permit.getPermitVersion() == version) {
      switch (permit.getPermitType()) {
      case DEDICATED:
        this.dedicatedPermits.release();
        break;
      case SHARED:
        this.maximumELPermitsCanUse.release();
        this.totalElasticPermits.release();
        break;
      case DONT_NEED_PERMIT:
        break;
      default:
        LOG.warn("Unexpected permitType {}", permit.getPermitType());
        break;
      }
    } else {
      LOG.warn("Wrong permit version fro {}, expect {}, actual {}.",
          nsId, version, permit.getPermitVersion());
    }
  }

  @Override
  public void drainPermits() {
    this.dedicatedPermits.drainPermits();
  }

  @Override
  public int availablePermits() {
    return this.dedicatedPermits.availablePermits()
        + this.maximumELPermitsCanUse.availablePermits();
  }
}
