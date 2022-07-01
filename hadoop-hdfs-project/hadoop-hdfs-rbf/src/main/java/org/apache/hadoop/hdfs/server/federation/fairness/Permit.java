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

import org.apache.hadoop.classification.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;

/**
 * A permit object contains PermitType and PermitVersion.
 */
public class Permit {
  private static final Permit DONTNEEDPERMIT =
      new Permit(PermitType.DONT_NEED_PERMIT, 0);
  private static final Permit NOPERMIT =
      new Permit(PermitType.NO_PERMIT, 0);

  public static Permit getDontNeedPermit() {
    return DONTNEEDPERMIT;
  }

  public static Permit getNoPermit() {
    return NOPERMIT;
  }

  private final PermitType permitType;
  private final int permitVersion;

  public Permit(PermitType permitType, int version) {
    this.permitType = permitType;
    this.permitVersion = version;
  }

  PermitType getPermitType() {
    return permitType;
  }

  int getPermitVersion() {
    return permitVersion;
  }

  public boolean isNoPermit() {
    return this.permitType.equals(PermitType.NO_PERMIT);
  }

  @VisibleForTesting
  public boolean isHoldPermit() {
    return !isNoPermit();
  }

  public boolean isDontNeedPermit() {
    return this.permitType.equals(PermitType.DONT_NEED_PERMIT);
  }

  @Override
  public String toString() {
    return "Permit[permitType=" + permitType
        + ",version=" + permitVersion + "]";
  }

  public enum PermitType {
    /** permit which is dedicated by namespace. */
    DEDICATED,
    /** permit which is shared by all namespaces. */
    SHARED,
    /** mock permit while no need to get permit. */
    DONT_NEED_PERMIT,
    /** a mock permit while can't get permits. */
    NO_PERMIT
  }

  /**
   * Build Permit instance of each PermitType based on the version.
   */
  public static Map<PermitType, Permit> getPermitBaseOnVersion(int version) {
    Map<PermitType, Permit> permitMap = new HashMap<>();
    for (PermitType permitType : PermitType.values()) {
      permitMap.put(permitType, new Permit(permitType, version));
    }
    return permitMap;
  }
}