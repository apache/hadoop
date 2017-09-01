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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * <p>The set of built-in erasure coding policies.</p>
 * <p>Although this is a private class, EC policy IDs need to be treated like a
 * stable interface. Adding, modifying, or removing built-in policies can cause
 * inconsistencies with older clients.</p>
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public final class SystemErasureCodingPolicies {

  // Private constructor, this is a utility class.
  private SystemErasureCodingPolicies() {}

  // 1 MB
  private static final int DEFAULT_CELLSIZE = 1024 * 1024;

  public static final byte RS_6_3_POLICY_ID = 1;
  private static final ErasureCodingPolicy SYS_POLICY1 =
      new ErasureCodingPolicy(ErasureCodeConstants.RS_6_3_SCHEMA,
          DEFAULT_CELLSIZE, RS_6_3_POLICY_ID);

  public static final byte RS_3_2_POLICY_ID = 2;
  private static final ErasureCodingPolicy SYS_POLICY2 =
      new ErasureCodingPolicy(ErasureCodeConstants.RS_3_2_SCHEMA,
          DEFAULT_CELLSIZE, RS_3_2_POLICY_ID);

  public static final byte RS_6_3_LEGACY_POLICY_ID = 3;
  private static final ErasureCodingPolicy SYS_POLICY3 =
      new ErasureCodingPolicy(ErasureCodeConstants.RS_6_3_LEGACY_SCHEMA,
          DEFAULT_CELLSIZE, RS_6_3_LEGACY_POLICY_ID);

  public static final byte XOR_2_1_POLICY_ID = 4;
  private static final ErasureCodingPolicy SYS_POLICY4 =
      new ErasureCodingPolicy(ErasureCodeConstants.XOR_2_1_SCHEMA,
          DEFAULT_CELLSIZE, XOR_2_1_POLICY_ID);

  public static final byte RS_10_4_POLICY_ID = 5;
  private static final ErasureCodingPolicy SYS_POLICY5 =
      new ErasureCodingPolicy(ErasureCodeConstants.RS_10_4_SCHEMA,
          DEFAULT_CELLSIZE, RS_10_4_POLICY_ID);

  // REPLICATION policy is always enabled.
  private static final ErasureCodingPolicy REPLICATION_POLICY =
      new ErasureCodingPolicy(ErasureCodeConstants.REPLICATION_POLICY_NAME,
          ErasureCodeConstants.REPLICATION_1_2_SCHEMA,
          DEFAULT_CELLSIZE,
          ErasureCodeConstants.REPLICATION_POLICY_ID);

  private static final List<ErasureCodingPolicy> SYS_POLICIES =
      Collections.unmodifiableList(Arrays.asList(
          SYS_POLICY1, SYS_POLICY2, SYS_POLICY3, SYS_POLICY4,
          SYS_POLICY5));

  /**
   * System policies sorted by name for fast querying.
   */
  private static final Map<String, ErasureCodingPolicy> SYSTEM_POLICIES_BY_NAME;

  /**
   * System policies sorted by ID for fast querying.
   */
  private static final Map<Byte, ErasureCodingPolicy> SYSTEM_POLICIES_BY_ID;

  /**
   * Populate the lookup maps in a static block.
   */
  static {
    SYSTEM_POLICIES_BY_NAME = new TreeMap<>();
    SYSTEM_POLICIES_BY_ID = new TreeMap<>();
    for (ErasureCodingPolicy policy : SYS_POLICIES) {
      SYSTEM_POLICIES_BY_NAME.put(policy.getName(), policy);
      SYSTEM_POLICIES_BY_ID.put(policy.getId(), policy);
    }
  }

  /**
   * Get system defined policies.
   * @return system policies
   */
  public static List<ErasureCodingPolicy> getPolicies() {
    return SYS_POLICIES;
  }

  /**
   * Get a policy by policy ID.
   * @return ecPolicy, or null if not found
   */
  public static ErasureCodingPolicy getByID(byte id) {
    return SYSTEM_POLICIES_BY_ID.get(id);
  }

  /**
   * Get a policy by policy name.
   * @return ecPolicy, or null if not found
   */
  public static ErasureCodingPolicy getByName(String name) {
    return SYSTEM_POLICIES_BY_NAME.get(name);
  }

  /**
   * Get the special REPLICATION policy.
   */
  public static ErasureCodingPolicy  getReplicationPolicy() {
    return REPLICATION_POLICY;
  }
}
