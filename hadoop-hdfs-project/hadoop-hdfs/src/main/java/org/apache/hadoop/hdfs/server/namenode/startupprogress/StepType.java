/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
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
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Indicates a particular type of {@link Step}.
 */
@InterfaceAudience.Private
public enum StepType {
  /**
   * The namenode has entered safemode and is awaiting block reports from
   * datanodes.
   */
  AWAITING_REPORTED_BLOCKS("AwaitingReportedBlocks", "awaiting reported blocks"),

  /**
   * The namenode is performing an operation related to delegation keys.
   */
  DELEGATION_KEYS("DelegationKeys", "delegation keys"),

  /**
   * The namenode is performing an operation related to delegation tokens.
   */
  DELEGATION_TOKENS("DelegationTokens", "delegation tokens"),

  /**
   * The namenode is performing an operation related to inodes.
   */
  INODES("Inodes", "inodes"),

  /**
   * The namenode is performing an operation related to cache pools.
   */
  CACHE_POOLS("CachePools", "cache pools"),

  /**
   * The namenode is performing an operation related to cache entries.
   */
  CACHE_ENTRIES("CacheEntries", "cache entries"),

  /**
   * The namenode is performing an operation related to erasure coding policies.
   */
  ERASURE_CODING_POLICIES("ErasureCodingPolicies", "erasure coding policies");

  private final String name, description;

  /**
   * Private constructor of enum.
   * 
   * @param name String step type name
   * @param description String step type description
   */
  private StepType(String name, String description) {
    this.name = name;
    this.description = description;
  }

  /**
   * Returns step type description.
   * 
   * @return String step type description
   */
  public String getDescription() {
    return description;
  }

  /**
   * Returns step type name.
   * 
   * @return String step type name
   */
  public String getName() {
    return name;
  }
}
