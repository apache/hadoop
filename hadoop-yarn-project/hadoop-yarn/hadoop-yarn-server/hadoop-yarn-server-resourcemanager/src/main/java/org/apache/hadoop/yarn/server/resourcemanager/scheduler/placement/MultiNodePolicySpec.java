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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

/**
 * MultiNodePolicySpec contains policyName and timeout.
 */
public class MultiNodePolicySpec {

  private String policyClassName;
  private long sortingInterval;

  public MultiNodePolicySpec(String policyClassName, long timeout) {
    this.setSortingInterval(timeout);
    this.setPolicyClassName(policyClassName);
  }

  public long getSortingInterval() {
    return sortingInterval;
  }

  public void setSortingInterval(long timeout) {
    this.sortingInterval = timeout;
  }

  public String getPolicyClassName() {
    return policyClassName;
  }

  public void setPolicyClassName(String policyClassName) {
    this.policyClassName = policyClassName;
  }

  @Override
  public String toString() {
    return "MultiNodePolicySpec {" +
        "policyClassName='" + policyClassName + '\'' +
        ", sortingInterval=" + sortingInterval +
        '}';
  }
}
