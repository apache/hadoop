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
package org.apache.hadoop.yarn.api.records;

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * Enumeration of particular allocations to be reclaimed. The platform will
 * reclaim exactly these resources, so the <code>ApplicationMaster</code> (AM)
 * may attempt to checkpoint work or adjust its execution plan to accommodate
 * it. In contrast to {@link PreemptionContract}, the AM has no flexibility in
 * selecting which resources to return to the cluster.
 * @see PreemptionMessage
 */
@Public
@Evolving
public abstract class StrictPreemptionContract {

  @Private
  @Unstable
  public static StrictPreemptionContract newInstance(Set<PreemptionContainer> containers) {
    StrictPreemptionContract contract =
        Records.newRecord(StrictPreemptionContract.class);
    contract.setContainers(containers);
    return contract;
  }

  /**
   * Get the set of {@link PreemptionContainer} specifying containers owned by
   * the <code>ApplicationMaster</code> that may be reclaimed by the
   * <code>ResourceManager</code>.
   * @return the set of {@link ContainerId} to be preempted.
   */
  @Public
  @Evolving
  public abstract Set<PreemptionContainer> getContainers();

  @Private
  @Unstable
  public abstract void setContainers(Set<PreemptionContainer> containers);

}
