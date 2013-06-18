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

import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.util.Records;

/**
 * Description of resources requested back by the <code>ResourceManager</code>.
 * The <code>ApplicationMaster</code> (AM) can satisfy this request according
 * to its own priorities to prevent containers from being forcibly killed by
 * the platform.
 * @see PreemptionMessage
 */
@Public
@Evolving
public abstract class PreemptionContract {

  @Private
  @Unstable
  public static PreemptionContract newInstance(
      List<PreemptionResourceRequest> req, Set<PreemptionContainer> containers) {
    PreemptionContract contract = Records.newRecord(PreemptionContract.class);
    contract.setResourceRequest(req);
    contract.setContainers(containers);
    return contract;
  }

  /**
   * If the AM releases resources matching these requests, then the {@link
   * PreemptionContainer}s enumerated in {@link #getContainers()} should not be
   * evicted from the cluster. Due to delays in propagating cluster state and
   * sending these messages, there are conditions where satisfied contracts may
   * not prevent the platform from killing containers.
   * @return List of {@link PreemptionResourceRequest} to update the
   * <code>ApplicationMaster</code> about resources requested back by the
   * <code>ResourceManager</code>.
   * @see AllocateRequest#setAskList(List)
   */
  @Public
  @Evolving
  public abstract List<PreemptionResourceRequest> getResourceRequest();

  @Private
  @Unstable
  public abstract void setResourceRequest(List<PreemptionResourceRequest> req);

  /**
   * Assign the set of {@link PreemptionContainer} specifying which containers
   * owned by the <code>ApplicationMaster</code> that may be reclaimed by the
   * <code>ResourceManager</code>. If the AM prefers a different set of
   * containers, then it may checkpoint or kill containers matching the
   * description in {@link #getResourceRequest}.
   * @return Set of containers at risk if the contract is not met.
   */
  @Public
  @Evolving
  public abstract Set<PreemptionContainer> getContainers();


  @Private
  @Unstable
  public abstract void setContainers(Set<PreemptionContainer> containers);

}
