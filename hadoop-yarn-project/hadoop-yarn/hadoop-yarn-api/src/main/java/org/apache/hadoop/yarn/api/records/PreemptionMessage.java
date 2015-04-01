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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * A {@link PreemptionMessage} is part of the RM-AM protocol, and it is used by
 * the RM to specify resources that the RM wants to reclaim from this
 * {@code ApplicationMaster} (AM). The AM receives a {@link
 * StrictPreemptionContract} message encoding which containers the platform may
 * forcibly kill, granting it an opportunity to checkpoint state or adjust its
 * execution plan. The message may also include a {@link PreemptionContract}
 * granting the AM more latitude in selecting which resources to return to the
 * cluster.
 * <p>
 * The AM should decode both parts of the message. The {@link
 * StrictPreemptionContract} specifies particular allocations that the RM
 * requires back. The AM can checkpoint containers' state, adjust its execution
 * plan to move the computation, or take no action and hope that conditions that
 * caused the RM to ask for the container will change.
 * <p>
 * In contrast, the {@link PreemptionContract} also includes a description of
 * resources with a set of containers. If the AM releases containers matching
 * that profile, then the containers enumerated in {@link
 * PreemptionContract#getContainers()} may not be killed.
 * <p>
 * Each preemption message reflects the RM's current understanding of the
 * cluster state, so a request to return <em>N</em> containers may not
 * reflect containers the AM is releasing, recently exited containers the RM has
 * yet to learn about, or new containers allocated before the message was
 * generated. Conversely, an RM may request a different profile of containers in
 * subsequent requests.
 * <p>
 * The policy enforced by the RM is part of the scheduler. Generally, only
 * containers that have been requested consistently should be killed, but the
 * details are not specified.
 */
@Public
@Evolving
public abstract class PreemptionMessage {

  @Private
  @Unstable
  public static PreemptionMessage newInstance(StrictPreemptionContract set,
      PreemptionContract contract) {
    PreemptionMessage message = Records.newRecord(PreemptionMessage.class);
    message.setStrictContract(set);
    message.setContract(contract);
    return message;
  }

  /**
   * @return Specific resources that may be killed by the
   * <code>ResourceManager</code>
   */
  @Public
  @Evolving
  public abstract StrictPreemptionContract getStrictContract();

  @Private
  @Unstable
  public abstract void setStrictContract(StrictPreemptionContract set);

  /**
   * @return Contract describing resources to return to the cluster.
   */
  @Public
  @Evolving
  public abstract PreemptionContract getContract();

  @Private
  @Unstable
  public abstract void setContract(PreemptionContract contract);

}
