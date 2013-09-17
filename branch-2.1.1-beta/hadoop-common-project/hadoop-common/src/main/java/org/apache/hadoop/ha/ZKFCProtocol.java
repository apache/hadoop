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
package org.apache.hadoop.ha;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.KerberosInfo;

import java.io.IOException;

/**
 * Protocol exposed by the ZKFailoverController, allowing for graceful
 * failover.
 */
@KerberosInfo(
    serverPrincipal=CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY)
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ZKFCProtocol {
  /**
   * Initial version of the protocol
   */
  public static final long versionID = 1L;

  /**
   * Request that this service yield from the active node election for the
   * specified time period.
   * 
   * If the node is not currently active, it simply prevents any attempts
   * to become active for the specified time period. Otherwise, it first
   * tries to transition the local service to standby state, and then quits
   * the election.
   * 
   * If the attempt to transition to standby succeeds, then the ZKFC receiving
   * this RPC will delete its own breadcrumb node in ZooKeeper. Thus, the
   * next node to become active will not run any fencing process. Otherwise,
   * the breadcrumb will be left, such that the next active will fence this
   * node.
   * 
   * After the specified time period elapses, the node will attempt to re-join
   * the election, provided that its service is healthy.
   * 
   * If the node has previously been instructed to cede active, and is still
   * within the specified time period, the later command's time period will
   * take precedence, resetting the timer.
   * 
   * A call to cedeActive which specifies a 0 or negative time period will
   * allow the target node to immediately rejoin the election, so long as
   * it is healthy.
   *  
   * @param millisToCede period for which the node should not attempt to
   * become active
   * @throws IOException if the operation fails
   * @throws AccessControlException if the operation is disallowed
   */
  @Idempotent
  public void cedeActive(int millisToCede)
      throws IOException, AccessControlException;
  
  /**
   * Request that this node try to become active through a graceful failover.
   * 
   * If the node is already active, this is a no-op and simply returns success
   * without taking any further action.
   * 
   * If the node is not healthy, it will throw an exception indicating that it
   * is not able to become active.
   * 
   * If the node is healthy and not active, it will try to initiate a graceful
   * failover to become active, returning only when it has successfully become
   * active. See {@link ZKFailoverController#gracefulFailoverToYou()} for the
   * implementation details.
   * 
   * If the node fails to successfully coordinate the failover, throws an
   * exception indicating the reason for failure.
   * 
   * @throws IOException if graceful failover fails
   * @throws AccessControlException if the operation is disallowed
   */
  @Idempotent
  public void gracefulFailover()
      throws IOException, AccessControlException;
}
