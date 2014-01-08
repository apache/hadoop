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
import org.apache.hadoop.conf.Configurable;

/**
 * A fencing method is a method by which one node can forcibly prevent
 * another node from making continued progress. This might be implemented
 * by killing a process on the other node, by denying the other node's
 * access to shared storage, or by accessing a PDU to cut the other node's
 * power.
 * <p>
 * Since these methods are often vendor- or device-specific, operators
 * may implement this interface in order to achieve fencing.
 * <p>
 * Fencing is configured by the operator as an ordered list of methods to
 * attempt. Each method will be tried in turn, and the next in the list
 * will only be attempted if the previous one fails. See {@link NodeFencer}
 * for more information.
 * <p>
 * If an implementation also implements {@link Configurable} then its
 * <code>setConf</code> method will be called upon instantiation.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface FenceMethod {
  /**
   * Verify that the given fencing method's arguments are valid.
   * @param args the arguments provided in the configuration. This may
   *        be null if the operator did not configure any arguments.
   * @throws BadFencingConfigurationException if the arguments are invalid
   */
  public void checkArgs(String args) throws BadFencingConfigurationException;
  
  /**
   * Attempt to fence the target node.
   * @param target the target of the service to fence
   * @param args the configured arguments, which were checked at startup by
   *             {@link #checkArgs(String)}
   * @return true if fencing was successful, false if unsuccessful or
   *              indeterminate
   * @throws BadFencingConfigurationException if the configuration was
   *         determined to be invalid only at runtime
   */
  public boolean tryFence(HAServiceTarget target, String args)
    throws BadFencingConfigurationException;
}
