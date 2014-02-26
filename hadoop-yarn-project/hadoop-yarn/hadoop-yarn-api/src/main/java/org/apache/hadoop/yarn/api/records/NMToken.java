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
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The NMToken is used for authenticating communication with
 * <code>NodeManager</code></p>
 * <p>It is issued by <code>ResourceMananger</code> when <code>ApplicationMaster</code>
 * negotiates resource with <code>ResourceManager</code> and
 * validated on <code>NodeManager</code> side.</p>
 * @see  AllocateResponse#getNMTokens()
 */
@Public
@Stable
public abstract class NMToken {

  @Private
  @Unstable
  public static NMToken newInstance(NodeId nodeId, Token token) {
    NMToken nmToken = Records.newRecord(NMToken.class);
    nmToken.setNodeId(nodeId);
    nmToken.setToken(token);
    return nmToken;
  }

  /**
   * Get the {@link NodeId} of the <code>NodeManager</code> for which the NMToken
   * is used to authenticate.
   * @return the {@link NodeId} of the <code>NodeManager</code> for which the
   * NMToken is used to authenticate.
   */
  @Public
  @Stable
  public abstract NodeId getNodeId();
  
  @Public
  @Stable
  public abstract void setNodeId(NodeId nodeId);

  /**
   * Get the {@link Token} used for authenticating with <code>NodeManager</code>
   * @return the {@link Token} used for authenticating with <code>NodeManager</code>
   */
  @Public
  @Stable
  public abstract Token getToken();
  
  @Public
  @Stable
  public abstract void setToken(Token token);


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result =
        prime * result + ((getNodeId() == null) ? 0 : getNodeId().hashCode());
    result =
        prime * result + ((getToken() == null) ? 0 : getToken().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    NMToken other = (NMToken) obj;
    if (getNodeId() == null) {
      if (other.getNodeId() != null)
        return false;
    } else if (!getNodeId().equals(other.getNodeId()))
      return false;
    if (getToken() == null) {
      if (other.getToken() != null)
        return false;
    } else if (!getToken().equals(other.getToken()))
      return false;
    return true;
  }
}
