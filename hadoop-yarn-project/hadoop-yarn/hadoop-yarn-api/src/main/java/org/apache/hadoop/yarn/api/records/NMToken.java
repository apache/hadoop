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
import org.apache.hadoop.yarn.util.Records;

/**
 * NMToken is returned by RM on AllocateResponse.
 */
public abstract class NMToken {

  @Public
  @Stable
  public abstract NodeId getNodeId();
  
  @Public
  @Stable
  public abstract void setNodeId(NodeId nodeId);
  
  @Public
  @Stable
  public abstract Token getToken();
  
  @Public
  @Stable
  public abstract void setToken(Token token);
  
  @Private
  public static NMToken newInstance(NodeId nodeId, Token token) {
    NMToken nmToken = Records.newRecord(NMToken.class);
    nmToken.setNodeId(nodeId);
    nmToken.setToken(token);
    return nmToken;
  }
}
