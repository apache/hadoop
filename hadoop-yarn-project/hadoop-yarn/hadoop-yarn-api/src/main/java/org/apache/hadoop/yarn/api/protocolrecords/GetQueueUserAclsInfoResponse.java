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

package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The response sent by the <code>ResourceManager</code> to clients
 * seeking queue acls for the user.</p>
 *
 * <p>The response contains a list of {@link QueueUserACLInfo} which
 * provides information about {@link QueueACL} per queue.</p>
 * 
 * @see QueueACL
 * @see QueueUserACLInfo
 * @see ApplicationClientProtocol#getQueueUserAcls(GetQueueUserAclsInfoRequest)
 */
@Public
@Stable
public abstract class GetQueueUserAclsInfoResponse {

  @Private
  @Unstable
  public static GetQueueUserAclsInfoResponse newInstance(
      List<QueueUserACLInfo> queueUserAclsList) {
    GetQueueUserAclsInfoResponse response =
        Records.newRecord(GetQueueUserAclsInfoResponse.class);
    response.setUserAclsInfoList(queueUserAclsList);
    return response;
  }

  /**
   * Get the <code>QueueUserACLInfo</code> per queue for the user.
   * @return <code>QueueUserACLInfo</code> per queue for the user
   */
  @Public
  @Stable
  public abstract List<QueueUserACLInfo> getUserAclsInfoList();
  
  @Private
  @Unstable
  public abstract void setUserAclsInfoList(
      List<QueueUserACLInfo> queueUserAclsList);
  
}
