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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>QueueUserACLInfo</code> provides information {@link QueueACL} for
 * the given user.</p>
 * 
 * @see QueueACL
 * @see ApplicationClientProtocol#getQueueUserAcls(org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest)
 */
@Public
@Stable
public abstract class QueueUserACLInfo {

  @Private
  @Unstable
  public static QueueUserACLInfo newInstance(String queueName,
      List<QueueACL> acls) {
    QueueUserACLInfo info = Records.newRecord(QueueUserACLInfo.class);
    info.setQueueName(queueName);
    info.setUserAcls(acls);
    return info;
  }

  /**
   * Get the <em>queue name</em> of the queue.
   * @return <em>queue name</em> of the queue
   */
  @Public
  @Stable
  public abstract String getQueueName();
  
  @Private
  @Unstable
  public abstract void setQueueName(String queueName);

  /**
   * Get the list of <code>QueueACL</code> for the given user.
   * @return list of <code>QueueACL</code> for the given user
   */
  @Public
  @Stable
  public abstract List<QueueACL> getUserAcls();

  @Private
  @Unstable
  public abstract void setUserAcls(List<QueueACL> acls);
}
