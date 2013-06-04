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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request sent by clients to get <em>queue information</em>
 * from the <code>ResourceManager</code>.</p>
 *
 * @see ClientRMProtocol#getQueueInfo(GetQueueInfoRequest)
 */
@Public
@Stable
public abstract class GetQueueInfoRequest {

  public static GetQueueInfoRequest
      newInstance(String queueName, boolean includeApplications,
          boolean includeChildQueues, boolean recursive) {
    GetQueueInfoRequest request = Records.newRecord(GetQueueInfoRequest.class);
    request.setQueueName(queueName);
    request.setIncludeApplications(includeApplications);
    request.setIncludeChildQueues(includeChildQueues);
    request.setRecursive(recursive);
    return request;
  }

  /**
   * Get the <em>queue name</em> for which to get queue information.
   * @return <em>queue name</em> for which to get queue information
   */
  public abstract String getQueueName();
  
  /**
   * Set the <em>queue name</em> for which to get queue information
   * @param queueName <em>queue name</em> for which to get queue information
   */
  public abstract void setQueueName(String queueName);

  /**
   * Is information about <em>active applications<e/m> required?
   * @return <code>true</code> if applications' information is to be included,
   *         else <code>false</code>
   */
  public abstract boolean getIncludeApplications();

  /**
   * Should we get fetch information about <em>active applications</em>?
   * @param includeApplications fetch information about <em>active 
   *                            applications</em>?
   */
  public abstract void setIncludeApplications(boolean includeApplications);

  /**
   * Is information about <em>child queues</em> required?
   * @return <code>true</code> if information about child queues is required,
   *         else <code>false</code>
   */
  public abstract boolean getIncludeChildQueues();
  
  /**
   * Should we fetch information about <em>child queues</em>?
   * @param includeChildQueues fetch information about <em>child queues</em>?
   */
  public abstract void setIncludeChildQueues(boolean includeChildQueues);

  /**
   * Is information on the entire <em>child queue hierarchy</em> required?
   * @return <code>true</code> if information about entire hierarchy is 
   *         required, <code>false</code> otherwise
   */
  public abstract boolean getRecursive();
  
  /**
   * Should we fetch information on the entire <em>child queue hierarchy</em>?
   * @param recursive fetch information on the entire <em>child queue 
   *                  hierarchy</em>?
   */
  public abstract void setRecursive(boolean recursive);
}

