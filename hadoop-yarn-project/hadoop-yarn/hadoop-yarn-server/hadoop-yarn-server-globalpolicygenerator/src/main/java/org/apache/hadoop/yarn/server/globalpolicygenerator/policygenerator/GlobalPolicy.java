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
package org.apache.hadoop.yarn.server.globalpolicygenerator.policygenerator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

import java.util.Collections;
import java.util.Map;

/**
 * This interface defines the plug-able policy that the PolicyGenerator uses
 * to update policies into the state store.
 */

public abstract class GlobalPolicy implements Configurable {

  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Return a map of the object type and RM path to request it from - the
   * framework will query these paths and provide the objects to the policy.
   * Delegating this responsibility to the PolicyGenerator enables us to avoid
   * duplicate calls to the same * endpoints as the GlobalPolicy is invoked
   * once per queue.
   *
   * @return a map of the object type and RM path.
   */
  protected Map<Class<?>, String> registerPaths() {
    // Default register nothing
    return Collections.emptyMap();
  }

  /**
   * Given a queue, cluster metrics, and policy manager, update the policy
   * to account for the cluster status. This method defines the policy generator
   * behavior.
   *
   * @param queueName   name of the queue
   * @param clusterInfo subClusterId map to cluster information about the
   *                    SubCluster used to make policy decisions
   * @param manager     the FederationPolicyManager for the queue's existing
   *                    policy the manager may be null, in which case the policy
   *                    will need to be created
   * @return policy manager that handles the updated (or created) policy
   */
  protected abstract FederationPolicyManager updatePolicy(String queueName,
      Map<SubClusterId, Map<Class, Object>> clusterInfo,
      FederationPolicyManager manager);

}
