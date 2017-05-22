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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Factory class for creating instances of
 * {@link ConfigurationMutationACLPolicy}.
 */
public final class ConfigurationMutationACLPolicyFactory {

  private static final Log LOG = LogFactory.getLog(
      ConfigurationMutationACLPolicyFactory.class);

  private ConfigurationMutationACLPolicyFactory() {
    // Unused.
  }

  public static ConfigurationMutationACLPolicy getPolicy(Configuration conf) {
    Class<? extends ConfigurationMutationACLPolicy> policyClass =
        conf.getClass(YarnConfiguration.RM_SCHEDULER_MUTATION_ACL_POLICY_CLASS,
            DefaultConfigurationMutationACLPolicy.class,
            ConfigurationMutationACLPolicy.class);
    LOG.info("Using ConfigurationMutationACLPolicy implementation - " +
        policyClass);
    return ReflectionUtils.newInstance(policyClass, conf);
  }
}