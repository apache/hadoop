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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Factory class for creating instances of {@link PlacementRule}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class PlacementFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementFactory.class);

  private PlacementFactory() {
    // Unused.
  }

  /**
   * Create a new {@link PlacementRule} based on the rule class from the
   * configuration. This is used to instantiate rules by the scheduler which
   * does not resolve the class before this call.
   * @param ruleStr The name of the class to instantiate
   * @param conf The configuration object to set for the rule
   * @return Created class instance
   * @throws ClassNotFoundException
   * no definition for the class with the specified name could be found.
   */
  public static PlacementRule getPlacementRule(String ruleStr,
      Configuration conf)
      throws ClassNotFoundException {
    Class<? extends PlacementRule> ruleClass = Class.forName(ruleStr)
        .asSubclass(PlacementRule.class);
    LOG.info("Using PlacementRule implementation - " + ruleClass);
    return ReflectionUtils.newInstance(ruleClass, conf);
  }

  /**
   * Create a new {@link PlacementRule} based on the rule class from the
   * configuration. This is used to instantiate rules by the scheduler which
   * resolve the class before this call.
   * @param ruleClass The specific class reference to instantiate
   * @param initArg The config to set
   * @return Created class instance
   */
  public static PlacementRule getPlacementRule(
      Class<? extends PlacementRule> ruleClass, Object initArg) {
    LOG.info("Creating PlacementRule implementation: " + ruleClass);
    PlacementRule rule = ReflectionUtils.newInstance(ruleClass, null);
    rule.setConfig(initArg);
    return rule;
  }
}