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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.ReflectionUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class PoolPlacementPolicy {
  private static final Map<String, Class<? extends PoolPlacementRule>> ruleClasses;
  static {
    Map<String, Class<? extends PoolPlacementRule>> map =
        new HashMap<String, Class<? extends PoolPlacementRule>>();
    map.put("user", PoolPlacementRule.User.class);
    map.put("primaryGroup", PoolPlacementRule.PrimaryGroup.class);
    map.put("specified", PoolPlacementRule.Specified.class);
    map.put("default", PoolPlacementRule.Default.class);
    map.put("reject", PoolPlacementRule.Reject.class);
    ruleClasses = Collections.unmodifiableMap(map);
  }
  
  private final List<PoolPlacementRule> rules;
  private final Set<String> configuredPools;
  private final Groups groups;
  
  public PoolPlacementPolicy(List<PoolPlacementRule> rules,
      Set<String> configuredPools, Configuration conf)
      throws AllocationConfigurationException {
    for (int i = 0; i < rules.size()-1; i++) {
      if (rules.get(i).isTerminal()) {
        throw new AllocationConfigurationException("Rules after rule "
            + i + " in pool placement policy can never be reached");
      }
    }
    if (!rules.get(rules.size()-1).isTerminal()) {
      throw new AllocationConfigurationException(
          "Could get past last pool placement rule without assigning");
    }
    this.rules = rules;
    this.configuredPools = configuredPools;
    groups = new Groups(conf);
  }
  
  /**
   * Builds a PoolPlacementPolicy from an xml element.
   */
  public static PoolPlacementPolicy fromXml(Element el, Set<String> configuredPools,
      Configuration conf) throws AllocationConfigurationException {
    List<PoolPlacementRule> rules = new ArrayList<PoolPlacementRule>();
    NodeList elements = el.getChildNodes();
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element) {
        Element element = (Element)node;
        String ruleName = element.getTagName();
        Class<? extends PoolPlacementRule> clazz = ruleClasses.get(ruleName);
        if (clazz == null) {
          throw new AllocationConfigurationException("No rule class found for "
              + ruleName);
        }
        PoolPlacementRule rule = ReflectionUtils.newInstance(clazz, null);
        rule.initializeFromXml(element);
        rules.add(rule);
      }
    }
    return new PoolPlacementPolicy(rules, configuredPools, conf);
  }
  
  /**
   * Applies this rule to an job with the given requested pool and user/group
   * information.
   * 
   * @param requestedPool
   *    The pool specified in the Context
   * @param user
   *    The user submitting the job
   * @return
   *    The name of the pool to assign the job to.  Or null is no pool was
   *    assigned
   * @throws IOException
   *    If an exception is encountered while getting the user's groups
   */
  public String assignJobToPool(String requestedPool, String user)
      throws IOException {
    for (PoolPlacementRule rule : rules) {
      String pool = rule.assignJobToPool(requestedPool, user, groups,
          configuredPools);

      if (pool == null || !pool.isEmpty()) {
        return pool;
      }
    }
    throw new IllegalStateException("Should have applied a rule before " +
    		"reaching here");
  }
}
