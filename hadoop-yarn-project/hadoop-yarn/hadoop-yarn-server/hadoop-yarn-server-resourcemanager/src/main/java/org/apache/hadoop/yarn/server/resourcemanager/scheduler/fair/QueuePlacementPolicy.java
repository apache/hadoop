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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

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

public class QueuePlacementPolicy {
  private static final Map<String, Class<? extends QueuePlacementRule>> ruleClasses;
  static {
    Map<String, Class<? extends QueuePlacementRule>> map =
        new HashMap<String, Class<? extends QueuePlacementRule>>();
    map.put("user", QueuePlacementRule.User.class);
    map.put("primaryGroup", QueuePlacementRule.PrimaryGroup.class);
    map.put("specified", QueuePlacementRule.Specified.class);
    map.put("default", QueuePlacementRule.Default.class);
    map.put("reject", QueuePlacementRule.Reject.class);
    ruleClasses = Collections.unmodifiableMap(map);
  }
  
  private final List<QueuePlacementRule> rules;
  private final Set<String> configuredQueues;
  private final Groups groups;
  
  public QueuePlacementPolicy(List<QueuePlacementRule> rules,
      Set<String> configuredQueues, Configuration conf)
      throws AllocationConfigurationException {
    for (int i = 0; i < rules.size()-1; i++) {
      if (rules.get(i).isTerminal()) {
        throw new AllocationConfigurationException("Rules after rule "
            + i + " in queue placement policy can never be reached");
      }
    }
    if (!rules.get(rules.size()-1).isTerminal()) {
      throw new AllocationConfigurationException(
          "Could get past last queue placement rule without assigning");
    }
    this.rules = rules;
    this.configuredQueues = configuredQueues;
    groups = new Groups(conf);
  }
  
  /**
   * Builds a QueuePlacementPolicy from an xml element.
   */
  public static QueuePlacementPolicy fromXml(Element el, Set<String> configuredQueues,
      Configuration conf) throws AllocationConfigurationException {
    List<QueuePlacementRule> rules = new ArrayList<QueuePlacementRule>();
    NodeList elements = el.getChildNodes();
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element) {
        Element element = (Element)node;
        String ruleName = element.getTagName();
        Class<? extends QueuePlacementRule> clazz = ruleClasses.get(ruleName);
        if (clazz == null) {
          throw new AllocationConfigurationException("No rule class found for "
              + ruleName);
        }
        QueuePlacementRule rule = ReflectionUtils.newInstance(clazz, null);
        rule.initializeFromXml(element);
        rules.add(rule);
      }
    }
    return new QueuePlacementPolicy(rules, configuredQueues, conf);
  }
  
  /**
   * Applies this rule to an app with the given requested queue and user/group
   * information.
   * 
   * @param requestedQueue
   *    The queue specified in the ApplicationSubmissionContext
   * @param user
   *    The user submitting the app
   * @return
   *    The name of the queue to assign the app to.  Or null if the app should
   *    be rejected.
   * @throws IOException
   *    If an exception is encountered while getting the user's groups
   */
  public String assignAppToQueue(String requestedQueue, String user)
      throws IOException {
    for (QueuePlacementRule rule : rules) {
      String queue = rule.assignAppToQueue(requestedQueue, user, groups,
          configuredQueues);
      if (queue == null || !queue.isEmpty()) {
        return queue;
      }
    }
    throw new IllegalStateException("Should have applied a rule before " +
    		"reaching here");
  }
}
