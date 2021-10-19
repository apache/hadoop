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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.placement.DefaultPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.FSPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PrimaryGroupPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.RejectPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.SecondaryGroupExistingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.SpecifiedPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserPlacementRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementFactory.getPlacementRule;

/**
 * The FairScheduler rules based policy for placing an application in a queue.
 * It parses the configuration and updates the {@link
 * org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager}
 * with a list of {@link PlacementRule}s to execute in order.
 */
@Private
@Unstable
final class QueuePlacementPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueuePlacementPolicy.class);

  // Simple private class to make the rule mapping simpler.
  private static final class RuleMap {
    private final Class<? extends PlacementRule> ruleClass;
    private final String terminal;

    private RuleMap(Class<? extends PlacementRule> clazz, String terminate) {
      this.ruleClass = clazz;
      this.terminal = terminate;
    }
  }

  // The list of known rules:
  // key to the map is the name in the configuration.
  // for each name the mapping contains the class name of the implementation
  // and a flag (true, false or create) which describes the terminal state
  // see the method getTerminal() for more comments.
  private static final Map<String, RuleMap> RULES;
  static {
    Map<String, RuleMap> map = new HashMap<>();
    map.put("user", new RuleMap(UserPlacementRule.class, "create"));
    map.put("primaryGroup",
        new RuleMap(PrimaryGroupPlacementRule.class, "create"));
    map.put("secondaryGroupExistingQueue",
        new RuleMap(SecondaryGroupExistingPlacementRule.class, "false"));
    map.put("specified", new RuleMap(SpecifiedPlacementRule.class, "false"));
    map.put("nestedUserQueue", new RuleMap(UserPlacementRule.class, "create"));
    map.put("default", new RuleMap(DefaultPlacementRule.class, "create"));
    map.put("reject", new RuleMap(RejectPlacementRule.class, "true"));
    RULES = Collections.unmodifiableMap(map);
  }

  private QueuePlacementPolicy() {
  }

  /**
   * Update the rules in the manager based on this placement policy.
   * @param newRules The new list of rules to set in the manager.
   * @param newTerminalState The list of terminal states for this set of rules.
   * @param fs the reference to the scheduler needed in the rule on init.
   * @throws AllocationConfigurationException for any errors
   */
  private static void updateRuleSet(List<PlacementRule> newRules,
                                    List<Boolean> newTerminalState,
                                    FairScheduler fs)
      throws AllocationConfigurationException {
    if (newRules.isEmpty()) {
      LOG.debug("Empty rule set defined, ignoring update");
      return;
    }
    LOG.debug("Placement rule order check");
    for (int i = 0; i < newTerminalState.size()-1; i++) {
      if (newTerminalState.get(i)) {
        String errorMsg = "Rules after rule "
            + (i+1) + " in queue placement policy can never be reached";
        if (fs.isNoTerminalRuleCheck()) {
          LOG.warn(errorMsg);
        } else {
          throw new AllocationConfigurationException(errorMsg);
        }
      }
    }
    if (!newTerminalState.get(newTerminalState.size()-1)) {
      throw new AllocationConfigurationException(
          "Could get past last queue placement rule without assigning");
    }
    // Set the scheduler in the rule to get queues etc
    LOG.debug("Initialising new rule set");
    try {
      for (PlacementRule rule: newRules){
        rule.initialize(fs);
      }
    } catch (IOException ioe) {
      // We should never throw as we pass in a FS object, however we still
      // should consider any exception here a config error.
      throw new AllocationConfigurationException(
          "Rule initialisation failed with exception", ioe);
    }
    // Update the placement manager with the new rule list.
    // We only get here when all rules are OK.
    fs.getRMContext().getQueuePlacementManager().updateRules(newRules);
    LOG.debug("PlacementManager active with new rule set");
  }

  /**
   * Builds a QueuePlacementPolicy from a xml element.
   * @param confElement the placement policy xml snippet from the
   *                    {@link FairSchedulerConfiguration}
   * @param fs the reference to the scheduler needed in the rule on init.
   * @throws AllocationConfigurationException for any errors
   */
  static void fromXml(Element confElement, FairScheduler fs)
      throws AllocationConfigurationException {
    LOG.debug("Reloading placement policy from allocation config");
    if (confElement == null || !confElement.hasChildNodes()) {
      throw new AllocationConfigurationException(
          "Empty configuration for QueuePlacementPolicy is not allowed");
    }
    List<PlacementRule> newRules = new ArrayList<>();
    List<Boolean> newTerminalState = new ArrayList<>();
    NodeList elements = confElement.getChildNodes();
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element &&
          node.getNodeName().equalsIgnoreCase("rule")) {
        String name = ((Element) node).getAttribute("name");
        LOG.debug("Creating new rule: {}", name);
        PlacementRule rule = createRule((Element)node);

        // The only child node that we currently know is a parent rule
        PlacementRule parentRule = null;
        String parentName = null;
        Element child = getParentRuleElement(node);
        if (child != null) {
          parentName = child.getAttribute("name");
          parentRule = getParentRule(child, fs);
        }
        // Need to make sure that the nestedUserQueue has a parent for
        // backwards compatibility
        if (name.equalsIgnoreCase("nestedUserQueue") && parentRule == null) {
          throw new AllocationConfigurationException("Rule '" + name
              + "' must have a parent rule set");
        }
        newRules.add(rule);
        if (parentRule == null) {
          newTerminalState.add(
              getTerminal(RULES.get(name).terminal, rule));
        } else {
          ((FSPlacementRule)rule).setParentRule(parentRule);
          newTerminalState.add(
              getTerminal(RULES.get(name).terminal, rule) &&
              getTerminal(RULES.get(parentName).terminal, parentRule));
        }
      }
    }
    updateRuleSet(newRules, newTerminalState, fs);
  }

  /**
   * Find the element that defines the parent rule.
   * @param node the xml node to check for a parent rule
   * @return {@link Element} that describes the parent rule or
   * <code>null</code> if none is found
   */
  private static Element getParentRuleElement(Node node)
      throws AllocationConfigurationException {
    Element parent = null;
    // walk over the node list
    if (node.hasChildNodes()) {
      NodeList childList = node.getChildNodes();
      for (int j = 0; j < childList.getLength(); j++) {
        Node child = childList.item(j);
        if (child instanceof Element &&
            child.getNodeName().equalsIgnoreCase("rule")) {
          if (parent != null) {
            LOG.warn("Rule '{}' has multiple parent rules defined, only the " +
                "last parent rule will be used",
                ((Element) node).getAttribute("name"));
          }
          parent = ((Element) child);
        }
      }
    }
    // sanity check the rule that is configured
    if (parent != null) {
      String parentName = parent.getAttribute("name");
      if (parentName.equals("reject") ||
          parentName.equals("nestedUserQueue")) {
        throw new AllocationConfigurationException("Rule '"
            + parentName
            + "' is not allowed as a parent rule for any rule");
      }
    }
    return parent;
  }

  /**
   * Retrieve the configured parent rule from the xml config.
   * @param parent the xml element that contains the name of the rule to add.
   * @param fs the reference to the scheduler needed in the rule on init.
   * @return {@link PlacementRule} to set as a parent
   * @throws AllocationConfigurationException for any error
   */
  private static PlacementRule getParentRule(Element parent,
                                             FairScheduler fs)
      throws AllocationConfigurationException {
    LOG.debug("Creating new parent rule: {}", parent.getAttribute("name"));
    PlacementRule parentRule = createRule(parent);
    // Init the rule, we do not want to add it to the list of the
    // placement manager
    try {
      parentRule.initialize(fs);
    } catch (IOException ioe) {
      // We should never throw as we pass in a FS object, however we
      // still should consider any exception here a config error.
      throw new AllocationConfigurationException(
          "Parent Rule initialisation failed with exception", ioe);
    }
    return parentRule;
  }

  /**
   * Returns the terminal status of the rule based on the definition and the
   * create flag set in the rule.
   * @param terminal The definition of the terminal flag
   * @param rule The rule to check
   * @return <code>true</code> if the rule is terminal <code>false</code> in
   * all other cases.
   */
  private static Boolean getTerminal(String terminal, PlacementRule rule) {
    switch (terminal) {
    case "true":    // rule is always terminal
      return true;
    case "false":   // rule is never terminal
      return false;
    default:        // rule is terminal based on the create flag
      return ((FSPlacementRule)rule).getCreateFlag();
    }
  }

  /**
   * Create a rule from a given a xml node.
   * @param element the xml element to create the rule from
   * @return PlacementRule
   * @throws AllocationConfigurationException for any error
   */
  @SuppressWarnings("unchecked")
  private static PlacementRule createRule(Element element)
      throws AllocationConfigurationException {

    String ruleName = element.getAttribute("name");
    if ("".equals(ruleName)) {
      throw new AllocationConfigurationException("No name provided for a "
          + "rule element");
    }

    Class<? extends PlacementRule> ruleClass = null;
    if (RULES.containsKey(ruleName)) {
      ruleClass = RULES.get(ruleName).ruleClass;
    }
    if (ruleClass == null) {
      throw new AllocationConfigurationException("No rule class found for "
          + ruleName);
    }
    return getPlacementRule(ruleClass, element);
  }
    
  /**
   * Build a simple queue placement policy from the configuration options
   * {@link FairSchedulerConfiguration#ALLOW_UNDECLARED_POOLS} and
   * {@link FairSchedulerConfiguration#USER_AS_DEFAULT_QUEUE}.
   * @param fs the reference to the scheduler needed in the rule on init.
   */
  static void fromConfiguration(FairScheduler fs) {
    LOG.debug("Creating base placement policy from config");
    Configuration conf = fs.getConfig();

    boolean create = conf.getBoolean(
        FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS,
        FairSchedulerConfiguration.DEFAULT_ALLOW_UNDECLARED_POOLS);
    boolean userAsDefaultQueue = conf.getBoolean(
        FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE,
        FairSchedulerConfiguration.DEFAULT_USER_AS_DEFAULT_QUEUE);
    List<PlacementRule> newRules = new ArrayList<>();
    List<Boolean> newTerminalState = new ArrayList<>();
    Class<? extends PlacementRule> clazz =
        RULES.get("specified").ruleClass;
    newRules.add(getPlacementRule(clazz, create));
    newTerminalState.add(false);
    if (userAsDefaultQueue) {
      clazz = RULES.get("user").ruleClass;
      newRules.add(getPlacementRule(clazz, create));
      newTerminalState.add(create);
    }
    if (!userAsDefaultQueue || !create) {
      clazz = RULES.get("default").ruleClass;
      newRules.add(getPlacementRule(clazz, true));
      newTerminalState.add(true);
    }
    try {
      updateRuleSet(newRules, newTerminalState, fs);
    } catch (AllocationConfigurationException ex) {
      throw new RuntimeException("Should never hit exception when loading" +
          "placement policy from conf", ex);
    }
  }
}
