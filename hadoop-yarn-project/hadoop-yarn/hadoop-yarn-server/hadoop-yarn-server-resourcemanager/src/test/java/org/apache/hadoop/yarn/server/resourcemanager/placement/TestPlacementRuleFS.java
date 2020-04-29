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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementFactory.getPlacementRule;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Simple tests for FS specific parts of the PlacementRule.
 */
public class TestPlacementRuleFS {

  // List of rules that are configurable (reject rule is not!)
  private static final List<Class <? extends PlacementRule>> CONFIG_RULES =
      new ArrayList<Class <? extends PlacementRule>>() {
    {
      add(DefaultPlacementRule.class);
      add(PrimaryGroupPlacementRule.class);
      add(SecondaryGroupExistingPlacementRule.class);
      add(SpecifiedPlacementRule.class);
      add(UserPlacementRule.class);
    }
  };

  // List of rules that are not configurable
  private static final List<Class <? extends PlacementRule>> NO_CONFIG_RULES =
      new ArrayList<Class <? extends PlacementRule>>() {
    {
      add(RejectPlacementRule.class);
    }
  };

  private final static FairSchedulerConfiguration CONF =
      new FairSchedulerConfiguration();
  private FairScheduler scheduler;
  private QueueManager queueManager;


  @Before
  public void initTest() {
    scheduler = mock(FairScheduler.class);
    // needed for all rules that rely on group info
    when(scheduler.getConfig()).thenReturn(CONF);
    // needed by all rules
    queueManager = new QueueManager(scheduler);
    when(scheduler.getQueueManager()).thenReturn(queueManager);
  }

  @After
  public void cleanTest() {
    queueManager = null;
    scheduler = null;
  }

  /**
   * Check the create and setting the config on the rule.
   * This walks over all known rules and check the behaviour:
   * - no config (null object)
   * - unknown object type
   * - boolean object
   * - xml config ({@link Element})
   * - calling initialize on the rule
   */
  @Test
  public void testRuleSetups() {
    // test for config(s) and init
    for (Class <? extends PlacementRule> ruleClass: CONFIG_RULES) {
      ruleCreateNoConfig(ruleClass);
      ruleCreateWrongObject(ruleClass);
      ruleCreateBoolean(ruleClass);
      ruleCreateElement(ruleClass);
      ruleInit(ruleClass);
    }
  }

  /**
   * Check the init of rules that do not use a config.
   */
  @Test
  public void testRuleInitOnly() {
    // test for init
    for (Class <? extends PlacementRule> ruleClass: NO_CONFIG_RULES) {
      ruleInit(ruleClass);
    }
  }

  private void ruleCreateNoConfig(Class <? extends PlacementRule> ruleClass) {
    PlacementRule rule = getPlacementRule(ruleClass, null);
    String name = ruleClass.getName();
    assertNotNull("Rule object should not be null for " + name, rule);
  }

  private void ruleCreateWrongObject(
      Class <? extends PlacementRule> ruleClass) {
    PlacementRule rule = getPlacementRule(ruleClass, "a string object");
    String name = ruleClass.getName();
    assertNotNull("Rule object should not be null for " + name, rule);
  }

  private void ruleCreateBoolean(Class <? extends PlacementRule> ruleClass) {
    PlacementRule rule = getPlacementRule(ruleClass, true);
    String name = ruleClass.getName();
    assertNotNull("Rule object should not be null for " + name, rule);
    assertTrue("Create flag was not set to true on " + name,
        getCreateFlag(rule));
    rule = getPlacementRule(ruleClass, false);
    assertNotNull("Rule object should not be null for " + name, rule);
    assertFalse("Create flag was not set to false on " + name,
        getCreateFlag(rule));
  }

  private void ruleCreateElement(Class <? extends PlacementRule> ruleClass) {
    String str = "<rule name='not used' create=\"true\" />";
    Element conf = createConf(str);
    PlacementRule rule = getPlacementRule(ruleClass, conf);
    String name = ruleClass.getName();
    assertNotNull("Rule object should not be null for " + name, rule);
    assertTrue("Create flag was not set to true on " + name,
        getCreateFlag(rule));
    str = "<rule name='not used' create=\"false\" />";
    conf = createConf(str);
    rule = getPlacementRule(ruleClass, conf);
    assertNotNull("Rule object should not be null for " + name, rule);
    assertFalse("Create flag was not set to false on " + name,
        getCreateFlag(rule));
  }

  private void ruleInit(Class <? extends PlacementRule> ruleClass) {
    PlacementRule rule = getPlacementRule(ruleClass, null);
    String name = ruleClass.getName();
    assertNotNull("Rule object should not be null for " + name, rule);
    try {
      rule.initialize(scheduler);
    } catch (IOException ioe) {
      fail("Unexpected exception on initialize of rule " + name);
    }
    // now set the parent rule: use the same rule as a child.
    // always throws: either because parentRule is not allowed or because it
    // is the same class as the child rule.
    ((FSPlacementRule)rule).setParentRule(rule);
    boolean exceptionThrown = false;
    try {
      rule.initialize(scheduler);
    } catch (IOException ioe) {
      exceptionThrown = true;
    }
    assertTrue("Initialize with parent rule should have thrown exception " +
            name, exceptionThrown);
  }

  private Element createConf(String str) {
    // Create a simple rule element to use in the rule create
    DocumentBuilderFactory docBuilderFactory =
        DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    Document doc = null;
    try {
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      doc = builder.parse(IOUtils.toInputStream(str,
          Charset.defaultCharset()));
    } catch (Exception ex) {
      fail("Element creation failed, failing test");
    }
    return doc.getDocumentElement();
  }

  private boolean getCreateFlag(PlacementRule rule) {
    if (rule instanceof FSPlacementRule) {
      return ((FSPlacementRule)rule).createQueue;
    }
    fail("Rule is not a FSPlacementRule");
    return false;
  }
}
