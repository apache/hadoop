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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;

/**
 * Abstract base for all {@link FairScheduler} Placement Rules.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class FSPlacementRule extends PlacementRule {
  private static final Logger LOG =
      LoggerFactory.getLogger(FSPlacementRule.class);

  // Flag to show if the rule can create a queue
  @VisibleForTesting
  protected boolean createQueue = true;
  private QueueManager queueManager;
  private PlacementRule parentRule;

  /**
   * Get the {@link QueueManager} loaded from the scheduler when the rule is
   * initialised. All rules are initialised before the can be called to place
   * an application.
   * @return The queue manager from the scheduler, this can never be
   * <code>null</code> for an initialised rule.
   */
  QueueManager getQueueManager() {
    return queueManager;
  }

  /**
   * Set a rule to generate the parent queue dynamically. The parent rule
   * should only be called on rule creation when the policy is read from the
   * configuration.
   * @param parent A PlacementRule
   */
  public void setParentRule(PlacementRule parent) {
    this.parentRule = parent;
  }

  /**
   * Get the rule that is set to generate the parent queue dynamically.
   * @return The rule set or <code>null</code> if not set.
   */
  @VisibleForTesting
  public PlacementRule getParentRule() {
    return parentRule;
  }

  /**
   * Set the config based on the type of object passed in.
   * @param initArg the config to be set
   */
  @Override
  public void setConfig(Object initArg) {
    if (null == initArg) {
      LOG.debug("Null object passed in: no config set");
      return;
    }
    if (initArg instanceof Element) {
      LOG.debug("Setting config from XML");
      setConfig((Element) initArg);
    } else if (initArg instanceof Boolean) {
      LOG.debug("Setting config from Boolean");
      setConfig((Boolean) initArg);
    } else {
      LOG.info("Unknown object type passed in as config for rule {}: {}",
          getName(), initArg.getClass());
    }
  }

  /**
   * Set the rule config from the xml config.
   * @param conf An xml element from the {@link FairScheduler#conf}
   */
  protected void setConfig(Element conf) {
    // Get the flag from the config (defaults to true if not set)
    createQueue = getCreateFlag(conf);
  }

  /**
   * Set the rule config just setting the create flag.
   * @param create flag to allow queue creation for this rule
   */
  protected void setConfig(Boolean create) {
    createQueue = create;
  }

  /**
   * Standard initialisation for {@link FairScheduler} rules, shared by all
   * rules. Each rule that extends this abstract and overrides this method must
   * call <code>super.initialize()</code> to run this basic initialisation.
   * @param scheduler the scheduler using the rule
   * @return <code>true</code> in all cases
   * @throws IOException for any errors
   */
  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    if (!(scheduler instanceof FairScheduler)) {
      throw new IOException(getName() +
          " rule can only be configured for the FairScheduler");
    }
    if (getParentRule() != null &&
        getParentRule().getName().equals(getName())) {
      throw new IOException("Parent rule may not be the same type as the " +
          "child rule: " + getName());
    }

    FairScheduler fs = (FairScheduler) scheduler;
    queueManager = fs.getQueueManager();

    return true;
  }

  /**
   * Check if the queue exists and is part of the configuration i.e. not
   * a {@link FSQueue#isDynamic()} queue.
   * @param queueName name of the queue to check
   * @return <code>true</code> if the queue exists and is a "configured" queue
   */
  boolean configuredQueue(String queueName) {
    FSQueue queue = queueManager.getQueue(queueName);
    return (queue != null && !queue.isDynamic());
  }

  /**
   * Get the create flag as set during the config setup.
   * @return The value of the {@link #createQueue} flag
   */
  public boolean getCreateFlag() {
    return createQueue;
  }

  /**
   * Get the create flag from the xml configuration element.
   * @param conf The FS configuration element for the queue
   * @return <code>false</code> only if the flag is set in the configuration to
   * a text that is not case ignored "true", <code>true</code> in all other
   * cases
   */
  boolean getCreateFlag(Element conf) {
    if (conf != null) {
      String create = conf.getAttribute("create");
      return create.isEmpty() || Boolean.parseBoolean(create);
    }
    return true;
  }
}
