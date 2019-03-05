/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.chillmode;

import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;


/**
 * Abstract class for ChillModeExitRules. When a new rule is added, the new
 * rule should extend this abstract class.
 *
 * Each rule Should do:
 * 1. Should add a handler for the event it is looking for during the
 * initialization of the rule.
 * 2. Add the rule in ScmChillModeManager to list of the rules.
 *
 *
 * @param <T>
 */
public abstract class ChillModeExitRule<T> implements EventHandler<T> {

  protected final SCMChillModeManager chillModeManager;
  protected final String ruleName;

  public ChillModeExitRule(SCMChillModeManager chillModeManager, String ruleName) {
    this.chillModeManager = chillModeManager;
    this.ruleName = ruleName;
  }

  /**
   * Return's the name of this ChillModeExit Rule.
   * @return
   */
  public String getRuleName() {
    return ruleName;
  }


  /**
   * Validate's this rule. If this rule condition is met, returns true, else
   * returns false.
   * @return boolean
   */
  public abstract boolean validate();

  /**
   * Actual processing logic for this rule.
   * @param report
   */
  public abstract void process(T report);

  /**
   * Cleanup action's need to be done, once this rule is satisfied.
   */
  public abstract void cleanup();

  @Override
  public void onMessage(T report, EventPublisher publisher) {

    // TODO: when we have remove handlers, we can remove getInChillmode check

    if (chillModeManager.getInChillMode()) {
      if (validate()) {
        cleanup();
        chillModeManager.validateChillModeExitRules(ruleName, publisher);
        return;
      }

      process(report);

      if (validate()) {
        cleanup();
        chillModeManager.validateChillModeExitRules(ruleName, publisher);
      }
    }
  }

}
