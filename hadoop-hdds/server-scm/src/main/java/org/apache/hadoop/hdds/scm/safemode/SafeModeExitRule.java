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
package org.apache.hadoop.hdds.scm.safemode;

import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;

/**
 * Abstract class for SafeModeExitRules. When a new rule is added, the new
 * rule should extend this abstract class.
 *
 * Each rule Should do:
 * 1. Should add a handler for the event it is looking for during the
 * initialization of the rule.
 * 2. Add the rule in ScmSafeModeManager to list of the rules.
 *
 *
 * @param <T>
 */
public abstract class SafeModeExitRule<T> implements EventHandler<T> {

  private final SCMSafeModeManager safeModeManager;
  private final String ruleName;

  public SafeModeExitRule(SCMSafeModeManager safeModeManager,
      String ruleName, EventQueue eventQueue) {
    this.safeModeManager = safeModeManager;
    this.ruleName = ruleName;
    eventQueue.addHandler(getEventType(), this);
  }

  /**
   * Return's the name of this SafeModeExit Rule.
   * @return ruleName
   */
  public String getRuleName() {
    return ruleName;
  }

  /**
   * Return's the event type this safeMode exit rule handles.
   * @return TypedEvent
   */
  protected abstract TypedEvent<T> getEventType();

  /**
   * Validate's this rule. If this rule condition is met, returns true, else
   * returns false.
   * @return boolean
   */
  protected abstract boolean validate();

  /**
   * Actual processing logic for this rule.
   * @param report
   */
  protected abstract void process(T report);

  /**
   * Cleanup action's need to be done, once this rule is satisfied.
   */
  protected abstract void cleanup();

  @Override
  public final void onMessage(T report, EventPublisher publisher) {

    // TODO: when we have remove handlers, we can remove getInSafemode check

    if (scmInSafeMode()) {
      if (validate()) {
        safeModeManager.validateSafeModeExitRules(ruleName, publisher);
        cleanup();
        return;
      }

      process(report);

      if (validate()) {
        safeModeManager.validateSafeModeExitRules(ruleName, publisher);
        cleanup();
      }
    }
  }

  /**
   * Return true if SCM is in safe mode, else false.
   * @return boolean
   */
  protected boolean scmInSafeMode() {
    return safeModeManager.getInSafeMode();
  }

}
