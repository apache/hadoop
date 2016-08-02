/*
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

package org.apache.slider.server.appmaster.monkey;

import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.service.AbstractService;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.actions.RenewingAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * A chaos monkey service which will invoke ChaosTarget events 
 */
public class ChaosMonkeyService extends AbstractService {
  protected static final Logger log =
      LoggerFactory.getLogger(ChaosMonkeyService.class);

  private final MetricRegistry metrics;
  private final QueueAccess queues;
  private final Random random = new Random();

  private final List<ChaosEntry> chaosEntries =
      new ArrayList<ChaosEntry>();

  public ChaosMonkeyService(MetricRegistry metrics, QueueAccess queues) {
    super("ChaosMonkeyService");
    this.metrics = metrics;
    this.queues = queues;
  }

  /**
   * Add a target ... it is only added if <code>probability &gt; 0</code>
   * @param name name
   * @param target chaos target
   * @param probability probability
   */
  public synchronized void addTarget(String name,
      ChaosTarget target, long probability) {
    if (probability > 0) {
      log.info("Adding {} with probability {}", name,
          ((double)probability) / InternalKeys.PROBABILITY_PERCENT_1);
      chaosEntries.add(new ChaosEntry(name, target, probability, metrics));
    } else {
      log.debug("Action {} not enabled", name);
    }
  }

  /**
   * Get the number of targets in the list
   * @return the count of added targets
   */
  public int getTargetCount() {
    return chaosEntries.size();
  }
  
  /**
   * Iterate through all the entries and invoke chaos on those wanted
   */
  public void play() {
    for (ChaosEntry chaosEntry : chaosEntries) {
      long p = randomPercentage();
      chaosEntry.maybeInvokeChaos(p);
    }
  }

  public int randomPercentage() {
    return random.nextInt(InternalKeys.PROBABILITY_PERCENT_100);
  }

  /**
   * Check for callers to see if chaos should be triggered; shares the
   * same random number source as the rest of the monkey entries
   * @param probability probability 
   * @return true if the action should happen
   */
  public boolean chaosCheck(long probability) {
    return randomPercentage() < probability; 
  }
  
  /**
   * Schedule the monkey
   *
   * @param delay initial delay
   * @param timeUnit time unit
   * @return true if it was scheduled (i.e. 1+ action) and interval > 0
   */
  public boolean schedule(long delay, long interval, TimeUnit timeUnit) {
    if (interval > 0 && !chaosEntries.isEmpty()) {
      queues.schedule(getChaosAction(delay, interval, timeUnit));
      return true;
    } else {
      return false;
    }
  }

  /**
   * Get the chaos action
   *
   * @param delay
   * @param timeUnit time unit
   * @return the action to schedule
   */
  public RenewingAction<MonkeyPlayAction> getChaosAction(long delay,
      long interval,
      TimeUnit timeUnit) {
    RenewingAction<MonkeyPlayAction> action = new RenewingAction<MonkeyPlayAction>(
        new MonkeyPlayAction(this, 0, TimeUnit.MILLISECONDS),
        delay,
        interval,
        timeUnit,
        0
    );
    return action;
  }
}
