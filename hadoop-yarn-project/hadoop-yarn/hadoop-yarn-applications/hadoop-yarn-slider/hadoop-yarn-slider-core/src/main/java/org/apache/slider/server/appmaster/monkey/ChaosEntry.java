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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.slider.api.InternalKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry in the chaos list
 */
public class ChaosEntry {

  protected static final Logger log =
      LoggerFactory.getLogger(ChaosEntry.class);
  public final String name;
  public final ChaosTarget target;
  public final long probability;

  private final Counter invocationCounter;


  /**
   * Constructor -includes validation of all arguments
   * @param name entry name
   * @param target target
   * @param probability probability of occurring
   */
  public ChaosEntry(String name, ChaosTarget target, long probability,
      MetricRegistry metrics) {
    Preconditions.checkArgument(!StringUtils.isEmpty(name), "missing name");
    Preconditions.checkArgument(target != null, "null target");
    Preconditions.checkArgument(probability > 0, "negative probability");
    Preconditions.checkArgument(probability <= InternalKeys.PROBABILITY_PERCENT_100,
        "probability over 100%: "+ probability);
    this.name = name;
    this.target = target;
    this.probability = probability;
    invocationCounter =
        metrics.counter(MetricRegistry.name(ChaosEntry.class, name));
  }

  /**
   * Trigger the chaos action
   */
  public void invokeChaos() {
    log.info("Invoking {}", name);
    invocationCounter.inc();
    target.chaosAction();
  }

  /**
   * Invoke Chaos if the trigger value is in range of the probability
   * @param value trigger value, 0-10K
   * @return true if the chaos method was invoked
   */
  public boolean maybeInvokeChaos(long value) {
    log.debug("Probability {} trigger={}", probability, value);
    if (value < probability) {
      invokeChaos();
      return true;
    }
    return false;
  }
}
