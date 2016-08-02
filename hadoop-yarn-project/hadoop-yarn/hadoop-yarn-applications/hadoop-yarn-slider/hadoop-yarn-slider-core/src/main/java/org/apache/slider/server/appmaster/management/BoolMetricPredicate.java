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

package org.apache.slider.server.appmaster.management;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;

/**
 * A metric which takes a predicate and returns 1 if the predicate evaluates
 * to true. The predicate is evaluated whenever the metric is read.
 */
public class BoolMetricPredicate implements Metric, Gauge<Integer> {

  private final Eval predicate;

  public BoolMetricPredicate(Eval predicate) {
    this.predicate = predicate;
  }

  @Override
  public Integer getValue() {
    return predicate.eval() ? 1: 0;
  }

  public interface Eval {
    boolean eval();
  }
}
