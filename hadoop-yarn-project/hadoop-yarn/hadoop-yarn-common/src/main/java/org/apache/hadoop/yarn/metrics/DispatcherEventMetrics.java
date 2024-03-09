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
package org.apache.hadoop.yarn.metrics;

import org.apache.hadoop.metrics2.MetricsSource;

/**
 * Interface for {@link org.apache.hadoop.yarn.event.Dispatcher}
 * can be used to publish {@link org.apache.hadoop.yarn.event.Event} related metrics
 */
public interface DispatcherEventMetrics extends MetricsSource {

  /**
   * Class of the event type what can be handled by the DispatcherEventMetrics
   * @param typeClass the event type
   */
  void init(Class<? extends Enum> typeClass);

  /**
   * Call if Event added for dispatching
   * @param type type of the event
   */
  void addEvent(Object type);

  /**
   * Call if Event handled
   * @param type type of the event
   */
  void removeEvent(Object type);

  /**
   * Call with how much time was required to handle the event
   * @param type type of the event
   * @param millisecond time interval
   */
  void updateRate(Object type, long millisecond);
}
