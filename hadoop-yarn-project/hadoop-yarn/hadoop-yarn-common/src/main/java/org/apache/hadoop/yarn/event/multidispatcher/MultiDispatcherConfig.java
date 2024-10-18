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

package org.apache.hadoop.yarn.event.multidispatcher;

import org.apache.hadoop.conf.Configuration;

/**
 * All the config what can be use in the {@link MultiDispatcher}
 */
class MultiDispatcherConfig extends Configuration {

  private final String prefix;

  public MultiDispatcherConfig(Configuration configuration, String dispatcherName) {
    super(configuration);
    this.prefix = String.format("yarn.dispatcher.multi-thread.%s.", dispatcherName);
  }

  /**
   * How many executor thread should be created to handle the incoming events
   * @return configured value, or default 4
   */
  public int getDefaultPoolSize() {
    return super.getInt(prefix + "default-pool-size", 4);
  }

  /**
   * Maximus size of the event queue of the executor threads.
   * If limit is reached then the queue#add method will block.
   * @return configured value, or default 1_000_000
   */
  public int getQueueSize() {
    return super.getInt(prefix + "queue-size", 1_000_000);
  }

  /**
   * How frequently the monitor thread should write the state of the dispatcher to the LOG.
   * If less than 1 this monitoring will be turned off.
   * @return configured value, or default 0
   */
  public int getMonitorSeconds() {
    return super.getInt(prefix + "monitor-seconds", 0);
  }

  /**
   * How long should the dispatcher wait to drain all event queue of workers,
   * after stop signal is received.
   * @return configured value, or default 60
   */
  public int getGracefulStopSeconds() {
    return super.getInt(prefix + "graceful-stop-seconds", 60);
  }

  /**
   * Dispatcher metrics should be published to the metric system.
   * @return configured value, or default false
   */
  public boolean getMetricsEnabled() {
    return super.getBoolean(prefix + "metrics-enabled", false);
  }
}
